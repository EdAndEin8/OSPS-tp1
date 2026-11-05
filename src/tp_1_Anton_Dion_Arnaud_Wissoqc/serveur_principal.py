"""Principal server (d, dispatcher).

- Creates the pair of named pipes dwtube1 / wdtube1, starts the secondary server
  (w) with fork() and restarts it when it is down (checked with waitpid).
- Receives the requests of the clients on port 2222 and delegates them to w.
  w is unique: while it is busy, new clients receive "occupe".
- Mutual exclusion on the shared memory (ping-pong): d writes in the segment only
  before sending "ping" and reads it only after receiving "pong"; w does the
  opposite. Exactly one of them can use the segment at any time.

States of d:
    libre        w waits for a request, d owns the segment
    debut        "ping" sent with a request, waiting for "pong" (debuttraitement)
    traitement   "ping" sent after the answer to the client, waiting for "pong"
"""
import logging
import os
import select
import socket
import time

import sysv_ipc

from . import config
from .outils import recv_one, send_line
from .serveur_secondaire import PING, PONG, STOP, ServeurSecondaire
from .systeme import fork_process, remove_file, wait_child

log = logging.getLogger("principal(d)")

FREE, STARTING, PROCESSING = "libre", "debut", "traitement"


class ServeurPrincipal:

    def __init__(self, segment, tube_dir):
        self.segment = segment
        self.dwtube_path = os.path.join(tube_dir, "dwtube1")
        self.wdtube_path = os.path.join(tube_dir, "wdtube1")
        self.server = None
        self.worker_pid = None
        self.dwtube = None          # write end of dwtube1
        self.wdtube = None          # read end of wdtube1
        self.state = FREE
        self.client = None          # client waiting for the port of w
        self.worker_restarts = 0
        self.done = 0               # number of processed requests

    def start(self):
        """Serve until Ctrl+C or MAX_REQUESTS requests. Return the exit code."""
        try:
            self.server = socket.create_server((config.HOST, config.PORT))
            self.server.setblocking(False)
            self.start_worker()
            log.info("ready on port %d", config.PORT)
            while not (config.MAX_REQUESTS and self.done >= config.MAX_REQUESTS):
                watched = [self.server] if self.wdtube is None else [self.server, self.wdtube]
                ready, _, _ = select.select(watched, [], [], 0.5)
                # The end of a request ("pong") is read before the new clients,
                # otherwise a client coming right after the previous result
                # would receive "occupe".
                if self.wdtube is not None and self.wdtube in ready:
                    self.read_worker()
                if self.server in ready:
                    self.accept_client()
                self.check_worker()
            log.info("%d requests processed, stopping", self.done)
            return 0
        except KeyboardInterrupt:
            log.info("stopped (Ctrl+C)")
            return 0
        except (OSError, ValueError, RuntimeError, sysv_ipc.Error) as e:
            log.error("stopped on error: %s", e)
            return 1
        finally:
            self.stop()

    # ---------------------------------------------------------------- clients

    def accept_client(self):
        try:
            conn, addr = self.server.accept()
        except BlockingIOError:
            return
        conn.settimeout(config.CLIENT_TIMEOUT)
        if self.state != FREE:
            log.info("client %s refused: secondary server busy", addr[0])
            self.reply_and_close(conn, "occupe")
            return
        try:
            request = recv_one(conn)
        except (OSError, ValueError) as e:
            log.warning("client %s: invalid request (%s)", addr[0], e)
            conn.close()
            return
        if request not in config.REQUEST_TYPES:
            log.warning("client %s: unknown request type", addr[0])
            self.reply_and_close(conn, "refus")
            return
        self.segment.write_request(request, addr[0])
        self.client = conn
        log.info("client %s: %s delegated to the secondary server", addr[0], request)
        self.ping(STARTING)

    @staticmethod
    def reply_and_close(conn, text):
        try:
            send_line(conn, text)
        except OSError as e:
            log.debug("answer not sent: %s", e)
        finally:
            conn.close()

    # -------------------------------------------------------------- ping-pong

    def ping(self, next_state):
        """Give the segment to w."""
        try:
            os.write(self.dwtube, PING)
        except OSError as e:
            log.error("secondary server unreachable: %s", e)
            self.close_tubes()          # w is restarted by check_worker
            return
        self.state = next_state

    def read_worker(self):
        message = os.read(self.wdtube, 16)
        if message != PONG or self.state == FREE:
            if message:
                log.warning("unexpected message on wdtube1: %r", message)
            # Closing the pipes makes w stop; it is restarted by check_worker.
            # Until then, the state stays "busy": the segment is not used.
            self.close_tubes()
            return
        try:
            status, port = self.segment.read_status()
        except ValueError as e:
            log.warning("%s", e)
            status, port = "errtraitement", 0

        if self.state == STARTING:
            if status == "debuttraitement":
                self.reply_and_close(self.client, f"port {port}")
            else:
                self.reply_and_close(self.client, "errtraitement")
            self.client = None
            self.ping(PROCESSING)
        else:
            log.info("%s", status)
            self.segment.clear()
            self.done += 1
            self.state = FREE

    # ---------------------------------------------------------- secondary server

    def start_worker(self):
        """Create dwtube1 / wdtube1, open their four ends, then fork w.

        Each read end is opened first, in non-blocking mode: no open() blocks, so
        d and w cannot wait for each other (deadlock of the Moodle example).
        """
        for path in (self.dwtube_path, self.wdtube_path):
            remove_file(path)                # pipes of a previous secondary server
            os.mkfifo(path, 0o600)
        fds = []
        try:
            for path in (self.dwtube_path, self.wdtube_path):
                fds.append(os.open(path, os.O_RDONLY | os.O_NONBLOCK))
                fds.append(os.open(path, os.O_WRONLY))
            dw_read, dw_write, wd_read, wd_write = fds
            self.segment.clear()
            pid = fork_process(self.run_worker, dw_read, dw_write, wd_read, wd_write)
        except OSError:
            for fd in fds:
                os.close(fd)
            raise

        os.close(dw_read)
        os.close(wd_write)
        self.dwtube, self.wdtube = dw_write, wd_read
        self.worker_pid = pid
        self.state = FREE
        log.info("secondary server %d started", pid)

    def run_worker(self, dw_read, dw_write, wd_read, wd_write):
        """Body of the child process: w only keeps its two ends and the segment."""
        self.server.close()
        os.close(dw_write)
        os.close(wd_read)
        os.set_blocking(dw_read, True)
        return ServeurSecondaire(self.segment, dw_read, wd_write).start()

    def close_tubes(self):
        for fd in (self.dwtube, self.wdtube):
            if fd is not None:
                os.close(fd)
        self.dwtube = self.wdtube = None

    def check_worker(self):
        """Check that w is not down (waitpid); if it is, collect it and restart it."""
        pid, status = os.waitpid(self.worker_pid, os.WNOHANG)
        if pid == 0:
            return                      # w is running
        log.warning("secondary server %d down (code %d)",
                    pid, os.waitstatus_to_exitcode(status))
        self.close_tubes()
        if self.client is not None:
            self.reply_and_close(self.client, "errtraitement")
            self.client = None
        self.worker_pid = None
        self.worker_restarts += 1
        if self.worker_restarts > config.MAX_RESTARTS:
            raise RuntimeError("secondary server restarted too many times")
        time.sleep(config.RESTART_DELAY)
        self.start_worker()

    # --------------------------------------------------------------------- stop

    def stop(self):
        """Release the resources in the reverse order of their acquisition.

        w is stopped first and collected by d (its parent) so that it does not
        stay as a zombie. The segment itself belongs to the watchdog.
        """
        if self.client is not None:
            self.client.close()
        if self.worker_pid is not None:
            if self.dwtube is not None:
                try:
                    os.write(self.dwtube, STOP)
                except OSError as e:
                    log.debug("stop not sent: %s", e)
            self.close_tubes()
            if wait_child(self.worker_pid, 10) is None:
                log.error("secondary server %d still running", self.worker_pid)
        self.close_tubes()
        for path in (self.dwtube_path, self.wdtube_path):
            remove_file(path)
        if self.server is not None:
            self.server.close()
        self.segment.clear()
