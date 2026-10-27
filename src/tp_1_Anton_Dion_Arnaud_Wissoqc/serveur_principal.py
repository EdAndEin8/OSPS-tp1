"""Principal server (d, dispatcher).

- Creates the pair of named pipes dwtube1 / wdtube1, starts the secondary server
  (w) with fork() and restarts it when it stops.
- Receives the requests of the clients on port 2222 and delegates them to w.
  w is unique: while it is busy, new clients receive "occupe".
- Mutual exclusion on the shared memory (ping-pong): d writes in the segment only
  before sending "ping" and reads it only after receiving "pong"; w does the
  opposite. Exactly one of them can use the segment at any time.

States of d:
    libre        w waits for a request, d owns the segment
    debut        "ping" sent with a request, waiting for "pong" (debuttraitement)
    traitement   "ping" sent after the answer to the client, waiting for "pong"
    bloque       w is stopping (error or too slow), waiting for its end
"""
import logging
import os
import secrets
import select
import socket
import time

import sysv_ipc

from . import config
from .outils import recv_one, send_line
from .serveur_secondaire import ServeurSecondaire
from .surveillance import PRINCIPAL, answer_watchdog

log = logging.getLogger("principal(d)")

FREE, STARTING, PROCESSING, STUCK = "libre", "debut", "traitement", "bloque"


class ServeurPrincipal:

    def __init__(self, mq, segment, tube_dir):
        self.mq = mq
        self.segment = segment
        self.dwtube_path = os.path.join(tube_dir, "dwtube1")
        self.wdtube_path = os.path.join(tube_dir, "wdtube1")
        self.server = None
        self.worker_pid = None
        self.dwtube = None          # write end of dwtube1
        self.wdtube = None          # read end of wdtube1
        self.state = FREE
        self.client = None          # client waiting for the port of w
        self.ticket = None
        self.deadline = 0.0
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
                self.loop_once()
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

    def loop_once(self):
        watched = [self.server] if self.wdtube is None else [self.server, self.wdtube]
        ready, _, _ = select.select(watched, [], [], 0.5)
        if self.server in ready:
            self.accept_client()
        if self.wdtube is not None and self.wdtube in ready:
            self.read_worker()
        answer_watchdog(self.mq, PRINCIPAL)
        self.check_worker()

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
        self.ticket = secrets.token_hex(8)        # one-time ticket for w
        self.segment.write_request(request, addr[0], self.ticket)
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
            os.write(self.dwtube, b"ping\n")
        except OSError as e:
            log.error("secondary server unreachable: %s", e)
            self.give_up_worker()
            return
        self.state = next_state
        self.deadline = time.monotonic() + config.WORKER_TIMEOUT

    def read_worker(self):
        try:
            message = os.read(self.wdtube, 16)
        except BlockingIOError:
            return
        if message != b"pong\n" or self.state not in (STARTING, PROCESSING):
            if message:
                log.warning("unexpected message on wdtube1: %r", message)
            self.give_up_worker()
            return
        try:
            status, port = self.segment.read_status()
        except ValueError as e:
            log.warning("%s", e)
            status, port = "errtraitement", 0

        if self.state == STARTING:
            if status == "debuttraitement":
                self.reply_and_close(self.client, f"port {port} ticket {self.ticket}")
            else:
                self.reply_and_close(self.client, "errtraitement")
            self.client = None
            self.ticket = None
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
            try:
                os.unlink(path)              # pipes of a previous secondary server
            except FileNotFoundError:
                pass
            os.mkfifo(path, 0o600)
        fds = []
        try:
            for path in (self.dwtube_path, self.wdtube_path):
                fds.append(os.open(path, os.O_RDONLY | os.O_NONBLOCK))
                fds.append(os.open(path, os.O_WRONLY))
            dw_read, dw_write, wd_read, wd_write = fds
            self.segment.clear()
            pid = os.fork()
        except OSError:
            for fd in fds:
                os.close(fd)
            raise

        if pid == 0:     # child: secondary server
            code = 1
            try:
                # w only keeps its two ends of the pipes and the segment.
                self.server.close()
                os.close(dw_write)
                os.close(wd_read)
                os.set_blocking(dw_read, True)
                code = ServeurSecondaire(self.mq, self.segment, dw_read, wd_write).start()
            except BaseException:
                log.exception("secondary server crashed")
            finally:
                os._exit(code)   # never go back into the code of d

        os.close(dw_read)
        os.close(wd_write)
        self.dwtube, self.wdtube = dw_write, wd_read
        self.worker_pid = pid
        self.state = FREE
        log.info("secondary server %d started", pid)

    def give_up_worker(self):
        """Close the pipes: w sees the end of file and stops.

        The segment is not used again before w is collected (check_worker), so
        two processes can never use it at the same time.
        """
        self.close_tubes()
        self.state = STUCK

    def close_tubes(self):
        for fd in (self.dwtube, self.wdtube):
            if fd is not None:
                os.close(fd)
        self.dwtube = self.wdtube = None

    def check_worker(self):
        """Collect w if it stopped (no zombie) and restart it."""
        if self.worker_pid is None:
            return
        pid, status = os.waitpid(self.worker_pid, os.WNOHANG)
        if pid == 0:
            if self.state in (STARTING, PROCESSING) and time.monotonic() > self.deadline:
                log.error("secondary server too slow, its pipes are closed")
                self.give_up_worker()
            return
        log.warning("secondary server %d ended (code %d)",
                    pid, os.waitstatus_to_exitcode(status))
        self.worker_pid = None
        self.close_tubes()
        if self.client is not None:
            self.reply_and_close(self.client, "errtraitement")
            self.client = None
        self.ticket = None
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
                    os.write(self.dwtube, b"stop\n")
                except OSError as e:
                    log.debug("stop not sent: %s", e)
            self.close_tubes()
            for _ in range(50):                  # at most 10 seconds
                pid, _status = os.waitpid(self.worker_pid, os.WNOHANG)
                if pid != 0:
                    break
                time.sleep(0.2)
            else:
                log.error("secondary server %d still running", self.worker_pid)
        self.close_tubes()
        for path in (self.dwtube_path, self.wdtube_path):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        if self.server is not None:
            self.server.close()
        self.segment.clear()
