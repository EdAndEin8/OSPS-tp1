"""Principal server: accepts the clients and manages the mutual exclusion.

For each client a secondary server is forked. Both are connected by a pair of
named pipes:
    fifo-p2s   principal -> secondary   "OK"  (access granted)
    fifo-s2p   secondary -> principal   "REQ" (access requested), "REL" (access released)

The principal server watches every pipe with select(). It answers "OK" to a
"REQ" only when nobody holds the access to the shared memory; otherwise the
request waits in a queue (first come, first served).
"""
import logging
import os
import select
import socket
import time

from . import config
from .serveur_secondaire import ServeurSecondaire


class ServeurPrincipal:

    def __init__(self, memory, fifo_dir):
        self.memory = memory
        self.fifo_dir = fifo_dir    # private directory created by the watchdog
        self.server = None
        self.count = 0              # used to give a different name to each pair of pipes
        self.children = set()       # PIDs of the secondary servers not collected yet
        self.secondaries = {}       # PID -> (read end of fifo-s2p, write end of fifo-p2s)
        self.fd_to_pid = {}         # read end of fifo-s2p -> PID
        self.waiting = []           # PIDs waiting for the shared memory
        self.holder = None          # PID that holds the shared memory, or None

    def start(self):
        """Serve the clients until Ctrl+C. Return the exit code of the process."""
        try:
            self.server = socket.create_server((config.HOST, config.PORT))
        except OSError as e:
            logging.error("cannot listen on %s:%d: %s", config.HOST, config.PORT, e)
            return 1
        self.server.setblocking(False)
        logging.info("principal server ready on %s:%d", config.HOST, config.PORT)
        try:
            while True:
                watched = [self.server, *self.fd_to_pid]
                ready, _, _ = select.select(watched, [], [], 1.0)
                for item in ready:
                    if item is self.server:
                        self.accept_client()
                    elif item in self.fd_to_pid:        # may have been closed meanwhile
                        self.read_fifo(item)
                self.collect_children()
                self.give_access()
        except KeyboardInterrupt:
            logging.info("principal server stopped")
            return 0
        finally:
            self.stop()

    # ---------------------------------------------------------------- clients

    def accept_client(self):
        try:
            connexion, adresse = self.server.accept()
        except BlockingIOError:
            return
        with connexion:              # the principal never keeps the client socket
            if len(self.children) >= config.MAX_CLIENTS:
                logging.warning("client %s:%d refused: too many clients", *adresse)
                try:
                    connexion.sendall(b"FIN\n")
                except OSError as e:
                    logging.debug("refusal not sent: %s", e)
                return
            try:
                self.open_secondary_server(connexion, adresse)
            except OSError as e:
                logging.error("cannot start a secondary server: %s", e)

    def create_fifos(self):
        """Create the pair of named pipes of a new secondary and open the four ends.

        The ends are opened before fork(), each read end first and in non-blocking
        mode, so no open() blocks (no deadlock between the two processes). The
        names are removed right away: the opened descriptors stay usable.
        """
        self.count += 1
        p2s = os.path.join(self.fifo_dir, f"fifo-p2s-{self.count}")
        s2p = os.path.join(self.fifo_dir, f"fifo-s2p-{self.count}")
        os.mkfifo(p2s, 0o600)
        os.mkfifo(s2p, 0o600)
        fds = []
        try:
            for path in (p2s, s2p):
                fds.append(os.open(path, os.O_RDONLY | os.O_NONBLOCK))
                fds.append(os.open(path, os.O_WRONLY))
        except OSError:
            for fd in fds:
                os.close(fd)
            raise
        finally:
            os.unlink(p2s)
            os.unlink(s2p)
        return fds       # p2s_read, p2s_write, s2p_read, s2p_write

    def open_secondary_server(self, connexion, adresse):
        p2s_read, p2s_write, s2p_read, s2p_write = self.create_fifos()
        try:
            pid = os.fork()
        except OSError:
            for fd in (p2s_read, p2s_write, s2p_read, s2p_write):
                os.close(fd)
            raise

        if pid == 0:     # child: secondary server
            code = 1
            try:
                # Close everything that belongs to the principal server or to the
                # other secondaries: the child only keeps its client and its pipes.
                self.server.close()
                os.close(p2s_write)
                os.close(s2p_read)
                for fifo_read, fifo_write in self.secondaries.values():
                    os.close(fifo_read)
                    os.close(fifo_write)
                son = ServeurSecondaire(self.memory, p2s_read, s2p_write)
                code = son.start(connexion, adresse)
            except BaseException:
                logging.exception("secondary server crashed")
            finally:
                os._exit(code)   # never go back into the loop of the principal server

        # parent: principal server
        os.close(p2s_read)
        os.close(s2p_write)
        self.children.add(pid)
        self.secondaries[pid] = (s2p_read, p2s_write)
        self.fd_to_pid[s2p_read] = pid
        logging.info("client %s:%d -> secondary server %d", *adresse, pid)

    # ------------------------------------------------------- mutual exclusion

    def read_fifo(self, fd):
        pid = self.fd_to_pid[fd]
        try:
            data = os.read(fd, 64)
        except BlockingIOError:
            return
        if not data:                     # the secondary closed its pipe
            self.close_fifos(pid)
            return
        for command in data.decode("ascii", errors="replace").split():
            if command == "REQ" and pid != self.holder and pid not in self.waiting:
                self.waiting.append(pid)
            elif command == "REL" and pid == self.holder:
                self.holder = None
            else:
                logging.warning("secondary server %d: invalid message, pipes closed", pid)
                self.close_fifos(pid)
                return

    def give_access(self):
        """Give the shared memory to the first waiting secondary when it is free."""
        while self.holder is None and self.waiting:
            pid = self.waiting.pop(0)
            if pid not in self.secondaries:
                continue
            try:
                os.write(self.secondaries[pid][1], b"OK\n")
                self.holder = pid
            except OSError as e:
                logging.warning("secondary server %d unreachable: %s", pid, e)
                self.close_fifos(pid)

    def close_fifos(self, pid):
        ends = self.secondaries.pop(pid, None)
        if ends is None:
            return
        del self.fd_to_pid[ends[0]]
        os.close(ends[0])
        os.close(ends[1])
        if pid in self.waiting:
            self.waiting.remove(pid)

    # ------------------------------------------------------- end of processes

    def collect_children(self):
        """Collect the finished secondaries (no zombie processes)."""
        while self.children:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                return
            self.children.discard(pid)
            self.close_fifos(pid)
            if pid == self.holder:
                # Freed only now that the process is gone: two processes can never
                # use the shared memory at the same time.
                logging.warning("secondary server %d ended while holding the access", pid)
                self.holder = None
            logging.info("secondary server %d ended (code %d)",
                         pid, os.waitstatus_to_exitcode(status))

    def stop(self):
        if self.server is not None:
            self.server.close()
        for pid in list(self.secondaries):
            self.close_fifos(pid)
        for _ in range(50):                      # wait at most 5 seconds
            self.collect_children()
            if not self.children:
                break
            time.sleep(0.1)
        else:
            logging.error("%d secondary server(s) still running", len(self.children))
