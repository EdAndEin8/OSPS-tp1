"""Secondary server (w, worker): processes the requests delegated by the principal server.

Cycle of one request on the pair of named pipes (ping-pong):
    1. wait "ping" on dwtube1: a request is in the shared memory;
    2. read it, write "debuttraitement" and the port of this server, send "pong";
    3. wait "ping": the principal server has given the port to the client;
    4. serve the client directly (address and ticket checked, simulated processing),
       write "fintraitement" or "errtraitement", send "pong".
Between a "ping" and the next "pong", only this process uses the segment.
"""
import hmac
import logging
import os
import select
import socket
import time

import sysv_ipc

from . import config
from .outils import recv_line, send_line
from .surveillance import SECONDAIRE, answer_watchdog

log = logging.getLogger("secondaire(w)")


class ServeurSecondaire:

    def __init__(self, mq, segment, dwtube, wdtube):
        self.mq = mq
        self.segment = segment
        self.dwtube = dwtube        # read end of dwtube1: principal -> secondary
        self.wdtube = wdtube        # write end of wdtube1: secondary -> principal
        self.server = None
        self.request = None
        self.client_ip = None
        self.ticket = None

    def start(self):
        """Process requests until the principal server stops. Return the exit code."""
        try:
            self.server = self.listen()
            log.info("ready on port %d", config.WORKER_PORT)
            while self.wait_ping():
                ok = self.begin()
                self.pong()
                if not self.wait_ping():
                    break
                status = self.serve_client() if ok else "errtraitement"
                self.segment.write_status(status, 0)
                self.pong()
            return 0
        except KeyboardInterrupt:
            return 0
        except (OSError, ValueError, sysv_ipc.Error) as e:
            log.error("stopped on error: %s", e)
            return 1
        finally:
            self.ticket = None
            if self.server is not None:
                self.server.close()
            os.close(self.dwtube)
            os.close(self.wdtube)

    def listen(self):
        """Open the socket of the secondary server.

        After a crash, the previous secondary server may still hold the port for a
        few seconds, so several attempts are made.
        """
        for _ in range(5):
            try:
                return socket.create_server((config.HOST, config.WORKER_PORT))
            except OSError as e:
                log.warning("port %d unavailable (%s), new attempt in 1 s",
                            config.WORKER_PORT, e)
                time.sleep(1)
        raise OSError(f"port {config.WORKER_PORT} unavailable")

    # ------------------------------------------------------------- ping-pong

    def wait_ping(self):
        """Wait for the next message of d. Return False on "stop" or end of file."""
        while True:
            ready, _, _ = select.select([self.dwtube], [], [], 0.5)
            answer_watchdog(self.mq, SECONDAIRE)
            if not ready:
                continue
            message = os.read(self.dwtube, 16)
            if message == b"ping\n":
                return True
            if message in (b"", b"stop\n"):
                log.info("stop requested by the principal server")
                return False
            raise ValueError(f"unexpected message on dwtube1: {message!r}")

    def pong(self):
        """Give the segment back to the principal server."""
        os.write(self.wdtube, b"pong\n")

    # ------------------------------------------------------------- processing

    def begin(self):
        """Read the request in the segment and answer with the port of this server."""
        try:
            self.request, self.client_ip, self.ticket = self.segment.read_request()
        except ValueError as e:
            log.warning("errtraitement: %s", e)
            self.segment.write_status("errtraitement", 0)
            return False
        log.info("debuttraitement: %s for %s", self.request, self.client_ip)
        self.segment.write_status("debuttraitement", config.WORKER_PORT)
        return True

    def accept_client(self):
        """Wait for the client announced by d. Other addresses are refused."""
        deadline = time.monotonic() + 2 * config.CLIENT_TIMEOUT
        while time.monotonic() < deadline:
            ready, _, _ = select.select([self.server], [], [], 0.5)
            answer_watchdog(self.mq, SECONDAIRE)
            if ready:
                conn, addr = self.server.accept()
                if addr[0] == self.client_ip:
                    return conn
                log.warning("connection from an unexpected address refused")
                conn.close()
        return None

    def serve_client(self):
        """Serve the client directly. Return the final status of the request."""
        conn = self.accept_client()
        if conn is None:
            log.warning("errtraitement: the client did not come")
            return "errtraitement"
        with conn:
            conn.settimeout(config.CLIENT_TIMEOUT)
            reader = conn.makefile("rb")
            try:
                ticket = recv_line(reader)
                # Constant-time comparison of the one-time ticket.
                if not hmac.compare_digest(ticket.encode(), self.ticket.encode()):
                    log.warning("errtraitement: wrong ticket")
                    send_line(conn, "refus")
                    return "errtraitement"
                data = recv_line(reader)
                time.sleep(config.REQUEST_TYPES[self.request])   # simulated processing
                send_line(conn, f"resultat {self.request} : {len(data)} caractères traités")
            except (OSError, ValueError) as e:
                log.warning("errtraitement: %s", e)
                return "errtraitement"
            finally:
                reader.close()
                self.ticket = None
        log.info("fintraitement: %s", self.request)
        return "fintraitement"
