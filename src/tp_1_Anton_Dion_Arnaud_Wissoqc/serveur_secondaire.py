import logging
import os
import socket
import time

import sysv_ipc

from . import config
from .outils import recv_line, send_line

log = logging.getLogger("secondaire(w)")

# Messages of the ping-pong on dwtube1 /wdtube1 (also used by d).
PING = b"ping\n"
PONG = b"pong\n"
STOP = b"stop\n"


class ServeurSecondaire:

    def __init__(self, segment, dwtube, wdtube):
        self.segment = segment
        self.dwtube = dwtube        # read end of dwtube1: principal -> secondary
        self.wdtube = wdtube        # write end of wdtube1: secondary -> principal
        self.server = None
        self.request = None
        self.client_ip = None


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
        message = os.read(self.dwtube, 16)
        if message == PING:
            return True
        
        if message in (b"", STOP):
            log.info("stop requested by the principal server")
            return False
        
        raise ValueError(f"unexpected message on dwtube1: {message!r}")

    def pong(self):
        """Give the segment back to the principal server."""
        os.write(self.wdtube, PONG)

    # ------------------------------------------------------------- processing

    def begin(self):
        """Read the request in the segment and answer with the port of this server."""
        try:
            self.request, self.client_ip = self.segment.read_request()

        except ValueError as e:
            log.warning("errtraitement: %s", e)
            self.segment.write_status("errtraitement", 0)
            return False
        
        log.info("debuttraitement: %s for %s", self.request, self.client_ip)
        self.segment.write_status("debuttraitement", config.WORKER_PORT)
        return True

    def serve_client(self):
        """Serve the client announced by d. Return the final status of the request."""
        self.server.settimeout(2 * config.CLIENT_TIMEOUT)

        try:
            conn, addr = self.server.accept()

        except TimeoutError:
            log.warning("errtraitement: the client did not come")
            return "errtraitement"
        
        with conn:
            if addr[0] != self.client_ip:
                log.warning("errtraitement: connection from an unexpected address")
                return "errtraitement"
            
            conn.settimeout(config.CLIENT_TIMEOUT)
            reader = conn.makefile("rb")

            try:
                data = recv_line(reader)
                time.sleep(config.REQUEST_TYPES[self.request])   # simulated processing
                send_line(conn, f"resultat {self.request} : {len(data)} caractères traités")

            except (OSError, ValueError) as e:
                log.warning("errtraitement: %s", e)
                return "errtraitement"
            
            finally:
                reader.close()
                
        log.info("fintraitement: %s", self.request)
        return "fintraitement"
