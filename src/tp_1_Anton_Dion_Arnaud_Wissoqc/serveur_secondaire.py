"""Secondary server: handles the session of one client.

The shared memory holds two counters (clients, messages). It is only read or
written between a "REQ" -> "OK" exchange and a "REL" message on the pair of named
pipes shared with the principal server: this is the mutual exclusion.
"""
import logging
import os
import select
import struct

from . import config
from .outils import recv_line, send_line

COUNTERS = struct.Struct("=QQ")


class ServeurSecondaire:

    def __init__(self, memory, fifo_read, fifo_write):
        self.memory = memory
        self.fifo_read = fifo_read        # principal -> secondary
        self.fifo_write = fifo_write      # secondary -> principal

    def lock(self):
        """Ask the principal server for the exclusive access to the shared memory."""
        os.write(self.fifo_write, b"REQ\n")
        ready, _, _ = select.select([self.fifo_read], [], [], config.LOCK_TIMEOUT)
        if not ready:
            raise TimeoutError("no answer from the principal server")
        if os.read(self.fifo_read, 16) != b"OK\n":     # b"" when the principal is gone
            raise ConnectionError("bad answer from the principal server")

    def unlock(self):
        os.write(self.fifo_write, b"REL\n")

    def update_counters(self, new_client=0, new_message=0):
        """Add to the counters in shared memory and return their new values."""
        self.lock()
        try:
            clients, messages = COUNTERS.unpack(self.memory.read(COUNTERS.size))
            clients += new_client
            messages += new_message
            self.memory.write(COUNTERS.pack(clients, messages))
        finally:
            self.unlock()                 # the access is released even after an error
        return clients, messages

    def start(self, connexion, adresse):
        """Run the session and return the exit code of the process."""
        client = f"{adresse[0]}:{adresse[1]}"
        connexion.settimeout(config.CLIENT_TIMEOUT)    # a silent client cannot block forever
        reader = connexion.makefile("rb")
        try:
            clients, _ = self.update_counters(new_client=1)
            send_line(connexion, f"Bienvenue, vous êtes le client n°{clients}. "
                                 "Envoyez vos messages (une ligne) ou FIN pour terminer.")
            while True:
                msgClient = recv_line(reader)
                if msgClient.upper() == "FIN":        # exact word: "FINANCE" is a message
                    logging.info("client %s ended the session", client)
                    return 0
                _, messages = self.update_counters(new_message=1)
                # Only the size is logged, not the content (data minimisation).
                logging.info("client %s: message of %d characters", client, len(msgClient))
                send_line(connexion, f"Message n°{messages} reçu ({len(msgClient)} caractères)")
        except (OSError, ValueError) as e:            # includes timeouts and disconnections
            logging.warning("client %s: session closed (%r)", client, str(e)[:80])
            self.say_goodbye(connexion)
            return 1
        except KeyboardInterrupt:
            self.say_goodbye(connexion)
            return 0
        finally:
            reader.close()
            connexion.close()
            os.close(self.fifo_read)
            os.close(self.fifo_write)

    @staticmethod
    def say_goodbye(connexion):
        """Tell the client that the session is over, if it is still connected."""
        try:
            connexion.settimeout(1)
            send_line(connexion, "FIN")
        except OSError as e:
            logging.debug("FIN not sent: %s", e)
