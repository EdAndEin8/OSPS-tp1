"""Shared memory segment between the principal server (d) and the secondary server (w).

Fixed layout:
    request   16 bytes   request type sent by the client            (written by d)
    client    16 bytes   IPv4 address of the client                 (written by d)
    ticket    16 bytes   one-time ticket given to the client        (written by d, erased by w)
    status    16 bytes   debuttraitement / fintraitement / errtraitement   (written by w)
    port       2 bytes   port of the secondary server               (written by w)

No locking here: the segment is only used by the process that received the
last message of the ping-pong on dwtube1 / wdtube1 (see serveur_principal.py).
"""
import logging
import struct

import sysv_ipc

from . import config

log = logging.getLogger("segment")

LAYOUT = struct.Struct("=16s16s16s16sH")
FIELD_SIZE = 16
STATUSES = ("debuttraitement", "fintraitement", "errtraitement")


def _encode(text):
    raw = text.encode("ascii")
    if len(raw) > FIELD_SIZE:                 # struct would truncate it silently
        raise ValueError("field too long for the shared memory")
    return raw


def _decode(raw):
    return raw.rstrip(b"\0").decode("ascii")  # UnicodeDecodeError is a ValueError


class Segment:

    def __init__(self):
        # Random key (None) and exclusive creation (IPC_CREX): another user cannot
        # create the segment first. Rights 0600: owner only. sysv_ipc fills a new
        # segment with spaces by default, it is filled with zeros instead.
        self.memory = sysv_ipc.SharedMemory(None, sysv_ipc.IPC_CREX, mode=0o600,
                                            size=LAYOUT.size, init_character=b"\0")

    def _read(self):
        fields = LAYOUT.unpack(self.memory.read(LAYOUT.size))
        return [_decode(field) for field in fields[:4]] + [fields[4]]

    def _write(self, request, client, ticket, status, port):
        self.memory.write(LAYOUT.pack(_encode(request), _encode(client), _encode(ticket),
                                      _encode(status), port))

    def write_request(self, request, client, ticket):
        """Used by d before "ping"."""
        self._write(request, client, ticket, "", 0)

    def read_request(self):
        """Used by w after "ping". The content is checked: it is not trusted."""
        request, client, ticket, _, _ = self._read()
        if request not in config.REQUEST_TYPES or len(ticket) != FIELD_SIZE:
            raise ValueError("invalid request in the shared memory")
        return request, client, ticket

    def write_status(self, status, port):
        """Used by w before "pong". The request and the ticket are erased."""
        self._write("", "", "", status, port)

    def read_status(self):
        """Used by d after "pong"."""
        _, _, _, status, port = self._read()
        if status not in STATUSES:
            raise ValueError("invalid status in the shared memory")
        return status, port

    def clear(self):
        self.memory.write(b"\0" * LAYOUT.size)

    def destroy(self):
        """Erase the content, then detach and remove the segment (creator only)."""
        try:
            self.clear()
            self.memory.detach()
            self.memory.remove()
        except sysv_ipc.Error as e:
            log.error("cannot remove the shared memory: %s", e)
