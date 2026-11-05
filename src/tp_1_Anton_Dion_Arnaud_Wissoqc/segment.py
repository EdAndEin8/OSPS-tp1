
"""Shared memory used by the main server (d) and the secondary server (w).

The memory has a fixed layout:
    request   16 bytes   Request type sent by the client (written by d)
    client    16 bytes   Client IPv4 address (written by d)
    status    16 bytes   Processing status (written by w)
    port       2 bytes   Secondary server port (written by w)

No lock is needed because only the process that received the last
ping-pong message can access the shared memory.
See serveur_principal.py for more details.
"""

import logging
import struct

import sysv_ipc

from . import config

log = logging.getLogger("segment")

# Define the size and order of each field in shared memory.
LAYOUT = struct.Struct("=16s16s16sH")

# Maximum size of each text field in bytes.
FIELD_SIZE = 16

# Allowed processing statuses.
STATUSES = ("debuttraitement", "fintraitement", "errtraitement")


def _encode(text):
    """Convert text into ASCII bytes.

    Raise an error if the text is too long to fit in the field.
    """
    raw = text.encode("ascii")

    if len(raw) > FIELD_SIZE:
        raise ValueError("field too long for the shared memory")

    return raw


def _decode(raw):
    """Convert ASCII bytes into text.

    Remove the empty bytes at the end before decoding.
    """
    return raw.rstrip(b"\0").decode("ascii")


class Segment:

    def __init__(self):
        """Create a new shared memory segment.

        Use a random key and exclusive creation.
        Only the owner can access it (permissions 0600).
        New memory is filled with zeros.
        """
        self.memory = sysv_ipc.SharedMemory(
            None,
            sysv_ipc.IPC_CREX,
            mode=0o600,
            size=LAYOUT.size,
            init_character=b"\0"
        )

    def _read(self):
        """Read all fields from shared memory.

        Return the request, client address, status and port.
        """
        request, client, status, port = LAYOUT.unpack(
            self.memory.read(LAYOUT.size)
        )

        return (
            _decode(request),
            _decode(client),
            _decode(status),
            port
        )

    def _write(self, request, client, status, port):
        """Write all fields into shared memory.

        Convert text fields into bytes and pack them into the memory layout.
        """
        self.memory.write(
            LAYOUT.pack(
                _encode(request),
                _encode(client),
                _encode(status),
                port
            )
        )

    def write_request(self, request, client):
        """Write the client request and IP address.

        Called by the main server (d) before sending a ping.
        """
        self._write(request, client, "", 0)

    def read_request(self):
        """Read and check the client request.

        Called by the secondary server (w) after receiving a ping.
        Check that the request type is allowed.
        """
        request, client, _, _ = self._read()

        if request not in config.REQUEST_TYPES:
            raise ValueError("invalid request in the shared memory")

        return request, client

    def write_status(self, status, port):
        """Write the processing status and server port.

        Called by the secondary server (w) before sending a pong.
        """
        self._write("", "", status, port)

    def read_status(self):
        """Read and check the processing status.

        Called by the main server (d) after receiving a pong.
        Check that the status is valid.
        """
        _, _, status, port = self._read()

        if status not in STATUSES:
            raise ValueError("invalid status in the shared memory")

        return status, port

    def clear(self):
        """Fill the entire shared memory with zeros."""
        self.memory.write(b"\0" * LAYOUT.size)

    def destroy(self):
        """Clear and remove the shared memory segment.

        Detach from the memory and remove the segment.
        Only the process that created it can remove it.
        """
        try:
            self.clear()
            self.memory.detach()
            self.memory.remove()

        except sysv_ipc.Error as e:
            log.error("cannot remove the shared memory: %s", e)
