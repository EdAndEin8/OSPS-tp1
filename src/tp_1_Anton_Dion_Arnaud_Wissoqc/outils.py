"""Text protocol of the sockets, shared by the client and the servers.
mesage control + string handling 
"""
from . import config


def check_message(text):
    """Raise ValueError if the message is too long or contains control characters."""
    if len(text.encode("utf-8")) > config.MAX_MSG_SIZE:
        raise ValueError("message too long")
    if not text.isprintable(): #UTF-8 protection
        raise ValueError("control characters are not allowed")


def send_line(sock, text):
    check_message(text)
    sock.sendall((text + "\n").encode("utf-8"))


def recv_line(reader):
    """Read one message from reader = sock.makefile("rb")."""
    raw = reader.readline(config.MAX_MSG_SIZE + 1)
    if raw == b"":
        raise ConnectionError("connection closed by the other side")
    if not raw.endswith(b"\n"):
        raise ValueError("message too long or incomplete")
    text = raw[:-1].decode("utf-8")      # UnicodeDecodeError is a ValueError [:1]  in order to erase \n
    check_message(text)
    return text


def recv_one(sock):
    """Read a single message from a socket."""
    reader = sock.makefile("rb")
    try:
        return recv_line(reader)
    finally:
        reader.close()
