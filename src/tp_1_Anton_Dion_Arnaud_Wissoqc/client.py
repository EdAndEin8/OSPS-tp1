"""Client: sends one request to the principal server, then gets the result
directly from the secondary server.

Usage: uv run tp-1-client [requetetype1|requetetype2] [data]
"""
import socket
import sys

from . import config
from .outils import recv_one, send_line


def send_request(request, data):
    """Run the whole protocol for one request. Return the last answer received."""
    # Steps 1 to 4: request to d, which answers with the port of w and a ticket.
    with socket.create_connection((config.HOST, config.PORT), timeout=10) as sock:
        send_line(sock, request)
        answer = recv_one(sock)
    words = answer.split()
    if len(words) != 4 or words[0] != "port" or words[2] != "ticket":
        return answer                       # "occupe", "refus" or "errtraitement"
    port = int(words[1])                    # ValueError if it is not a number
    if not config.PORT < port < 65536:      # the answer of the server is checked too
        raise ValueError(f"invalid port received: {port}")

    # Steps 5 and 6: direct connection to w. Only the port comes from the server,
    # the address stays the configured one.
    with socket.create_connection((config.HOST, port), timeout=10) as sock:
        send_line(sock, words[3])           # one-time ticket
        send_line(sock, data)               # data to process
        return recv_one(sock)


def main():
    request = sys.argv[1] if len(sys.argv) > 1 else "requetetype1"
    data = sys.argv[2] if len(sys.argv) > 2 else "image satellite 42"
    if request not in config.REQUEST_TYPES:
        print("Usage : tp-1-client [requetetype1|requetetype2] [données]", file=sys.stderr)
        return 2
    try:
        answer = send_request(request, data)
    except (OSError, ValueError) as e:
        print("Erreur :", e, file=sys.stderr)
        return 1
    print("Réponse :", answer)
    return 0 if answer.startswith("resultat") else 1


if __name__ == "__main__":
    sys.exit(main())
