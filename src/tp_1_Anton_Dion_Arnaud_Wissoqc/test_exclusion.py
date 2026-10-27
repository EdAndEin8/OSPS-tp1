"""Test of the mutual exclusion. Start the servers first, then: uv run tp-1-test

MAX_CLIENTS clients send 50 messages each at the same time. If the mutual
exclusion works, every message number given by the servers is different:
no update of the shared memory was lost.
"""
import re
import socket
import threading

from . import config
from .outils import recv_line, send_line

MESSAGES = 50


def client(numbers, errors, mutex):
    try:
        with socket.create_connection((config.HOST, config.PORT), timeout=10) as sock:
            reader = sock.makefile("rb")
            recv_line(reader)
            for i in range(MESSAGES):
                send_line(sock, f"test {i}")
                number = int(re.match(r"Message n°(\d+)", recv_line(reader)).group(1))
                with mutex:
                    numbers.append(number)
            send_line(sock, "FIN")
            reader.close()
    except (OSError, ValueError, AttributeError) as e:
        with mutex:
            errors.append(str(e))


def main():
    numbers = []
    errors = []
    mutex = threading.Lock()
    threads = [threading.Thread(target=client, args=(numbers, errors, mutex))
               for _ in range(config.MAX_CLIENTS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    expected = config.MAX_CLIENTS * MESSAGES
    print(f"messages: {len(numbers)}/{expected}, different numbers: {len(set(numbers))}, "
          f"errors: {len(errors)}")
    if len(set(numbers)) == expected and not errors:
        print("OK")
        return 0
    print("FAILED", errors[:3])
    return 1
