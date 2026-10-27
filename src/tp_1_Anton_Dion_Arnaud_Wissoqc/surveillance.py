"""Communication between the watchdog and the servers: System V message queue.

Message types (the type selects the receiver):
    1   watchdog -> principal server (d)    "ping <n>"
    2   watchdog -> secondary server (w)    "ping <n>"
    3   d -> watchdog                       "pong <n>"
    4   w -> watchdog                       "pong <n>"

The servers answer with answer_watchdog() in their main loop, without blocking.
"""
import logging

import sysv_ipc

PRINCIPAL = 1
SECONDAIRE = 2
REPLY_OFFSET = 2            # type of the answer = type of the ping + 2
MAX_SIZE = 32               # maximum size of one message, in bytes

log = logging.getLogger("surveillance")


def create_queue():
    """Random key, exclusive creation, owner-only rights, bounded messages."""
    return sysv_ipc.MessageQueue(None, sysv_ipc.IPC_CREX, mode=0o600,
                                 max_message_size=MAX_SIZE)


def answer_watchdog(mq, role):
    """Answer every pending ping of the watchdog, without blocking."""
    while True:
        try:
            message, _ = mq.receive(block=False, type=role)
        except sysv_ipc.BusyError:          # no pending message
            return
        words = message.split()
        if len(words) != 2 or words[0] != b"ping" or not words[1].isdigit():
            log.warning("invalid message ignored on the message queue")
            continue
        try:
            mq.send(b"pong " + words[1], block=False, type=role + REPLY_OFFSET)
        except sysv_ipc.BusyError:
            log.warning("message queue full, pong not sent")
