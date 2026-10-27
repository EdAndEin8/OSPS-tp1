"""Watchdog: independent supervision process.

    watchdog --fork--> principal server (d) --fork--> secondary server (w)
        |                      |                              |
        +------ System V message queue: ping / pong ----------+

The watchdog creates the message queue, the shared memory segment and the
private directory of the named pipes, then starts d (d starts w).
Every WATCHDOG_PERIOD seconds it sends "ping <n>" to d and to w and waits for
"pong <n>". A server that misses WATCHDOG_MISSES pings in a row is reported
as frozen. d is restarted when it stops on an error, w is restarted by d.

No signal is used. Ctrl+C stops every process (KeyboardInterrupt).
"""
import logging
import os
import shutil
import tempfile
import time

import sysv_ipc

from . import config
from .segment import Segment
from .serveur_principal import ServeurPrincipal
from .surveillance import PRINCIPAL, REPLY_OFFSET, SECONDAIRE, create_queue

log = logging.getLogger("watchdog")

NAMES = {PRINCIPAL: "principal server (d)", SECONDAIRE: "secondary server (w)"}


def run_principal(mq, segment, tube_dir):
    pid = os.fork()
    if pid == 0:     # child: principal server
        code = 1
        try:
            code = ServeurPrincipal(mq, segment, tube_dir).start()
        except BaseException:
            log.exception("principal server crashed")
        finally:
            os._exit(code)   # never go back into the code of the watchdog
    log.info("principal server %d started", pid)
    return pid


def ping_round(mq, n):
    """Send "ping n" to both servers. Return the set of servers that answered."""
    for role in NAMES:
        try:
            mq.send(f"ping {n}".encode(), block=False, type=role)
        except sysv_ipc.BusyError:
            log.warning("message queue full, ping not sent")
    answered = set()
    deadline = time.monotonic() + config.WATCHDOG_TIMEOUT
    while len(answered) < len(NAMES) and time.monotonic() < deadline:
        for role in NAMES:
            try:
                message, _ = mq.receive(block=False, type=role + REPLY_OFFSET)
            except sysv_ipc.BusyError:
                continue
            if message == f"pong {n}".encode():    # older answers are ignored
                answered.add(role)
        time.sleep(0.05)
    return answered


def supervise(mq, pid):
    """Supervise d until it stops. Return its status (os.waitpid)."""
    n = 0
    misses = dict.fromkeys(NAMES, 0)
    while True:
        deadline = time.monotonic() + config.WATCHDOG_PERIOD
        while time.monotonic() < deadline:
            wpid, status = os.waitpid(pid, os.WNOHANG)
            if wpid == pid:
                return status
            time.sleep(0.2)
        n += 1
        answered = ping_round(mq, n)
        for role, name in NAMES.items():
            if role in answered:
                if misses[role] >= config.WATCHDOG_MISSES:
                    log.info("%s answers again", name)
                misses[role] = 0
                continue
            misses[role] += 1
            if misses[role] == config.WATCHDOG_MISSES:
                log.critical("%s frozen: no answer to %d pings", name, misses[role])
            else:
                log.warning("%s did not answer ping %d", name, n)


def wait_principal(pid):
    """d stops on Ctrl+C as well: wait for it, within a bounded delay."""
    for _ in range(75):                      # at most 15 seconds
        wpid, _status = os.waitpid(pid, os.WNOHANG)
        if wpid == pid:
            return
        time.sleep(0.2)
    log.error("principal server %d still running", pid)


def watch(mq, tube_dir):
    restarts = 0
    while True:
        segment = Segment()                  # new segment for each principal server
        try:
            started = time.monotonic()
            pid = run_principal(mq, segment, tube_dir)
            try:
                status = supervise(mq, pid)
            except KeyboardInterrupt:
                wait_principal(pid)
                return 0
        finally:
            segment.destroy()

        code = os.waitstatus_to_exitcode(status)
        if code == 0:
            log.info("principal server stopped normally")
            return 0
        log.warning("principal server ended (code %d)", code)
        if time.monotonic() - started > 60:
            restarts = 0
        restarts += 1
        if restarts > config.MAX_RESTARTS:
            log.critical("too many restarts, giving up")
            return 1
        log.warning("restart in %.0f s", config.RESTART_DELAY)
        time.sleep(config.RESTART_DELAY)


def start_servers():
    """Return the exit code of the program."""
    if os.geteuid() == 0:
        log.critical("do not run the servers as root")
        return 1
    os.umask(0o077)          # every file created is readable by its owner only
    try:
        mq = create_queue()
        tube_dir = tempfile.mkdtemp(prefix="osps-")   # mode 0700, random name
    except (OSError, sysv_ipc.Error) as e:
        log.critical("cannot create the resources: %s", e)
        return 1
    try:
        return watch(mq, tube_dir)
    except KeyboardInterrupt:
        return 0
    except (OSError, sysv_ipc.Error) as e:
        log.critical("stopped on error: %s", e)
        return 1
    finally:
        shutil.rmtree(tube_dir)
        mq.remove()
