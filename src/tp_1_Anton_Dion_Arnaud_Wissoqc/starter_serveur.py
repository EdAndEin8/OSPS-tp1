"""Watchdog: creates the shared memory, starts the principal server and restarts it
if it stops on an error (at most MAX_RESTARTS times).

    watchdog (this process)
     +-- principal server
          +-- one secondary server per client

Ctrl+C stops every process: Python raises KeyboardInterrupt in all of them.
"""
import logging
import os
import shutil
import tempfile
import time

import sysv_ipc

from . import config
from .serveur_principal import ServeurPrincipal


def run_principal(memory, fifo_dir):
    pid = os.fork()
    if pid == 0:     # child: principal server
        code = 1
        try:
            code = ServeurPrincipal(memory, fifo_dir).start()
        except BaseException:
            logging.exception("principal server crashed")
        finally:
            os._exit(code)   # never go back into the watchdog code
    return pid


def start_servers():
    """Return the exit code of the program."""
    if os.geteuid() == 0:
        logging.critical("do not run the servers as root")
        return 1
    os.umask(0o077)          # every file created is readable by its owner only

    # Private directory for the named pipes (mode 0700, random name) instead of
    # fixed names in /tmp, which another user could create first. It is created
    # and removed here, so it does not stay behind if the principal server crashes.
    fifo_dir = tempfile.mkdtemp(prefix="osps-")
    try:
        return watch(fifo_dir)
    finally:
        shutil.rmtree(fifo_dir)


def watch(fifo_dir):
    """Start the principal server and restart it after an error."""
    restarts = 0
    while True:
        # A new segment for each principal server, with a random key (None),
        # exclusive creation (IPC_CREX) and owner-only rights (0600).
        # The old fixed key 4321 could be created first by another user.
        # init_character: sysv_ipc fills a new segment with spaces by default,
        # the counters must start at zero.
        try:
            memory = sysv_ipc.SharedMemory(None, sysv_ipc.IPC_CREX, mode=0o600,
                                           size=config.SHM_SIZE, init_character=b"\0")
        except sysv_ipc.Error as e:
            logging.critical("cannot create the shared memory: %s", e)
            return 1
        try:
            pid = run_principal(memory, fifo_dir)
            logging.info("watchdog: principal server %d started", pid)
            try:
                _, status = os.waitpid(pid, 0)
            except KeyboardInterrupt:
                os.waitpid(pid, 0)        # the principal server also stops on Ctrl+C
                return 0
        finally:
            memory.detach()
            memory.remove()               # no segment left behind (check with ipcs -m)

        code = os.waitstatus_to_exitcode(status)
        if code == 0:
            return 0
        restarts += 1
        if restarts > config.MAX_RESTARTS:
            logging.critical("watchdog: too many restarts, giving up")
            return 1
        logging.warning("watchdog: principal server ended (code %d), restart in 2 s", code)
        time.sleep(2)
