"""Watchdog: independent supervision process.

    watchdog --fork--> principal server (d) --fork--> secondary server (w)

The watchdog creates the shared memory segment and the private directory of the
named pipes, then starts d (d starts w). It waits for d with waitpid(): when d
is down because of an error, it is restarted with a new segment. w is checked
and restarted by d, its parent: only the parent of a process can collect it.

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
from .systeme import fork_process, wait_child

log = logging.getLogger("watchdog")


def run_principal(segment, tube_dir):
    """Body of the child process: the principal server."""
    return ServeurPrincipal(segment, tube_dir).start()


def watch(tube_dir):
    """Start d and restart it when it is down. Return the exit code."""
    restarts = 0
    while True:
        segment = Segment()                  # new segment for each principal server
        try:
            pid = fork_process(run_principal, segment, tube_dir)
            log.info("principal server %d started", pid)
            try:
                _, status = os.waitpid(pid, 0)   # blocks until d stops
            except KeyboardInterrupt:
                # d stops on Ctrl+C as well: wait for it, within a bounded delay.
                if wait_child(pid, 15) is None:
                    log.error("principal server %d still running", pid)
                return 0
        finally:
            segment.destroy()

        code = os.waitstatus_to_exitcode(status)
        if code == 0:
            log.info("principal server stopped normally")
            return 0
        restarts += 1
        if restarts > config.MAX_RESTARTS:
            log.critical("principal server down (code %d), too many restarts", code)
            return 1
        log.warning("principal server down (code %d), restart in %.0f s",
                    code, config.RESTART_DELAY)
        time.sleep(config.RESTART_DELAY)


def start_servers():
    """Return the exit code of the program."""
    if os.geteuid() == 0:
        log.critical("do not run the servers as root")
        return 1
    os.umask(0o077)          # every file created is readable by its owner only
    try:
        tube_dir = tempfile.mkdtemp(prefix="osps-")   # mode 0700, random name
    except OSError as e:
        log.critical("cannot create the directory of the pipes: %s", e)
        return 1
    try:
        return watch(tube_dir)
    except KeyboardInterrupt:
        return 0
    except (OSError, sysv_ipc.Error) as e:
        log.critical("stopped on error: %s", e)
        return 1
    finally:
        shutil.rmtree(tube_dir)
