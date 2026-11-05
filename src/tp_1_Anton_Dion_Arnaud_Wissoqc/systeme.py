"""Process and file helpers shared by the watchdog, the servers and the test."""
import logging
import os
import time

log = logging.getLogger("systeme")


def fork_process(target, *args):
    """Run target(*args) in a child process created with fork(). Return its PID.

    The child ends with the value returned by target as exit code (1 on error).
    os._exit() is used so that the child never goes back into the code of its
    parent (loops, finally blocks): no unwanted multiple execution.
    """
    pid = os.fork()                      # raises OSError on failure
    if pid == 0:
        code = 1
        try:
            code = target(*args)
        except BaseException:
            log.exception("child process crashed")
        finally:
            os._exit(code)
    return pid


def wait_child(pid, timeout):
    """Collect a child process within `timeout` seconds (no zombie).

    Return its status (as given by os.waitpid), or None if it is still running.
    """
    deadline = time.monotonic() + timeout
    while True:
        wpid, status = os.waitpid(pid, os.WNOHANG)
        if wpid == pid:
            return status
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.2)


def remove_file(path):
    """Remove a file if it exists."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        log.debug("%s already removed", path)
