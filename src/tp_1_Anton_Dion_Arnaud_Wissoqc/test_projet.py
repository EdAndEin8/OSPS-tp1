"""Functional test. Start the servers first, then: uv run tp-1-test

1. three requests in a row: each one must be processed;
2. three clients at the same time, each one in its own process (threads are
   forbidden by the subject): w is unique, so one client is served and the two
   others receive "occupe".
"""
import os

from .client import send_request
from .systeme import fork_process

SERVED, BUSY, ERROR = 0, 3, 1


def child_request(i):
    """Body of one simultaneous client. Its result is given by its exit code."""
    try:
        answer = send_request("requetetype2", f"client simultané {i}")
    except (OSError, ValueError):
        return ERROR
    if answer.startswith("resultat"):
        return SERVED
    return BUSY if answer == "occupe" else ERROR


def main():
    ok = True
    print("1) Requêtes successives")
    for i, request in enumerate(("requetetype1", "requetetype2", "requetetype1")):
        try:
            answer = send_request(request, f"données {i}")
        except (OSError, ValueError) as e:
            answer = f"erreur : {e}"
        print("  ", answer)
        ok = ok and answer.startswith("resultat")

    print("2) Trois clients simultanés")
    pids = [fork_process(child_request, i) for i in range(3)]
    codes = [os.waitstatus_to_exitcode(os.waitpid(pid, 0)[1]) for pid in pids]
    served, busy = codes.count(SERVED), codes.count(BUSY)
    print(f"   servis : {served}, refusés (occupe) : {busy}, "
          f"erreurs : {len(codes) - served - busy}")
    ok = ok and served == 1 and busy == 2

    print("OK" if ok else "ÉCHEC")
    return 0 if ok else 1
