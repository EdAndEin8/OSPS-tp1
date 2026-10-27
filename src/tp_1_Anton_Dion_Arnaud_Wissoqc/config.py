"""Project settings."""

HOST = "127.0.0.1"          # local connections only
PORT = 2222                 # principal server (d)
WORKER_PORT = 2223          # secondary server (w), given to the client by d

MAX_MSG_SIZE = 256          # maximum size of one message, in bytes

# Request types accepted from the clients, with their simulated processing time
# in seconds. It must stay shorter than WATCHDOG_TIMEOUT (see the subject, part 4).
REQUEST_TYPES = {"requetetype1": 0.5, "requetetype2": 1.0}

CLIENT_TIMEOUT = 2.0        # maximum wait for a message of a client
WORKER_TIMEOUT = 10.0       # maximum duration of one exchange with the secondary server
MAX_REQUESTS = 0            # stop the servers after this number of requests (0: no limit)

WATCHDOG_PERIOD = 5.0       # seconds between two pings of the watchdog
WATCHDOG_TIMEOUT = 3.0      # maximum wait for the answer to a ping
WATCHDOG_MISSES = 3         # unanswered pings in a row before a server is said frozen

MAX_RESTARTS = 3            # restarts in a row before giving up
RESTART_DELAY = 2.0         # seconds before a restart
