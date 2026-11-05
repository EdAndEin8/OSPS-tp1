"""Project settings."""

HOST = "127.0.0.1"          # local connections only
PORT = 2222                 # principal server (d)
WORKER_PORT = 2223          # secondary server (w), given to the client by d

MAX_MSG_SIZE = 256          # maximum size of one message, in bytes

# Request types accepted from the clients, with their simulated processing time
# in seconds.
REQUEST_TYPES = {"requetetype1": 0.5, "requetetype2": 1.0}

CLIENT_TIMEOUT = 2.0        # maximum wait for a message of a client
MAX_REQUESTS = 0            # stop the servers after this number of requests (0: no limit)

MAX_RESTARTS = 3            # restarts in a row before giving up
RESTART_DELAY = 2.0         # seconds before a restart
