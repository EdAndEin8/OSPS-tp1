"""Project settings."""

HOST = "127.0.0.1"        # local connections only
PORT = 2222               # above 1024: no root privilege needed

MAX_CLIENTS = 5           # maximum number of secondary servers at the same time
MAX_MSG_SIZE = 512        # maximum size of one message, in bytes
CLIENT_TIMEOUT = 120      # seconds without message before a session is closed
LOCK_TIMEOUT = 5          # seconds to wait for the access to the shared memory

SHM_SIZE = 16             # two unsigned 64-bit counters: clients, messages
MAX_RESTARTS = 3          # restarts of the principal server before giving up
