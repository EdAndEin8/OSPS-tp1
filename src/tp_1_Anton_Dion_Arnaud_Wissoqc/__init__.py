"""Entry point of the servers. Run with: uv run tp-1-anton-dion"""
import logging
import sys

from .starter_serveur import start_servers


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [pid %(process)d] %(message)s")
    logging.info("Lancement du projet python de Anton et Arnaud")
    try:
        return start_servers()
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
