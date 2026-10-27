"""Client: sends one line per message and displays the answers of the server.

Run with: uv run tp-1-client
"""
import socket
import sys

from . import config
from .outils import check_message, recv_line, send_line


def main():
    try:
        mySocket = socket.create_connection((config.HOST, config.PORT), timeout=10)
    except OSError as e:
        print(f"Connexion impossible à {config.HOST}:{config.PORT} : {e}", file=sys.stderr)
        return 1

    with mySocket:
        mySocket.settimeout(30)
        reader = mySocket.makefile("rb")
        try:
            msgServeur = recv_line(reader)
            if msgServeur == "FIN":
                print("Connexion refusée : serveur plein.", file=sys.stderr)
                return 1
            print("Serveur>", msgServeur)

            while True:
                msgClient = input("Client> ")
                if not msgClient:
                    continue
                try:
                    check_message(msgClient)
                except ValueError as e:
                    print("Message refusé :", e)
                    continue
                send_line(mySocket, msgClient)
                if msgClient.upper() == "FIN":
                    print("Connexion terminée.")
                    return 0

                msgServeur = recv_line(reader)
                if msgServeur == "FIN":
                    print("Le serveur a fermé la connexion.")
                    return 0
                print("Serveur>", msgServeur)

        except (OSError, ValueError) as e:
            print("Erreur :", e, file=sys.stderr)
            return 1
        except (KeyboardInterrupt, EOFError):
            print("\nConnexion interrompue.")
            return 0
        finally:
            reader.close()


if __name__ == "__main__":
    sys.exit(main())
