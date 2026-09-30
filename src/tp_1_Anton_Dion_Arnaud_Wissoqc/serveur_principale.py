# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# lib
import os 
import sys

fifo_path="/tmp/tube-tp1-securite"

def son():
    print("Lancement du fils")

def dad():
    """dad() est une fonction réservé au serveur principal. 
    
    """
    print("Lancement du père")
    
    try:
        os.makefifo(fifo_path,o0600) # permission bits 
    except FileExistsError: 
        print(f"FIFO {fifo_path} already exists", file=sys.stderr)
    except OSError as e:
        print(f"Error creating FIFO: {e}", file=sys.stderr)
    finally:
        print("Tentative du fifo")
        if os.path.exists(fifo_path): # vérification de la présence du fifo dans le système 
            print("Fermeture du fifo")




def main() -> None:
    print("Lancement du projet python.\n" \
    "")
    # MS avec fork 
    ppid = os.fork()
    if (ppid < 0):
        print()
    elif ppid == 0:
        son()
    else:
        dad()


if __name__ == "__main__":
    main()