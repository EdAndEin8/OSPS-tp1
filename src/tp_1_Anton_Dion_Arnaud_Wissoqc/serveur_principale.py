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
    #pipe_fifo
    try:
        os.makefifo(fifo_path,0o0600) # permission bits pour le fichier
    except FileExistsError: # retour le fichier n'existe
        raise FileExistsError(f"FIFO {fifo_path} already exists", file=sys.stderr)
    except OSError as e: # erreur de création 
        raise OSError(f"Error creating FIFO: {e}", file=sys.stderr)

    print("Tentative du fifo")
    #écriture fifo
    try:
        #ouverture
        open(fifo_path,"w")
    except FileExistsError:
        raise FileExistsError(f"Error openning fifo: {e}",file=sys.stderr)
    except OSError:
        raise OSError(f"Error system process error while openning : {e}",file=sys.stderr)
    finally:
        #ecriture
        texte=""
    
    if os.path.exists(fifo_path): # vérification de la présence du fifo dans le système 
        print("Closing fifo")

    
    
print

def main() -> None:
    print("Lancement du projet python.\n" \
    "")
    # MemoryShared avec fork 
    ppid = os.fork() 
    if (ppid < 0):
        print("Fork error")
    elif ppid == 0:
        son()
    else:
        dad()


if __name__ == "__main__":
    main()