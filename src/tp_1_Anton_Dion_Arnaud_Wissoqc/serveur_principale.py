# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# lib
import os 
import sys

fifo1_path="/tmp/tube-tp1-securite-f" #père -> fils 
fifo2_path="/tmp/tube-tp1-securite-s" #fils -> père


def son():
    print("Lancement du fils")

def dad():
    """dad() est une fonction réservé au serveur principal. 
    """
    print("Lancement du père")
    #Creation des deux pipe 1 et 2 FIFO 
    #fifo1
    try:
        os.makefifo(fifo1_path,0o0600) # permission bits pour le fichier
        print("Second FIFO created at {fifo2_path}")
    except FileExistsError: # retour le fichier n'existe
        raise FileExistsError(f"FIFO {fifo1_path} already exists", file=sys.stderr)
    except OSError as e: # erreur de création 
        raise OSError(f"Error creating FIFO: {e}", file=sys.stderr)
    #fifo2
    try:
        os.makefifo(fifo2_path,0o0600) # permission bits pour le fichier
        print("Second FIFO created at {fifo2_path}")
    except FileExistsError: # retour le fichier n'existe
        raise FileExistsError(f"FIFO {fifo2_path} already exists", file=sys.stderr)
    except OSError as e: # erreur de création 
        raise OSError(f"Error creating FIFO: {e}", file=sys.stderr)
    
    print("Tentative du fifo")
    #Ouverture en écriture fifo1
    try:
        #ouverture
        open(fifo1_path,"w")
    except FileExistsError:
        raise FileExistsError(f"Error openning fifo 1 : {e}",file=sys.stderr)
    except OSError:
        raise OSError(f"Error system process error while openning 2 : {e}",file=sys.stderr)
    finally:
        #ecriture
        texte=""
    print("Tentative du fifo")
    #Ouverture en lecture fifo1
    try:
        #lecture
        open(fifo2_path,"r")
    except FileExistsError:
        raise FileExistsError(f"Error openning fifo2: {e}",file=sys.stderr)
    except OSError:
        raise OSError(f"Error system process error while openning 2 : {e}",file=sys.stderr)
    finally:
        #ecriture
        texte=""


    
    if os.path.exists(fifo1_path): # vérification de la présence du fifo dans le système 
        print("Closing fifo")
    if os.path.exists(fifo2_path): # vérification de la présence du fifo dans le système 
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