# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# lib
import os 
import sys

fifo1_path="/tmp/tube-tp1-securite-f" #père -> fils 
fifo2_path="/tmp/tube-tp1-securite-s" #fils -> père

def create_fifo(fifo_path):
    try:
        os.makefifo(fifo_path,0o0600) # permission bits pour le fichier
        print("The FIFO was created at {fifo2_path}")
    except FileExistsError: # retour le fichier n'existe
        raise FileExistsError(f"FIFO {fifo_path} already exists")
    except OSError as e: # erreur de création 
        raise OSError(f"Error creating FIFO: {e}")

def son(): #TODO what if : le tube est ouvert avant la creation 
    """son() is the son on wukong 
    """
    print("Launching the son")

    # Ouverture des deux fifo
    #fifo1 TODO
    try:
        
    except:


def dad():
    """dad() est une fonction réservé au serveur principal. 
    Crée les deux fifo pour communiqué via de la Share mémory 
    """
    print("Lauching the dad process")
        
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


    
    

    
def main() -> None:
    print("Lancement du projet python.\n" \
    "")
    print("Creation of the 2 pipe for the communication in between the two process.")
    #Creation des deux pipe 1 et 2 FIFO 
    #fifo1
    create_fifo(fifo1_path)
    #fifo2
    create_fifo(fifo2_path)
    # MemoryShared avec fork 
    ppid = os.fork() 
    if (ppid < 0):
        print("Fork error")
    elif ppid == 0:
        son()
    else:
        dad()
    if os.path.exists(fifo1_path): # vérification de la présence du fifo dans le système 
            print("Closing fifo1")
            os.unlink(fifo1_path)
    if os.path.exists(fifo2_path): # vérification de la présence du fifo dans le système 
            print("Closing fifo")
            os.unlink(fifo2_path)


if __name__ == "__main__":
    main()