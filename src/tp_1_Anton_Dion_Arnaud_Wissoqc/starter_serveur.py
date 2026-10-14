import os 
import sys
import sysv_ipc

from config import fifo1_path, fifo2_path, SHARED_MEMORY_KEY
from serveur_principal import ServeurPrincipal
from serveur_secondaire import ServeurSecondaire

fifo_list= [fifo1_path,fifo2_path]

def create_fifo(path_list):
    """create fifo from a path list"""
    for fifo_path in path_list:
        try:
            os.mkfifo(fifo_path,0o0600) # permission bits pour le fichier
            print(f"The FIFO was created at {fifo_path}")
        except FileExistsError as e: # retour le fichier n'existe
            os.unlink(fifo_path)
            raise FileExistsError(f"FIFO {fifo_path} already exists")
        except OSError as e: # erreur de création 
            raise OSError(f"Error creating FIFO: {e}")

def erase_fifo(path_list):
    """
    erase fifo from a list of path
    """
    for fifo_path in path_list:
        if os.path.exists(fifo_path): # vérification de la présence du fifo dans le système 
                    print(f"Closing the fifo from {fifo_path} .")
                    os.unlink(fifo_path)

def start_servers() -> None:
    print("Lancement du projet python de Anton et Arnaud.\n--------------------------" \
    "")
    print("Creation of the 2 pipe for the communication in between the two process.")

    # SharedMemory
    try:
        memory = sysv_ipc.SharedMemory(SHARED_MEMORY_KEY, sysv_ipc.IPC_CREX, size=1024)
        print("[Writer] Segment de mémoire partagée créé avec succès.")
    except sysv_ipc.ExistentialError:
        # Si le segment existe déjà, on l'ouvre simplement
        memory = sysv_ipc.SharedMemory(SHARED_MEMORY_KEY)
        print("[Writer] Segment de mémoire partagée déjà existant, ouvert.")

    try:
        #Creation des deux pipe 1 et 2 FIFO 
        create_fifo(fifo_list)
        
        # MemoryShared avec fork 
        ppid = os.fork() 
        if (ppid < 0):
            print("Fork error")
        elif ppid == 0:
            son = ServeurSecondaire()
            son.start()
            return
        else:
            dad = ServeurPrincipal()
            dad.start()
        erase_fifo(fifo_list)
    except KeyboardInterrupt as e:
        erase_fifo(fifo_list)