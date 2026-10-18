"""
Watchdog fork le serveur principal. TODO
Le serveur principal fork le serveur secondaire. 
"""
import os 
import sys
import sysv_ipc

from config import SHARED_MEMORY_KEY
from serveur_principal import ServeurPrincipal

def start_servers() -> None:
    print("Lancement du projet python de Anton et Arnaud.\n--------------------------")
    # Watchdog
    # SharedMemory
    try:
        memory = sysv_ipc.SharedMemory(SHARED_MEMORY_KEY, sysv_ipc.IPC_CREX, size=1024)
        print("[Writer] Segment de mémoire partagée créé avec succès.")
    except sysv_ipc.ExistentialError:
        # Si le segment existe déjà, on l'ouvre simplement
        memory = sysv_ipc.SharedMemory(SHARED_MEMORY_KEY)
        print("[Writer] Segment de mémoire partagée déjà existant, ouvert.")

    ppid_w= os.fork()
    if ppid_w < 0 :
         print("Fork error")
    elif ppid_w == 0: # watchdog
        pass #TODO
    else: 
        # Lancement du serveur principal
        dad = ServeurPrincipal()
        dad.start()


        # Test de création d'un serveur secondaire
        dad.open_secondary_server()
                