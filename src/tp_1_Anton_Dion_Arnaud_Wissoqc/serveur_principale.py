# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# Connaitre la taille de pipe ou faire un flush 
# lib
import os 
import sys
import sysv_ipc

from config import fifo1_path, fifo2_path, SHARED_MEMORY_KEY

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

def son():
    """son() is the son on wukong 
    """
    print("Launching the son process")

    print("Tentative du fifo")
    #Ouverture en écriture fifo1
    try:
        #ouverture
        fifo1 = open(fifo1_path,"r")
    except FileExistsError as e:
        raise FileExistsError(f"Error openning fifo 1 : {e}")
    except OSError as e:
        raise OSError(f"Error system process error while openning 2 : {e}")

    #Ouverture en lecture fifo1
    try:
        #lecture
        fifo2 = open(fifo2_path,"w")
    except FileExistsError as e:
        raise FileExistsError(f"Error openning fifo2: {e}")
    except OSError as e:
        raise OSError(f"Error system process error while openning 2 : {e}")

    #Réception et envoie de message entre père et fils
    for i in range(3):

        print ('Processus secondaire prêt pour échanger des messages...')

        print ('Processus secondaire en attente de réception de messages...')

        line=fifo1.readline()
        print ("Message recu : " + line)


        print ('Écriture dans le tube2...');

        fifo2.write(f"pong {i}\n")
        fifo2.flush()


    print ('Son: Closing fifo1...');

    fifo1.close()

    print ('Son: Closing fifo2...');

    fifo2.close()

def dad():
    """
    dad() est une fonction réservé au serveur principal. 
    Crée les deux fifo pour communiqué via de la Share mémory 
    """
    print("Lauching the dad process")
        
    print("Tentative du fifo")
    #Ouverture en écriture fifo1
    try:
        #ouverture
        fifo1 = open(fifo1_path,"w")
    except FileExistsError as e:
        raise FileExistsError(f"Error openning fifo 1 : {e}")
    except OSError as e:
        raise OSError(f"Error system process error while openning 2 : {e}")

    print("Tentative du fifo")
    #Ouverture en lecture fifo1
    try:
        #lecture
        fifo2 = open(fifo2_path,"r")
    except FileExistsError as e:
        raise FileExistsError(f"Error openning fifo2: {e}")
    except OSError as e:
        raise OSError(f"Error system process error while openning 2 : {e}")

    for i in range(3):

        print ('Processus principal prêt pour échanger des messages...')

        print ('Écriture dans le tube1...');

        fifo1.write(f"ping {i}\n")
        fifo1.flush() # Protection deadlock

        print ('Processus principal en attente de réception de messages...')

        line=fifo2.readline()
        print("Message recu : " + line)

    print ('Dad: Closing fifo1...');

    fifo1.close()

    print ('Dad: Closing fifo2...');

    fifo2.close()


def main() -> None:
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
        create_fifo([fifo1_path,fifo2_path])
        
        # MemoryShared avec fork 
        ppid = os.fork() 
        if (ppid < 0):
            print("Fork error")
        elif ppid == 0:
            son()
        else:
            dad()
        erase_fifo([fifo1_path,fifo1_path])
    except KeyboardInterrupt as e:
        erase_fifo(fifo_list)


if __name__ == "__main__":
    main()
    

# Utiliser shared memory de SystemV MessageQueue