# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# Connaitre la taille de pipe ou faire un flush 
# lib
import os 
import sys
import sysv_ipc

from config import fifo1_path, fifo2_path, SHARED_MEMORY_KEY

class ServeurPrincipal:

    def __init__(self):
        pass

    def start(self):
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
    

# Utiliser shared memory de SystemV MessageQueue