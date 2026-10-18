# Travail de la Mise en place d’un segment mémoire partagé entre un serveur principal et un serveur secondaire
# Connaitre la taille de pipe ou faire un flush 
# lib
import os 
import sys
import sysv_ipc

from serveur_secondaire import ServeurSecondaire
from config import base_fifo_path_father_to_son, base_fifo_path_son_to_father, SHARED_MEMORY_KEY

class ServeurPrincipal:

    def __init__(self):
        self.opened_secondary_server_count = 0

    def start(self):
        """
        dad() est une fonction réservé au serveur principal. 
        Crée les deux fifo pour communiquer via des tubes nommés
        """
        print("Lauching the dad process")
        # TODO créer la message queue avec le watchdog

    def create_fifo(self, path_list):
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
            
    def erase_fifo(self, path_list):
        """
        erase fifo from a list of path
        """
        for fifo_path in path_list:
            if os.path.exists(fifo_path): # vérification de la présence du fifo dans le système 
                print(f"Closing the fifo from {fifo_path} .")
                os.unlink(fifo_path)
    
    def open_secondary_server(self):
        # Incrémenter le nb de serveur ouvert pour avoir plusieurs tubes en même temps
        self.opened_secondary_server_count +=1
        
        print("Tentative du fifo")

        # Création des 2 tubes nommés
        fifo1_path = base_fifo_path_father_to_son + str(self.opened_secondary_server_count)
        fifo2_path = base_fifo_path_son_to_father + str(self.opened_secondary_server_count)
        fifo_list = [fifo1_path, fifo2_path]

        self.create_fifo(fifo_list)
        
        # Création d'un serveur secondaire
        try:
            ppid_w= os.fork()
            if ppid_w < 0 :
                print("Fork error")
            elif ppid_w == 0: # Père

                #Ouverture en écriture fifo1
                try:
                    #ouverture du tube 1
                    fifo1 = open(fifo1_path,"w")
                except FileExistsError as e:
                    raise FileExistsError(f"Error openning fifo 1 : {e}")
                except OSError as e:
                    raise OSError(f"Error system process error while openning 2 : {e}")

                #Ouverture en lecture fifo2
                try:
                    #lecture
                    fifo2 = open(fifo2_path,"r")
                except FileExistsError as e:
                    raise FileExistsError(f"Error openning fifo2: {e}")
                except OSError as e:
                    raise OSError(f"Error system process error while openning 2 : {e}")
        
                print ('Processus principal prêt pour échanger des messages...')

                # Envoi de messages au fils (test)
                for i in range(3):

                    print ('Écriture dans le tube1...');

                    fifo1.write(f"ping {i}\n")
                    fifo1.flush() # Protection deadlock

                    print ('Processus principal en attente de réception de messages...')

                    line=fifo2.readline()
                    print("Message recu : " + line)

                print ('Dad: Closing fifo1...')

                fifo1.close()

                print ('Dad: Closing fifo2...')

                fifo2.close()
                
                # Suppression des pipes avant de quitter (sinon chiant)
                self.erase_fifo(fifo_list)
                return


            else:  # Fils
                # Lancement du serveur secondaire
                son = ServeurSecondaire()
                son.start(fifo1_path, fifo2_path)
                return
        except KeyboardInterrupt as e:
            self.erase_fifo(fifo_list)
    

# Utiliser shared memory de SystemV MessageQueue