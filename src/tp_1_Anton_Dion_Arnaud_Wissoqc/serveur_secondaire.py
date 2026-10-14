from config import fifo1_path, fifo2_path, SHARED_MEMORY_KEY

class ServeurSecondaire:

    def __init__(self):
        pass

    def start(self):
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