from config import SHARED_MEMORY_KEY

class ServeurSecondaire:

    def __init__(self):
        pass

    def start(self, fifo1_path, fifo2_path, connexion, adresse):
        """
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
            raise OSError(f"Error system process error while openning 1 : {e}")

        #Ouverture en lecture fifo1
        try:
            #lecture
            fifo2 = open(fifo2_path,"w")
        except FileExistsError as e:
            raise FileExistsError(f"Error openning fifo2: {e}")
        except OSError as e:
            raise OSError(f"Error system process error while openning 2 : {e}")

        print ('Processus secondaire prêt pour échanger des messages...')
        
        #Réception et envoie de message entre père et fils
        # for i in range(3):

        #     print ('Processus secondaire en attente de réception de messages...')

        #     line=fifo1.readline()
        #     print ("Message recu : " + line)


        #     print ('Écriture dans le tube2...');

        #     fifo2.write(f"pong {i}\n")
        #     fifo2.flush()

        # Envoi d'un premier texte au client pour dire commencer la discussion
        connexion.send(bytes('Vous etes connecte au serveur de test. Envoyez vos messages (sur une ligne) ou le mot FIN pour terminer.', 'UTF-8'))

        stopConnexion = False

        # Boucle de discussion avec le client
        while not stopConnexion:

            # Réception et décodage du message
            msgClientraw = connexion.recv(1024)
            msgClient = msgClientraw.decode('UTF-8')
            print ('Client>', msgClient)

            # Si client demande la fin, on arrete tout
            if msgClient.upper() == 'FIN' or msgClient =='':
                # Envoi du message de fin au serv principal
                fifo2.write(msgClient + "\n")
                fifo2.flush()
                # Pas besoin d'envoyer au client, il sait déjà
                stopConnexion = True
                continue

            msgServeur = input('Serveur> ')

            # Si serveur demande la fin, on arrete tout
            if msgServeur.upper() == 'FIN' or msgServeur =='':

                # Envoi du message de fin au serv principal
                fifo2.write(msgServeur + "\n")
                fifo2.flush()

                # Envoi du message de fin auclienterv principal
                msgServeurEncoded = bytes("FIN", 'UTF-8')
                connexion.send(msgServeurEncoded)
                stopConnexion = True
                continue
            
            # Envoi du message reçu au serv principal
            fifo2.write(msgServeur + "\n")
            fifo2.flush()

            # En attente du message "OK" du serv principal
            line=fifo1.readline()

            # SI serv principal pas ok, on envoie la fin au client
            if "OK" not in line:
                msgServeurEncoded = bytes("FIN", 'UTF-8')
                connexion.send(msgServeurEncoded)
                stopConnexion = True
                continue

            # Si serv principal ok, on envoie le message au client
            msgServeurEncoded = bytes(msgServeur, 'UTF-8')
            connexion.send(msgServeurEncoded)


        print ('Son: Closing fifo1...')

        fifo1.close()

        print ('Son: Closing fifo2...')

        fifo2.close()