import socket, sys
from config import base_fifo_path_father_to_son, base_fifo_path_son_to_father, SHARED_MEMORY_KEY, HOST, PORT

if __name__ == "__main__":
    # Ouverture du socket
    mySocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Envoi d'une requête de connexion au serveur :
    try:
        mySocket.connect((HOST, PORT))
    except socket.error:
        print ("\nConnexion impossible à {}:{} !\n".format(HOST, PORT))
        sys.exit()    

    print ("Connexion établie avec le serveur de test ({}:{}).".format(HOST, PORT))

    stopConnexion = False

    while not stopConnexion:

        # Reception message serv
        msgServeurraw = mySocket.recv(1024)
        msgServeur = msgServeurraw.decode('UTF-8')
        if msgServeur.upper() == "FIN" or msgServeur == "":
            stopConnexion = True
            continue

        print ("Serveur>", msgServeur)

        # Envoi message du client
        msgClient = input("Client> ")
        msgClientEncoded = bytes(msgClient, 'UTF-8')
        mySocket.send(msgClientEncoded)
        if msgClient.upper() == "FIN" or msgClient == "":
            stopConnexion = True
            continue

    # Fermeture du socket (i.e. de la connexion) :
    print ("Connexion interrompue.")
    mySocket.close()
    del mySocket
    sys.exit(0)