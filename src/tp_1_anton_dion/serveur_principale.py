# lib
import os 

path='/tmp/tube-tp1-securite'

def son():
    print("Lancement du fils")

def dad():
    print("Lancement du père")
    try:
        os.makefifo(path,o0600) # permission bits 
    except FileExistsError:
        print("",,sys= )



def main() -> None:
    print("Lancement du projet python.")
    # MS avec fork 
    ppid = fork()
    if (ppid < 0):
        print()
    elif ppid == 0:
        son()
    else:
        dad()


if __name__ == "__main__":
    main()