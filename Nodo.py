import helpers.env as env
from os import system
import socket

system("cls")

class Nodo:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.nodo = None

    def conectar_broker(self): # Conectar a broker
        try:
            self.nodo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.nodo.connect((self.host, self.port))
            self.nodo.send("[NODO]".encode()) # Enviar mensaje para identificar a un nodo en el broker
        except Exception as e:
            print(f"Error al conectar con el broker: {e}")

if __name__ == "__main__":
    system("cls")

    Nodo = Nodo(env.BROKER_HOST, env.BROKER_PORT)
    Nodo.conectar_broker()

    input("Presiona Enter para cerrar la conexión...")
    Nodo.nodo.close()