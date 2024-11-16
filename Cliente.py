from helpers.Colores import *
import helpers.env as env
from os import system
import socket

class Cliente:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.client = None
    
    def conectar_a_broker(self):
        try:
            self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client.connect((self.host, self.port))
            self.client.send("[CLIENTE]".encode()) # Enviar mensaje para identificar a un cliente en el broker
        except Exception as e:
            print(f"Error al conectar con el broker: {e}")        

if __name__ == "__main__":
    system("cls") # Limpiar consola
    
    Cliente = Cliente(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Cliente
    Cliente.conectar_a_broker() # Conectar a broker
    
    input("Presiona Enter para enviar el video...") # Esperar a que el usuario presione Enter
    
    Cliente.enviar_video("video.mp4") # Enviar video
    
    input("Presiona Enter para cerrar la conexión...") # Esperar a que el usuario presione Enter
    Cliente.client.close() # Cerrar conexión