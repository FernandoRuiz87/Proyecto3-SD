from helpers.Colores import *
import helpers.env as env
from os import system
import threading
import socket
import cv2
import signal
import sys

class Broker:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.server = None
        self.nodos = []  # Lista de nodos conectados
        self.clientes = []  # Lista de clientes conectados
    
    def iniciar_servidor(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.bind((self.host, self.port))
            self.server.listen()
            print(f"Servidor iniciado en {PURPLE}{self.host}:{self.port}{GRAY}")
            print("Esperando conexiones...")
        except Exception as e:
            print(f"Error al iniciar el servidor: {e}")
    
    def aceptar_conexiones(self):
        while True:
            conexion, address = self.server.accept()
            threading.Thread(target=self.procesar_conexion, args=(conexion, address)).start()
    
    def procesar_conexion(self, conexion, address):
        try:
            data = conexion.recv(1024).decode()
            if data == "[NODO]":
                self.nodos.append(conexion)
                print(f"{GREEN}Nodo{GRAY} conectado desde {address}")
            elif data == "[CLIENTE]":
                self.clientes.append(conexion)
                print(f"{CYAN}Cliente{GRAY} conectado desde {address}")
        except Exception as e:
            print(f"Error al procesar conexión: {e}")
    
    def recibir_video(self,conexion):
        try:
            while True:
                data = conexion.recv(1024)
                if not data:
                    print("Terminado")
                    break
                print("Recibiendo...")
        except Exception as e:
            print(f"Error al recibir video: {e}")
    
    

if __name__ == "__main__":
    system("cls") # Limpiar consola
    Broker = Broker(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Broker
    Broker.iniciar_servidor() # Iniciar servidor

    if Broker.server: # Si el servidor se inicio correctamente aceptar conexiones
        Broker.aceptar_conexiones()
    
    
    

