from datetime import datetime
from helpers.Colores import *
import helpers.env as env
import cv2 
import threading
import socket
import os
import uuid

class Nodo:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.conexion = None
        self.identificador = uuid.uuid4()

    def conectar_broker(self): # Conectar a broker
        try:
            self.conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conexion.connect((self.host, self.port))
            self.conexion.send("[NODO]".encode()) # Enviar mensaje para identificar a un nodo en el broker
        except Exception as e:
            print(f"Error al conectar con el broker: {e}")
    
    def manejador(self):
        while True:
            data = self.conexion.recv(1024).decode()
            
            if data == "[VIDEO]":
                print(f"{BOLD}{CYAN}Recibiendo video...{RESET}")
                # Recibir metadata del video
                video_id = self.conexion.recv(1024).decode()
                
                tamaño_video = self.conexion.recv(1024).decode()
                
                n_segmento = self.conexion.recv(1024).decode()
                
                print(f"{BOLD}{LIGHT_PURPLE}VIDEO_ID: {video_id}{RESET}")
                print(f"{BOLD}{LIGHT_PURPLE}TAMAÑO: {tamaño_video} bytes{RESET}")
                print(f"{BOLD}{LIGHT_PURPLE}N_SEGMENTO: {n_segmento}{RESET}")                
                               
                self.recibir_video(self.conexion, int(tamaño_video), video_id)
                
            
    
    def recibir_video(self, conexion, tamaño_video,video_id): # Recibir video del cliente
        try:
            if not os.path.exists(f"Nodos/NODO-{self.identificador}/{video_id}"): # Crear directorio para el video con uuid
                os.makedirs(f"Nodos/NODO-{self.identificador}/{video_id}")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora actual

            # Mostrar mensaje de recepción de video
            print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [RECIBIENDO_ARCHIVO]{RESET} - {RESET}{YELLOW}[TAMAÑO: {tamaño_video} bytes{RESET}]")

            # Recibir video del cliente y almacenarlo en el directorio correspondientes
            with open(f"Nodos/NODO-{self.identificador}/{video_id}/SinProcesar.mp4", "wb") as video: 
                contador = 0 # Contador de bytes recibidos

                # Recibir mientras el contador sea menor o igual al tamaño del video
                while contador <= tamaño_video: 
                    datos = conexion.recv(1024) 
                    if not datos:
                        break

                    # Si se recibe el mensaje de finalización terminar la transferencia
                    if datos == b"[FIN]": 
                        break

                    video.write(datos) # Escribir datos en el archivo
                    contador += len(datos)  # Actualizar contador
            # Mostrar mensaje de confirmación
            print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [ARCHIVO_RECIBIDO]{RESET} - [TAMAÑO: {tamaño_video} bytes{RESET}]")
            return video.name
        except Exception as e:
            print(f"{RED}[ERROR AL RECIBIR EL VIDEO] = {YELLOW}{e}{RESET}")
            return False

if __name__ == "__main__":
    os.system("cls")
    
    Nodo = Nodo(env.BROKER_HOST, env.BROKER_PORT)
    
    # Crear directorios para almacenar los videos
    print(f"{BOLD}{YELLOW}CREANDO DIRECTORIOS...{RESET}")
    if not os.path.exists("Nodos"):
        os.makedirs(f"Nodos")
    
    if not os.path.exists(f"Nodos/NODO-{Nodo.identificador}"): # Crear directorio para el nodo
        os.makedirs(f"Nodos/NODO-{Nodo.identificador}")
    
    Nodo.conectar_broker()

    confirmacion = Nodo.conexion.recv(1024).decode()
    
    print(f"{GREEN}{confirmacion}{GRAY}")
    
    Nodo.manejador()
    
    input("Presiona Enter para cerrar la conexión...")
    Nodo.conexion.close()