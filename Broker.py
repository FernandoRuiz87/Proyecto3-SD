import sys
from helpers.Colores import *
import helpers.env as env
from os import system
from datetime import datetime
import time
import threading
import socket
from tqdm import tqdm
import uuid
class Broker:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.server = None
        self.nodos = []  # Lista de nodos conectados
        self.cliente = None  # Lista de clientes conectados
        self.identificador = None  # Identificador del video
        self.address = None  # Dirección del cliente
        
    def iniciar_servidor(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.bind((self.host, self.port))
            self.address = self.server.getsockname()
            self.server.listen()
            print(f"{REVERSE}Servidor iniciado en ({self.host}:{self.port}){RESET}\n")
            print(f"{CYAN}{BOLD}LOG DEL SERVIDOR{RESET}\n---------------------------------------{RESET}")
        except Exception as e:
            print(f"{RED}[ERROR AL INICIAR EL SERVIDOR] = {YELLOW}{e}{RESET}")
    
    def aceptar_conexiones(self):
        while True:
            conexion, address = self.server.accept()
            threading.Thread(target=self.manejador, args=(conexion, address)).start()
    
    def manejador(self, conexion, address):
        tipo_conexion = None
        while True:
            now = datetime.now()
            hora_conexion = now.strftime("%Y-%m-%d %H:%M:%S")
            try:
                data = conexion.recv(1024)
                try:
                    data = data.decode() # Recibir datos del cliente
                except UnicodeDecodeError:
                    print("Error al decodificar los datos recibidos")
                    continue
                
                if not data: # Si no hay datos cerrar la conexión
                    conexion.close()
                    break
                
                if data == "[NODO]": # Si el mensaje de identificacion es de un nodo
                    tipo_conexion = "NODO"
                    self.nodos.append(conexion)
                    print(f"{BLUE}[CONEXIÓN_NODO] {YELLOW}{address}{RESET} ~~~ {BLUE}[HORA_CONEXIÓN] {YELLOW}{hora_conexion}{RESET}")
                    conexion.send("[CONECTADO CON EXITO]".encode()) # Enviar mensaje de confirmación
                    
                if data == "[CLIENTE]": # Si el mensaje de identificacion es de un cliente
                    tipo_conexion = "CLIENTE"
                    self.cliente = conexion
                    print(f"{BOLD}{GREEN}[{hora_conexion}] {GREEN}[CONEXIÓN_CLIENTE]{RESET} - {CYAN}[DIRECCION : {address}]{RESET}")
                    conexion.send("[CONECTADO CON EXITO]".encode())
                
                if data == "[VIDEO]": # Si el mensaje de identificacion es de un video
                    tamaño_video = conexion.recv(1024).decode()
                    
                    self.recibir_video(conexion, int(tamaño_video)) 
    
            except Exception as e:
                hora_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"{BOLD}{RED}[{hora_error}] [DESCONEXION_{tipo_conexion}]{RESET} - {CYAN}[DIRECCION : {address}]{RESET}")
                conexion.close() 
                break
                
    def recibir_video(self, conexion, tamaño_video): # Recibir video del cliente
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora
        print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [RECIBIENDO_ARCHIVO]{RESET} - {CYAN}[FUENTE : {self.address}] - {RESET}{YELLOW}[TAMAÑO: {tamaño_video} bytes{RESET}]")
        self.identificador = str(uuid.uuid4()) # Generar identificador único para el video
        with open(f"Broker_files/Sin_procesar/{self.identificador}.mp4", "wb") as video: 
            contador = 0
            
            while contador <= tamaño_video:
                datos = conexion.recv(1024)
                if not datos:
                    break
                    
                if datos == b"[FIN]":
                    break
                    
                video.write(datos)
                contador += len(datos)        
        
            
if __name__ == "__main__":
    system("cls") # Limpiar consola
    
    broker = Broker(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Broker
    broker.iniciar_servidor() # Iniciar servidor

    if broker.server: # Si el servidor se inicio correctamente aceptar conexiones
        broker.aceptar_conexiones()
