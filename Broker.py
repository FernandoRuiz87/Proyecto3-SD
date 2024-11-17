from datetime import datetime
from helpers.Colores import *
import helpers.env as env
import cv2 
import threading
import socket
import os
import uuid
class Broker:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.server = None
        self.nodos = []  # Lista de nodos conectados
        self.cliente = None  # Lista de clientes conectados
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
                
                # Si el mensaje de identificacion es de un nodo
                if data == "[NODO]": 
                    self.nodos.append(conexion) # Agregar nodo a la lista de nodos
                    print(f"{BLUE}[CONEXIÓN_NODO] {YELLOW}{address}{RESET} ~~~ {BLUE}[HORA_CONEXIÓN] {YELLOW}{hora_conexion}{RESET}")
                    conexion.send("[CONECTADO CON EXITO]".encode()) # Enviar mensaje de confirmación
                    tipo_conexion = "NODO"
                
                # Si el mensaje de identificacion es de un cliente
                if data == "[CLIENTE]": 
                    tipo_conexion = "CLIENTE"
                    self.cliente = conexion
                    print(f"{BOLD}{GREEN}[{hora_conexion}] {GREEN}[CONEXIÓN_CLIENTE]{RESET} - {CYAN}[DIRECCION : {address}]{RESET}")
                    conexion.send("[CONECTADO CON EXITO]".encode())
                
                # Si el mensaje de identificacion es de un video# Si el mensaje de identificacion es de un video
                if data == "[VIDEO]":
                    video_id = str(uuid.uuid4()) # Generar identificador único para el video
                    tamaño_video = conexion.recv(1024).decode()
                    video_path =  self.recibir_video(conexion, int(tamaño_video),video_id)
                    # cantidad_nodos = len(self.nodos)
                    cantidad_nodos = 3 # Cantidad de nodos a los que se enviará el video fuerza bruta
                    
                    if video_path and cantidad_nodos > 0:
                        print("xsadsa")
                        self.dividir_video(video_path,cantidad_nodos,video_id)
                        
                        # # Enviar video a un nodo
                        # for nodo in self.nodos:
                            # nodo.send(f"[VIDEO]{video_id}".encode())
                            # print(f"{BOLD}{LIGHT_PURPLE}[{hora_conexion}] [ENVIANDO_VIDEO]{RESET} - {CYAN}[NODO : {nodo.getpeername()}]{RESET}")
    
            except Exception as e:
                hora_error = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"{BOLD}{RED}[{hora_error}] [DESCONEXION_{tipo_conexion}]{RESET} - {CYAN}[DIRECCION : {address}]{RESET}\n{e}")
                print(e)
                conexion.close()
                break
                
    def recibir_video(self, conexion, tamaño_video,identificador): # Recibir video del cliente
        try:
            #Preparar directorios para almacenar todos los videos
            if not os.path.exists("Broker_files"):
                os.makedirs("Broker_files")

            if not os.path.exists(f"Broker_files/{identificador}"): # Crear directorio para el video con uuid
                os.makedirs(f"Broker_files/{identificador}/SinProcesar")
                os.makedirs(f"Broker_files/{identificador}/Procesado")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora actual

            # Mostrar mensaje de recepción de video
            print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [RECIBIENDO_ARCHIVO]{RESET} - {CYAN}[FUENTE : {self.address}] - {RESET}{YELLOW}[TAMAÑO: {tamaño_video} bytes{RESET}]")

            # Recibir video del cliente y almacenarlo en el directorio correspondientes
            with open(f"Broker_files/{identificador}/SinProcesar/Original.mp4", "wb") as video: 
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
            print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [ARCHIVO_RECIBIDO]{RESET} - {CYAN}[FUENTE : {self.address}] - {RESET}{YELLOW}[TAMAÑO: {tamaño_video} bytes{RESET}]")
            return video.name
        except Exception as e:
            print(f"{RED}[ERROR AL RECIBIR EL VIDEO] = {YELLOW}{e}{RESET}")
            return False
    
    def dividir_video(self, video, cantidad_nodos, identificador):
        print("")
        output_dir = f"Broker_files/{identificador}/SinProcesar"  # Directorio de salida
        os.makedirs(output_dir, exist_ok=True)  # Crear el directorio de salida si no existe
        cap = cv2.VideoCapture(video)  # Cargar video con OpenCV

        # Verificar si el video se abrió correctamente
        if not cap.isOpened():
            print("Error al abrir el video.")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)  # Obtener FPS del video
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))  # Ancho del frame
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))  # Alto del frame

        # Obtener cantidad de frames del video
        cantidad_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Calcular cantidad de frames por segmento
        duracion_segmento = cantidad_frames // cantidad_nodos

        # Crear los VideoWriter para cada segmento
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # Códec para mp4

        for i in range(cantidad_nodos):
            # Nombre del archivo de salida para cada segmento
            nombre_segmento = f"{output_dir}/segmento_{i + 1}.mp4"
            out = cv2.VideoWriter(nombre_segmento, fourcc, fps, (frame_width, frame_height))

            # Escribir frames en cada segmento
            for j in range(duracion_segmento):
                ret, frame = cap.read()
                if not ret:
                    break
                out.write(frame)

            # Liberar el VideoWriter para este segmento
            out.release()
            print(f"Segmento {i + 1} guardado como {nombre_segmento}")

        # Si hay más frames después de dividirlos, guardarlos en un segmento adicional
        if cantidad_frames % cantidad_nodos != 0:
            nombre_segmento = f"{output_dir}/segmento_{cantidad_nodos + 1}.mp4"
            out = cv2.VideoWriter(nombre_segmento, fourcc, fps, (frame_width, frame_height))

            # Escribir los frames restantes en el último segmento
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                out.write(frame)

            # Liberar el VideoWriter del último segmento
            out.release()
            print(f"Segmento final guardado como {nombre_segmento}")

        # Liberar recursos
        cap.release()
        print("División del video completada.")
        
if __name__ == "__main__":
    os.system("cls") # Limpiar consola
    
    broker = Broker(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Broker
    broker.iniciar_servidor() # Iniciar servidor

    if broker.server: # Si el servidor se inicio correctamente aceptar conexiones
        broker.aceptar_conexiones()
