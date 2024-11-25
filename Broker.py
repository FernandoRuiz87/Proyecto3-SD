from queue import Queue, Empty
from datetime import datetime
from helpers.Colores import *
import helpers.env as env
import threading
import socket
import uuid
import time
import cv2 
import os
import errno
import glob
class Broker:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.server = None
        self.nodos = []  # Lista de nodos conectados
        self.cola_fragmentos_listos = Queue()  # Cola para enviar fragmentos de video a los nodos
        self.cliente = None # Cliente conectado
        self.evento_mensaje_enviado = threading.Event()
        self.total_hilos = 0  # Número total de hilos que ejecutarán la tarea
        self.hilos_activos = 0  # Contador de hilos activos
        self.lock = threading.Lock()  # Lock para proteger el contador
        self.video_procesado = Queue()  # Cola para recibir los videos procesados
                
    def iniciar_servidor(self):
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.bind((self.host, self.port))
            self.server.listen()
            print(f"{REVERSE}Servidor iniciado en ({self.host}:{self.port}){RESET}\n")
            print(f"{CYAN}{BOLD}LOG DEL SERVIDOR{RESET}\n---------------------------------------{RESET}")
        except Exception as e:
            print(f"{RED}[ERROR AL INICIAR EL SERVIDOR] = {YELLOW}{e}{RESET}")
    
    def aceptar_conexiones(self):
        while True:
            try:
                conexion, address = self.server.accept()
                # Organizar hilos para manejar las conexiones
                tipo_conexion = conexion.recv(1024).decode()

                if tipo_conexion == "[CLIENTE]": # Si es un cliente inicia hilo de cliente
                    threading.Thread(target=self.manejador_cliente, args=(conexion, address), daemon=True).start() # Hilo de clientes

                if tipo_conexion == "[NODO]": # Si es un nodo inicia hilo de nodo
                    self.total_hilos += 1 
                    threading.Thread(target=self.manejador_nodo, args=(conexion, address),daemon=True).start() # Hilo de nodos
                    
            except Exception as e:
                print(f"Error aceptando conexiones: {e}")
                break
    
    def hora_evento(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Proceso de recepcion de video del cliente
    def manejador_cliente(self, conexion, address):
        self.cliente = conexion
        print(f"{BOLD}{GREEN}[{self.hora_evento()}] [CONEXIÓN_CLIENTE]{RESET} - {YELLOW}[DIRECCION : {address}]{RESET}")
        
        while True:
            data = self.recibir_datos(conexion)
            
            if data is None:
                self.manejar_desconexion("CLIENTE", address, conexion)
                break
            
            if data == "[VIDEO]":
                self.manejador_video(conexion, address)
            
            if data == "[UNIR_VIDEO]":
                result_path = self.video_procesado.get()
                video_id = result_path.split("/")[1].split("/")[0]
                final_path = self.unir_fragmentos(video_id)
                self.devolver_video(conexion, final_path)
            
    def manejador_nodo(self, conexion, address):
        self.nodos.append(conexion) # Agregar nodo a la lista de nodos
        print(f"{BOLD}{BLUE}[{self.hora_evento()}] [CONEXIÓN_NODO]{RESET} - {YELLOW}[DIRECCION : {address}]{RESET}")
        print(f"{GRAY}[{self.hora_evento()}] CANTIDAD DE NODOS CONECTADOS = {len(self.nodos)}{RESET}")
        
        while True:
            data = self.recibir_datos(conexion)
            
            if data is None:
                self.manejar_desconexion("NODO", address, conexion)
                break
            
            if data == "[LISTO_PARA_RECIBIR]":
                with self.lock:
                    self.hilos_activos += 1 # Añaadir un hilo activo
                fragmento_path = self.cola_fragmentos_listos.get() # Obtener fragmento de la cola
                video_id = fragmento_path.split("/")[1]
                n_segmento = fragmento_path.split("_")[2].split(".")[0]
                self.enviar_video_nodo(fragmento_path, conexion, n_segmento,video_id)
                print(f"{LIGHT_PURPLE}[{self.hora_evento()}] [PROCESANDO-VIDEO]{RESET} - [VIDEO_ID: {video_id}]")
                
            if data == "[VIDEO_PROCESADO]":
                self.recibir_video_procesado(conexion,video_id,n_segmento)
                
                with self.lock: # Proteger el contador
                    self.hilos_activos -= 1 # Disminuir el contador de hilos activos
                    if self.hilos_activos == 0: # Si acabaron todos los hilos
                        self.cliente.send(b"[UNIR_VIDEO]")
                                
    def enviar_video_nodo(self,fragmento_path,conexion ,n_segmento,video_id):
        try:
            # Obtener metadata del video
            tamaño_video = os.path.getsize(fragmento_path)  # Obtener tamaño del video
            metadata = f"{video_id},{tamaño_video},{n_segmento}"
            conexion.send(metadata.encode())
            
            # Enviar video al nodo
            with open(fragmento_path, "rb") as video:
                contador = 0
                while contador <= tamaño_video:
                    datos = video.read(1024)
                    if not datos:
                        break
                    conexion.sendall(datos)  # Enviar datos al broker
                    contador += len(datos)
            
        except Exception as e:
            print(f"Error al enviar el video: {e}")
        pass
        
    def recibir_datos(self, conexion):
        try:
            data = conexion.recv(1024)
            data = data.decode()
            return data if data else None
        except Exception as e:
            return None
        except UnicodeDecodeError:
            print("Error al decodificar los datos recibidos")
            return None

    def manejar_desconexion(self, tipo_conexion, address,conexion):
        hora_evento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{BOLD}{RED}[{hora_evento}] [DESCONEXIÓN_{tipo_conexion}]{RESET} - {YELLOW}[DIRECCION : {address}]{RESET}")
        if tipo_conexion == "CLIENTE":
            conexion.close()
        elif tipo_conexion == "NODO":
            self.nodos = [nodo for nodo in self.nodos if nodo != conexion]
            self.conexion_nodo = None
            print(f"{GRAY}[{hora_evento}] CANTIDAD DE NODOS CONECTADOS = {len(self.nodos)}{RESET}")

    def manejador_video(self, conexion_cliente, address):
        if not self.nodos:
            # Respondemos al cliente que no hay nodos disponibles
            conexion_cliente.send("[SIN-NODOS]".encode())
            hora_evento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{RED}{BOLD}[{hora_evento}] [ERROR] [NO HAY NODOS DISPONIBLES]{RESET}")
            return

        try:
            # Confirmar al cliente que puede enviar el video
            conexion_cliente.send("[OK]".encode())
            video_id = str(uuid.uuid4()) # Identificador único para el video
            tamaño_video = conexion_cliente.recv(1024).decode() # Recibir tamaño del video
        except Exception as e:
            print(f"{RED}[ERROR AL RECIBIR EL VIDEO] = {YELLOW}{e}{RESET}")
        
        video_path = self.recibir_video(conexion_cliente, int(tamaño_video), video_id,address)
        cantidad_nodos = len(self.nodos)

        if video_path and cantidad_nodos > 0:
            self.dividir_video(video_path, cantidad_nodos, video_id)
            for nodo in self.nodos:
                nodo.send(b"[VIDEO]")
            return

    def handle_exception(self, e, tipo_conexion, conexion, address):
        if hasattr(e, 'errno') and e.errno == errno.WSAECONNRESET:
            hora_desconexion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if tipo_conexion == "NODO":
                self.nodos.remove(conexion)
                print(f"{BOLD}{RED}[{hora_desconexion}] [DESCONEXION_{tipo_conexion}]{RESET} - {YELLOW}[DIRECCION : {address}]{RESET}")
                print(f"{GRAY}[{hora_desconexion}] CANTIDAD DE NODOS CONECTADOS = {len(self.nodos)}{RESET}")
            elif tipo_conexion == "CLIENTE":
                self.cliente = None
                print(f"{BOLD}{RED}[{hora_desconexion}] [DESCONEXION_{tipo_conexion}]{RESET} - {YELLOW}[DIRECCION : {address}]{RESET}")
        else:
            print(f"{BOLD}{RED}[ERROR]{RESET} - {e}")
        conexion.close()
                
    def recibir_video(self, conexion, tamaño_video, identificador,address):  # Recibir video del cliente
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora actual

            # Preparar directorios para almacenar todos los videos
            base_dir = "Broker_files"
            video_dir = f"{base_dir}/{identificador}/SinProcesar"
            processed_dir = f"{base_dir}/{identificador}/Procesado"

            os.makedirs(video_dir, exist_ok=True)
            os.makedirs(processed_dir, exist_ok=True)

            # Recibir video del cliente y almacenarlo en el directorio correspondiente
            original_video_path = f"{video_dir}/Original.mp4"
            contador = 0  # Contador de bytes recibidos

            with open(original_video_path, "wb") as video:
                while contador <= tamaño_video:
                    datos = conexion.recv(1024)
                    if not datos or datos == b"[FIN]":
                        break
                    video.write(datos)
                    contador += len(datos)

            # Mostrar mensaje de confirmación
            print(f"{BOLD}{LIGHT_PURPLE}[{timestamp}] [VIDEO-RECIBIDO]{RESET} - {CYAN}[FUENTE : {address}] - {YELLOW}[TAMAÑO: {tamaño_video} bytes]{RESET}")
            return original_video_path

        except Exception as e:
            print(f"{RED}[ERROR AL RECIBIR EL VIDEO] = {YELLOW}{e}{RESET}")
            return False
    
    def dividir_video(self, video, cantidad_nodos, identificador):
        output_dir = f"Broker_files/{identificador}/SinProcesar"  # Directorio de salida
        os.makedirs(output_dir, exist_ok=True)  # Crear el directorio de salida si no existe

        cap = cv2.VideoCapture(video)  # Cargar video con OpenCV
        if not cap.isOpened():
            print("Error al abrir el video.")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)  # Obtener FPS del video
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))  # Ancho del frame
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))  # Alto del frame
        cantidad_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))  # Cantidad de frames
        frames_por_segmento = cantidad_frames // cantidad_nodos  # Frames por segmento
        cuatrocc = cv2.VideoWriter_fourcc(*'mp4v')  # Códec para mp4

        # Dividir el video en segmentos y asegurarse de manejar los frames restantes
        for i in range(cantidad_nodos):
            nombre_segmento = f"{output_dir}/segmento_{i + 1}.mp4"
            out = cv2.VideoWriter(nombre_segmento, cuatrocc, fps, (frame_width, frame_height))

            # Calcular los frames que debe incluir este segmento
            inicio_frame = i * frames_por_segmento
            fin_frame = inicio_frame + frames_por_segmento

            # Si es el último nodo, incluir los frames restantes
            if i == cantidad_nodos - 1:
                fin_frame = cantidad_frames

            # Escribir los frames en el archivo del segmento
            cap.set(cv2.CAP_PROP_POS_FRAMES, inicio_frame)
            for _ in range(inicio_frame, fin_frame):
                ret, frame = cap.read()
                if not ret:
                    break
                out.write(frame)
            out.release()
            self.cola_fragmentos_listos.put(nombre_segmento)
        
        self.cola_fragmentos_listos.put(None)  # Señal de que terminó de dividir el video
        cap.release()
        hora_evento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{BOLD}{LIGHT_PURPLE}[{hora_evento}] [VIDEO-DIVIDIDO]{RESET} - {YELLOW}[CANTIDAD-SEGMENTOS: {cantidad_nodos}]{RESET}")

    def recibir_video_procesado(self,conexion,video_id,segmento):
        # Armar el path del video procesado
        video_path = f"Broker_files/{video_id}/Procesado/{segmento}.mp4"
        
        # Recibir metadata del video
        tamaño_video = conexion.recv(1024).decode()
        tamaño_video = int(tamaño_video)
        
        # Recibir video del nodo y almacenarlo en el directorio correspondiente
        with open(video_path, "wb") as video_file:
            contador = 0  # Contador de bytes recibidos

            while contador < tamaño_video:
                
                datos = conexion.recv(1024)
                
                if datos == b"[FIN]" or not datos:
                    break

                video_file.write(datos)
                contador += len(datos)
        self.video_procesado.put(video_path)
        print(f"{BOLD}{LIGHT_PURPLE}[{self.hora_evento()}] [VIDEO-PROCESADO]{RESET} - {YELLOW}[VIDEO_ID: {video_id}] - {YELLOW}[SEGMENTO: {segmento}]{RESET}")

    def unir_fragmentos(self, video_id):
        output_dir = f"Broker_files/{video_id}/Procesado"
        fragmentos = glob.glob(f"Broker_files/{video_id}/Procesado/*.mp4") # Obtener fragmentos
        video_salida = f"Broker_files/{video_id}/Procesado/Video_Final.mp4"

        if not fragmentos:
            print(f"{RED}[ERROR] No se encontraron fragmentos para unir.{RESET}")
            return

        # Obtener propiedades del primer fragmento
        cap = cv2.VideoCapture(fragmentos[0])
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()

        # Crear el archivo de video final
        out = cv2.VideoWriter(video_salida, cv2.VideoWriter_fourcc(*'mp4v'), fps, (frame_width, frame_height))

        for fragmento in fragmentos:
            cap = cv2.VideoCapture(fragmento)
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                out.write(frame)
            cap.release()

        out.release()
        return video_salida
        print(f"{BOLD}{LIGHT_PURPLE}[{self.hora_evento()}] [VIDEO-UNIDO]{RESET} - {YELLOW}[VIDEO_ID: {video_id}]{RESET}")
    
    def devolver_video(self, conexion, video_path):
        try:
            print(video_path)
            conexion.send("[VIDEO-PROCESADO]".encode())  # Enviar mensaje de video procesado
            tamaño_video = os.path.getsize(video_path)  # Obtener tamaño del video
            conexion.send(str(tamaño_video).encode())  # Enviar tamaño del video
            
            with open(video_path, "rb") as video:
                contador = 0
                while contador <= tamaño_video:
                    datos = video.read(1024)
                    if not datos or datos == b"[FIN]":
                        break
                    conexion.sendall(datos)
                    contador += len(datos)
            
            conexion.send(b"[FIN]")  # Enviar mensaje de finalización            
        except Exception as e:
            print(f"Error al enviar el video: {e}")    
    
if __name__ == "__main__":
    os.system("cls") # Limpiar consola
    
    broker = Broker(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Broker
    broker.iniciar_servidor() # Iniciar servidor
    
    if broker.server: # Si el servidor se inicio correctamente aceptar conexiones
        broker.aceptar_conexiones()
    