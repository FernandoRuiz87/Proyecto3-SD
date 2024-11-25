from datetime import datetime
from helpers.Colores import *
import helpers.env as env
import cv2 
import socket
import os
import uuid
import numpy as np

class Nodo:
    def __init__(self, host, port): # Constructor
        self.host = host
        self.port = port
        self.conexion = None
        self.identificador = uuid.uuid4()
        self.modificacion = None
        self.procesador = Procesador_video()

    def conectar_broker(self): # Conectar a broker
        try:
            self.conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conexion.connect((self.host, self.port))
            self.conexion.send("[NODO]".encode()) # Enviar mensaje para identificar a un nodo en el broker 
            return True
        except Exception as e:
            print(f"{RED}[ERROR AL CONECTAR CON EL BROKER]: {RESET}{e}")
            return False
    
    def manejador(self):
        print(f"{BOLD}{YELLOW}[INFORMACION DEL NODO]{RESET}")
        print(f"{BOLD}{PINK}ID_NODO: {self.identificador}{RESET}")
        print(f"{BOLD}{PINK}MODIFICACION: {self.modificacion}{RESET}\n")
        print(f"{BOLD}{LIGHT_BLUE}➡ [ESPERANDO PETICIONES...]{RESET}")
        
        while True:
            data = self.recibir_datos(self.conexion)
            
            if data == "[VIDEO]":
                self.conexion.send("[LISTO_PARA_RECIBIR]".encode())
                # Recibir metadata del video
                metadata = self.recibir_datos(self.conexion)
                
                video_id = metadata.split(",")[0]
                tamaño_video = metadata.split(",")[1]
                n_segmento = metadata.split(",")[2]
                
                print(f"{BOLD}{LIGHT_PURPLE}VIDEO_ID: {video_id}{RESET}")
                print(f"{BOLD}{LIGHT_PURPLE}TAMAÑO: {tamaño_video} bytes{RESET}")
                print(f"{BOLD}{LIGHT_PURPLE}N_SEGMENTO: {n_segmento}{RESET}")
                                
                video_path = self.recibir_video(self.conexion, int(tamaño_video), video_id)
                out_path = self.procesador.procesar_video(video_path,video_id,self.modificacion, self.conexion)
                
                if out_path:
                    self.conexion.send("[VIDEO_PROCESADO]".encode())
                    self.enviar_video(out_path)
    
    def recibir_datos(self, conexion):
        try:
            data = conexion.recv(1024)
            data = data.decode()
            return data if data else None
        except UnicodeDecodeError:
            print("Error al decodificar los datos recibidos")
            return None
             
    def recibir_video(self, conexion, tamaño_video, video_id):  # Recibir video del cliente
        try:
            video_dir = os.path.join("Nodos", f"NODO-{self.identificador}", video_id)
            os.makedirs(video_dir, exist_ok=True)  # Crear directorio para el video con uuid

            hora_evento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora actual
            video_path = os.path.join(video_dir, "SinProcesar.mp4")

            # Recibir video del cliente y almacenarlo en el directorio correspondiente
            with open(video_path, "wb") as video_file:
                contador = 0  # Contador de bytes recibidos

                while contador < tamaño_video:
                    
                    datos = conexion.recv(1024)
                    
                    if datos == b"[FIN]" or not datos:
                        print("recibio mensaje [FIN]")
                        break

                    video_file.write(datos)
                    contador += len(datos)

            # Mostrar mensaje de confirmación
            print(f"{BOLD}{LIGHT_PURPLE}[{hora_evento}] [ARCHIVO_RECIBIDO]{RESET} - {YELLOW}[TAMAÑO: {tamaño_video} bytes]{RESET}")
            return video_file.name
        except Exception as e:
            print(f"{RED}[ERROR AL RECIBIR EL VIDEO] = {YELLOW}{e}{RESET}")
            return False

    def enviar_video(self, ruta_video):  # Enviar video al broker
        try:
            # Obtener metadata del video
            tamaño_video = os.path.getsize(ruta_video)  # Obtener tamaño del video
            print(tamaño_video)
            self.conexion.send(str(tamaño_video).encode())

            # Enviar video al broker
            with open(ruta_video, "rb") as video:
                contador = 0

                while contador <= int(tamaño_video):
                    datos = video.read(1024)
                    if not datos:
                        break
                    self.conexion.sendall(datos)  # Enviar datos al broker
                    contador += len(datos)  # Actualizar contador

            self.conexion.send(b"[FIN]")  # Enviar mensaje de finalización

        except Exception as e:
           print("Error", f"No se pudo enviar el video: {e}")
class Procesador_video:
    
    def procesar_video(self, ruta_video, video_id, efecto, conexion):
        """
        Procesa un video aplicando un efecto seleccionado y guarda el resultado.
        """
        
        # Seleccionar el efecto a aplicar
        match efecto:
            case "escala_de_grises":
                efecto = self.escala_de_grises
            case "invertir_colores":
                efecto = self.invertir_colores
            case "efecto_sepia":
                efecto = self.efecto_sepia
            case "efecto_espejo":
                efecto = self.efecto_espejo
            case "grises_y_bordes":
                efecto = self.grises_y_bordes
        
        print("Procesando video con efecto:", efecto.__name__)
        
        cap = cv2.VideoCapture(ruta_video)
        
        if not cap.isOpened():
            print("No se pudo abrir el archivo de video.")
            return
        
        # Obtener propiedades del video
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        ancho = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        alto = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        codec = cv2.VideoWriter_fourcc(*'mp4v')
        out_path = f"Nodos/NODO-{Nodo.identificador}/{video_id}/{efecto.__name__}.mp4"
        salida = cv2.VideoWriter(out_path, codec, fps, (ancho, alto))
        
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Aplicar el efecto al frame
            frame_procesado = efecto(frame)
            
            # Guardar el frame procesado
            salida.write(frame_procesado)
            
            # Salir con la tecla 'q'
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        
        # Liberar recursos
        cap.release()
        salida.release()
        cv2.destroyAllWindows()
        print("Procesamiento completado.")
        return out_path
    
    def escala_de_grises(self,frame):
        """Convierte el frame a escala de grises"""
        gris = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        return cv2.cvtColor(gris, cv2.COLOR_GRAY2BGR)
    
    def invertir_colores(self,frame):
        """Invierte los colores del frame"""
        return cv2.bitwise_not(frame)
    
    def efecto_sepia(self,frame):
        """Aplica un efecto sepia al frame."""
        kernel = np.array([
        [0.272, 0.534, 0.131],
        [0.349, 0.686, 0.168],
        [0.393, 0.769, 0.189]
        ])
        sepia = cv2.transform(frame, kernel)
        return cv2.convertScaleAbs(sepia)
    
    def efecto_espejo(self,frame):
        """Refleja el frame horizontalmente."""
        return cv2.flip(frame, 1)
    
    def grises_y_bordes(self,frame):
        """Combina escala de grises y detección de bordes."""
        gris = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        bordes = cv2.Canny(gris, 50, 150)
        return cv2.cvtColor(bordes, cv2.COLOR_GRAY2BGR)
        

if __name__ == "__main__":
    os.system("cls")
    Nodo = Nodo(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de la clase Nodo
    
    # Modificaciones disponibles
    while True:
        print(f"{BOLD}{LIGHT_CYAN}[ CONFIGURACIÓN DEL NODO ]{RESET}\n")
        print(f"{UNDERLINE}{WHITE}Métodos de modificación disponibles:{RESET}")
        print(f"  1.Escala de grises")
        print(f"  2.Invertir colores")
        print(f"  3.Efecto sepia")
        print(f"  4.Efecto espejo")
        print(f"  5.Grises y bordes\n")
        opcion = input(f"{BOLD}Seleccione una opción: {RESET}")
        
        match opcion:
            case "1": 
                Nodo.modificacion = "escala_de_grises"
                break
            case "2":
                Nodo.modificacion = "invertir_colores" 
                break
            case "3":
                Nodo.modificacion = "efecto_sepia"
                break
            case "4":
                Nodo.modificacion = "efecto_espejo"
                break
            case "5":
                Nodo.modificacion = "grises_y_bordes"
                break
            case _:
                print(f"{RED}Opcion invalida{RESET}")
                os.system("cls")
    
    os.system("cls")
    
    print(f"{BOLD}{BLUE}[ESTABLECIENDO CONEXIÓN CON EL BROKER, POR FAVOR ESPERE...]{RESET}")
    flag = Nodo.conectar_broker()
    
    if not flag: # Si no se puede conectar con el broker terminar el programa
        exit()
    
    print(f"{BOLD}{LIGHT_GREEN}[ CONEXIÓN REALIZADA CON ÉXITO ]{RESET}\n")
    
    # Crear directorios para almacenar los videos
    print(f"{BOLD}{YELLOW}➡ Creando directorios para almacenar datos del nodo...{RESET}")
    if not os.path.exists("Nodos"):
        os.makedirs(f"Nodos")
    
    if not os.path.exists(f"Nodos/NODO-{Nodo.identificador}"): # Crear directorio para el nodo
        os.makedirs(f"Nodos/NODO-{Nodo.identificador}")
    
    # Confirmación al finalizar la creación de directorios
    print(f"{BOLD}{LIGHT_GREEN}[ TODOS LOS DIRECTORIOS HAN SIDO CREADOS EXITOSAMENTE ]\n{RESET}")
    
    Nodo.manejador()
    
    Nodo.conexion.close()