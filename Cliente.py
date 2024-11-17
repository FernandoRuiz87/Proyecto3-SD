from helpers.Colores import *
import helpers.env as env
import socket
import time
import cv2
import os

class Cliente:
    def __init__(self, host, port): # Constructor
        self.Broker_host = host
        self.Broker_port = port
        self.conexion = None

    def conectar_a_broker(self): 
        try:
            self.conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conexion.connect((self.Broker_host, self.Broker_port))
            self.conexion.send(b"[CLIENTE]") # Enviar mensaje para identificar a un cliente en el broker
        except Exception as e:
            print(f"Error al conectar con el broker: {e}")
    
    def enviar_video(self, ruta_video): # Enviar video al broker
        try:
            # Obtener metadata del video
            tamaño_video = os.path.getsize(ruta_video) # Obtener tamaño del video
            
            # Enviar metadata del video
            self.conexion.send(b"[VIDEO]")
            self.conexion.send(str(tamaño_video).encode())

            # Enviar video al broker
            with open(ruta_video, "rb") as video:
                contador = 0

                start_time = time.time() # Iniciar temporizador

                while contador <= int(tamaño_video):
                    datos = video.read(1024)
                    if not datos:
                        break
                    self.conexion.sendall(datos) # Enviar datos al broker
                    contador += len(datos) # Actualizar contador
                
                end_time = time.time() # Finalizar temporizador
            
            self.conexion.send(b"[FIN]") # Enviar mensaje de finalización
            
            # Mostrar mensaje de confirmación
            print(f"Transferencia completa - Tiempo de envío: {round((end_time - start_time),3)} segundos")
        except Exception as e:
            print(f"Error al enviar el video: {e}")

if __name__ == "__main__":
    os.system("cls") # Limpiar consola
  
    Cliente = Cliente(env.BROKER_HOST, env.BROKER_PORT) # Crear instancia de Cliente
    Cliente.conectar_a_broker() # Conectar a broker
    
    input("Presiona Enter para enviar video...") # Esperar a que el usuario presione Enter

    Cliente.enviar_video("video.mp4") # Enviar video
    
    input("Presiona Enter para cerrar la conexión...") # Esperar a que el usuario presione Enter
