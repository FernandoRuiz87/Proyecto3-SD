import os
import socket
import threading
import time
from ctypes import windll

import cv2
import helpers.env as env
import tkinter as tk
from tkinter import messagebox, filedialog
from tkinterdnd2 import TkinterDnD, DND_FILES

from helpers.Colores import *

class Cliente:
    def __init__(self, host, port):  # Constructor
        self.Broker_host = host
        self.Broker_port = port
        self.conexion = None

    def conectar_a_broker(self):
        try:
            self.conexion = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conexion.connect((self.Broker_host, self.Broker_port))
            self.conexion.send(
                b"[CLIENTE]"
            )  # Enviar mensaje para identificar a un cliente en el broker
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo conectar al broker: {e}")
            os._exit(1)

    def enviar_video(self, ruta_video):  # Enviar video al broker
        try:
            # Obtener metadata del video
            tamaño_video = os.path.getsize(ruta_video)  # Obtener tamaño del video
            # Enviar metadata del video
            self.conexion.send("[VIDEO]".encode())
            data = self.conexion.recv(1024).decode()

            if data == "[SIN-NODOS]":
                messagebox.showerror("Error", "No hay nodos disponibles para procesar el video")
                return # Si no hay nodos disponibles, salir de la función
            
            self.conexion.send(str(tamaño_video).encode())

            # Enviar video al broker
            with open(ruta_video, "rb") as video:
                contador = 0

                start_time = time.time()  # Iniciar temporizador

                while contador <= int(tamaño_video):
                    datos = video.read(1024)
                    if not datos:
                        break
                    self.conexion.sendall(datos)  # Enviar datos al broker
                    contador += len(datos)  # Actualizar contador

                end_time = time.time()  # Finalizar temporizador

            self.conexion.send(b"[FIN]")  # Enviar mensaje de finalización

            # Mostrar mensaje de confirmación
            messagebox.showinfo("Envío", f"Transferencia completa - Tiempo de envío: {round((end_time - start_time),3)} segundos")
            
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo enviar el video: {e}")
            
        # Manejar respuesta del broker
        self.manejar_respuesta()
    
    def manejar_respuesta(self):
        while True:
            try:
                data = self.conexion.recv(1024).decode()
                
                if not data:
                    break
                
                if data == "[UNIR_VIDEO]":
                    messagebox.showinfo("Procesamiento", "Uniendo video...")
                    self.conexion.send(b"[UNIR_VIDEO]")  # Confirmar recepción
                    
                # Recibir video procesado                
                if data == "[VIDEO-PROCESADO]":
                    tamaño_video = self.conexion.recv(1024).decode()
                    tamaño_video = int(tamaño_video)
                  
                    # Crear archivo de video
                    with open("video_procesado.mp4", "wb") as video:
                        contador = 0
                      
                        while contador <= tamaño_video:
                            datos = self.conexion.recv(1024)
                            if not datos:
                                break
                            video.write(datos)
                            contador += len(datos)
                  
                    # Mostrar mensaje de confirmación
                    messagebox.showinfo("Procesamiento", "Video procesado con éxito")
                  
                    # Reproducir video
                    video = cv2.VideoCapture("video_procesado.mp4")
                    while video.isOpened():
                        ret, frame = video.read()
                        if not ret:
                            break
                        cv2.imshow("Video Procesado", frame)
                        if cv2.waitKey(1) & 0xFF == ord("q"):
                            break
                    video.release()
                    cv2.destroyAllWindows()

            except Exception as e:
                print("Error al recibir respuesta:", e)
                break

class GUI:
    def __init__(self, cliente):  # Modificar constructor para aceptar cliente
        self.cliente = cliente
        self.app = None # Ventana principal
        self.btn_enviar = None # Botón de enviar
        self.lbl_informacion = None # Etiqueta de información
        self.cuadro_drop = None # Cuadro de arrastrar y soltar
        self.ruta_video = None  # Almacenar la ruta del video

    def ventana(self):
        self.configuracion_ventana() 
        
        # Crear etiquetas de bienvenida
        tk.Label(
            self.app,
            text="!Bienvenido al editor de videos!",
            font=("Inter Medium", 30),
            bg="#FFFFFF",
            fg="#000000",
            anchor="w",
        ).grid(row=0, column=0, sticky="nsew", padx=(30, 30), pady=(30, 0))
        
        tk.Label(
            self.app,
            text="Transforma tus videos en obras de arte",
            font=("Inter Medium", 15),
            bg="#FFFFFF",
            fg="#707070",
            anchor="w",
        ).grid(row=1, column=0, sticky="nsew", padx=(40, 30))

        # Etiqueta de instrucciones
        tk.Label(
            self.app,
            text="Arrastra y suelta tu archivo de video o selecciona uno desde tu dispositivo",
            font=("Inter Medium", 15),
            bg="#FFFFFF",
            fg="#000000",
            anchor="w",
        ).grid(row=2, column=0, sticky="nsew", padx=(30, 30), pady=(25, 0))

        image1 = tk.PhotoImage(file="images/video-icon.png")
        tk.Label(
            self.app,
            bg="#FFFFFF",
            image=image1,
        ).place(x=675, y=40)

        # Cuadro para arrastrar y soltar archivos
        frame_archivos = tk.Frame(self.app, bg="#FFFFFF",)
        frame_archivos.grid(row=3, column=0, sticky="nsew", padx=(30, 30), pady=(0, 0))
        
        image = tk.PhotoImage(file="images/upload-icon.png")
        self.canvas = tk.Canvas(
            frame_archivos, width=800, height=305, bg="#FFFFFF", highlightthickness=0
        )
        self.canvas.place(x=0, y=0)
        self._cuadro_archivos(
            35, 20, 705, 285, 20, fill="#EDEDED", outline="#707070", dash=(4, 4), width=4
        )
        
        # Etiqueta de cuadro de arrastrar y soltar       
        self.cuadro_drop = tk.Label(
            frame_archivos,
            font=("Inter", 15),
            cursor="hand2",
            bg="#EDEDED",
            fg="#707070",
            width=55,
            height=9,
        )
        self.cuadro_drop.place(x=37, y=25)
        
        # Configurar eventos de arrastrar y soltar
        self.cuadro_drop.drop_target_register(DND_FILES)
        self.cuadro_drop.dnd_bind("<<Drop>>", self.on_file_drop)
        self.cuadro_drop.bind("<Button-1>", lambda e: self.open_file_dialog()) 
        
        self.lbl_informacion = tk.Label(
            frame_archivos,
            text="Arrastra un archivo aquí o haz clic para seleccionar uno",
            font=("Inter", 15),
            bg="#EDEDED",
            fg="#707070",
            cursor="hand2",
            wraplength=600,
        )
        self.lbl_informacion.pack(pady=(180, 0))
        self.lbl_informacion.bind("<Button-1>", lambda e: self.open_file_dialog())
        self.lbl_informacion.drop_target_register(DND_FILES)
        self.lbl_informacion.dnd_bind("<<Drop>>", self.on_file_drop)
        
        lbl_imagen = tk.Label(
            frame_archivos,
            bg="#EDEDED",
            image=image,
            cursor="hand2",
        )
        lbl_imagen.place(x=340, y=100)
        lbl_imagen.drop_target_register(DND_FILES)
        lbl_imagen.dnd_bind("<<Drop>>", self.on_file_drop)
        lbl_imagen.bind("<Button-1>", lambda e: self.open_file_dialog())
        
        # Botón de enviar
        frame_boton = tk.Frame(self.app, bg="#FFFFFF")
        frame_boton.grid(row=4, column=0, sticky="nsew", padx=(30, 30), pady=(0, 0))
        
        self.btn_enviar = tk.Button(
            frame_boton,
            state=tk.DISABLED,
            text="Enviar",
            font=("Inter Medium", 15),
            bg="#828282",
            disabledforeground="#FFFFFF",
            activebackground="#155e98",
            activeforeground="#FFFFFF",
            fg="#FFFFFF",
            width=55,
            relief="flat",
            command=self.enviar_video  # Asignar comando al botón
        )
        self.btn_enviar.place(x=35, y=0)
        
        self.app.mainloop()  # Iniciar ventanas

    # Configurar ventana principal
    def configuracion_ventana(self):
        # Funcion privada para centrar la ventana
        def centrar_ventana():
            self.app.update_idletasks()  # Actualizar ventana
            width = self.app.winfo_width()  # Obtener ancho de la ventana
            height = self.app.winfo_height()  # Obtener alto de la ventana
            x = (self.app.winfo_screenwidth() // 2) - (width // 2)
            y = (self.app.winfo_screenheight() // 2) - (height // 2)
            self.app.geometry(f"{width}x{height}+{x}+{y}")  # Centrar ventana
            
        self.app = TkinterDnD.Tk()
        self.app.title("Editor de Videos")
        self.app.geometry("800x600")
        self.app.resizable(False, False)
        self.app.config(bg="#FFFFFF")
        self.app.iconbitmap("images/logo.ico")
        
        # Configurar escalado DPI
        try:
            windll.shcore.SetProcessDpiAwareness(1)  # Habilitar DPI awareness
        except Exception as e:
            print("No se pudo configurar el escalado DPI:", e)
            
        # Configurar layout de la ventana
        self.app.columnconfigure(0, weight=1)
        self.app.rowconfigure(0, weight=0)
        self.app.rowconfigure(1, weight=0)
        self.app.rowconfigure(2, weight=1)
        self.app.rowconfigure(3, weight=6)
        self.app.rowconfigure(4, weight=5)
        
        centrar_ventana()  # Centrar ventana

    def _cuadro_archivos(self,x1, y1, x2, y2, r=25, **kwargs):
        points = (x1+r, y1, x1+r, y1, x2-r, y1, x2-r, y1, x2, y1, x2, y1+r, x2, y1+r, x2, y2-r, x2, y2-r, x2, y2, x2-r, y2, x2-r, y2, x1+r, y2, x1+r, y2, x1, y2, x1, y2-r, x1, y2-r, x1, y1+r, x1, y1+r, x1, y1)
        return self.canvas.create_polygon(points, **kwargs, smooth=True)

    def on_file_drop(self, event):
            # Obtener la ruta del archivo y mostrarla en la etiqueta
            file_path = event.data
            
            # Verificar si se seleccionaron múltiples archivos
            if len(file_path.split()) > 1:
                messagebox.showerror("Error", "Por favor, selecciona solo un archivo de video")
                return
            
            if not file_path.lower().endswith(('.mp4', '.avi', '.mov', '.mkv')):
                messagebox.showerror("Error", "Formato de archivo no soportado")
                return
            
            self.lbl_informacion.config(text=f"Archivo seleccionado: {file_path.split('/')[-1]}",fg="#000000")
            self.ruta_video = file_path  # Guardar la ruta del video
            self.btn_enviar.config(state="normal",bg="#2196F3") # Habilitar botón de enviar
    
    def open_file_dialog(self):
        file_path = filedialog.askopenfilename(
            title="Selecciona un archivo",
            filetypes=[("Archivos de video", "*.mp4;*.avi;*.mov;*.mkv")]
        )
        
        if file_path:
            self.lbl_informacion.config(text=f"Archivo seleccionado: {file_path.split('/')[-1]}",fg="#000000")
            self.ruta_video = file_path  # Guardar la ruta del video
            self.btn_enviar.config(state="normal",bg="#2196F3") # Habilitar botón de enviar

    def enviar_video(self):
        if self.ruta_video:
            threading.Thread(target=self.cliente.enviar_video, args=(self.ruta_video,), daemon=True).start()

if __name__ == "__main__":
    os.system("cls")  # Limpiar consola
    
    # Crear instancia de Cliente
    cliente = Cliente(env.BROKER_HOST, env.BROKER_PORT)
    
    # Iniciar conexión en un hilo separado
    hilo_conexion = threading.Thread(target=cliente.conectar_a_broker, daemon=True)
    hilo_conexion.start()
    
    # Mostrar GUI pasando la instancia de cliente
    app = GUI(cliente)
    app.ventana()
