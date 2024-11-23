import tkinter as tk
import subprocess
import sys


# Almacenamos los procesos para poder detenerlos
procesos = {}

# Función que ejecuta el script y abre la terminal
def ejecutar_script(script, nombre_script, abrir_terminal=True):
    if abrir_terminal:
        if sys.platform == "win32":
            # En Windows, abrimos una nueva ventana de cmd para ejecutar el script
            proceso = subprocess.Popen(['start', 'cmd', '/K', f'python {script}'], shell=True)
        elif sys.platform == "darwin" or sys.platform.startswith("linux"):
            # En macOS/Linux, usamos gnome-terminal o xterm
            proceso = subprocess.Popen(['gnome-terminal', '--', 'python', script])  # Para GNOME terminal
            # proceso = subprocess.Popen(['xterm', '-e', f'python {script}'])  # Si prefieres xterm
        else:
            print("Sistema no soportado para abrir la terminal")
            return
    else:
        # Ejecuta el script sin abrir una terminal (en segundo plano)
        proceso = subprocess.Popen([sys.executable, script], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Guardamos el proceso en el diccionario para poder detenerlo luego
    procesos[nombre_script] = proceso


# Crear la ventana principal
root = tk.Tk()
root.title('Ejecutar Scripts')
root.attributes("-topmost", True)

# Crear los botones para ejecutar los scripts
boton1 = tk.Button(root, text="Ejecutar Broker", command=lambda: ejecutar_script('Broker.py', 'script1'))
boton1.pack(pady=10)

boton2 = tk.Button(root, text="Ejecutar Cliente", command=lambda: ejecutar_script('Cliente.py', 'script2', abrir_terminal=False))
boton2.pack(pady=10)

boton3 = tk.Button(root, text="Ejecutar Nodo", command=lambda: ejecutar_script('Nodo.py', 'script3'))
boton3.pack(pady=10)

# Configurar el tamaño de la ventana
root.geometry('300x150')

# Ejecutar la interfaz gráfica
root.mainloop()
