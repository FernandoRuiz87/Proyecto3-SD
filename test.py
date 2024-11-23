import threading
import queue

# Crear una cola
data_queue = queue.Queue()

# Función para el primer hilo
def producer():
    for i in range(5):
        print(f"Produciendo {i}")
        data_queue.put(i)  # Poner datos en la cola
    data_queue.put(None)  # Señal de que terminó

# Función para el segundo hilo
def consumer():
    while True:
        data = data_queue.get()
        if data is None:  # Si es la señal de terminar, salimos
            break
        print(f"Consumiendo {data}")

# Crear y empezar los hilos
t1 = threading.Thread(target=producer)
t2 = threading.Thread(target=consumer)

t1.start()
t2.start()

t1.join()
t2.join()
print("Trabajo terminado.")
