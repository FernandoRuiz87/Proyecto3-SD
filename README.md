# Proyecto3-SD

## Descripción

Este proyecto implementa un sistema distribuido para la transferencia y procesamiento de videos. El sistema consta de tres componentes principales: Cliente, Broker y Nodo. El Cliente envía videos al Broker, el Broker distribuye los videos entre los Nodos, y los Nodos reciben y almacenan los videos.

## Funcionamiento

1. **Cliente**: Se conecta al Broker y envía un video.
2. **Broker**: Recibe el video del Cliente, lo divide en segmentos y distribuye estos segmentos entre los Nodos conectados.
3. **Nodo**: Recibe los segmentos de video del Broker y los almacena en su sistema de archivos.

## Dependencias

- Python 3.x
- OpenCV (`cv2`)
- `socket`
- `threading`
- `uuid`
- `os`
- `time`
- `datetime`
- `windows-curses`
- `PyQt5`
- `tkinterdnd2`

## Instalación de Dependencias

Puedes instalar las dependencias necesarias utilizando `pip`:
