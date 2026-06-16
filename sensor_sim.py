import socket
import struct
import time
import random

def rodar_sensor_estresse_binario():
    HOST = '127.0.0.1'
    PORT = 9990  # Sincroniza com a nova porta do servidor
    SENSOR_ID = 1024

    print(f"[SENSOR {SENSOR_ID}] Inicializando engine de telemetria binária...")

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        print("[SENSOR] Handshake TCP efetuado. Iniciando streaming contínuo...")

        # Dispara 50 pacotes em altíssima velocidade
        for i in range(1, 51):
            temperatura = random.choice([25.4, 40.2, 115.8, 36.6, 142.1]) 
            timestamp_atual = int(time.time())
            
            header_magico = 0xDEAD
            payload_extra = b"NEMESIS"
            
            packet = struct.pack("=HIId7s", header_magico, SENSOR_ID, timestamp_atual, temperatura, payload_extra)
            
            client_socket.sendall(packet)
            print(f"[STREAM] Pacote {i}/50 Enviado. Temp: {temperatura}°C")
            
            # Delay mínimo de 0.01 segundos para entupir o buffer do servidor
            time.sleep(0.01)

    except ConnectionRefusedError:
        print("[ERRO] Servidor offline. Verifique se o server.py está rodando na porta correta.")
    except Exception as e:
        print(f"[ERRO NO SENSOR]: {e}")
    finally:
        client_socket.close()
        print("[SENSOR] Stream encerrada.")

if __name__ == "__main__":
    rodar_sensor_estresse_binario()