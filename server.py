import asyncio
import struct
import sys
import os

class RingBuffer:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.buffer = [None] * capacity  
        self.head = 0  
        self.tail = 0  
        self.size = 0
        self.lock = asyncio.Lock()
        self.not_empty = asyncio.Event()
        self.not_full = asyncio.Event()
        self.not_full.set()

    async def put(self, item):
        async with self.lock:
            while self.size == self.capacity:
                self.not_full.clear()
                print("[⚠️ BACKPRESSURE] Buffer cheio! Segurando a ingestão de rede...")
                await asyncio.sleep(0.05)
                
            self.buffer[self.head] = item
            self.head = (self.head + 1) % self.capacity
            self.size += 1
            self.not_empty.set()

    async def get(self):
        while self.size == 0:
            self.not_empty.clear()
            await self.not_empty.wait()
            
        async with self.lock:
            item = self.buffer[self.tail]
            self.buffer[self.tail] = None  
            self.tail = (self.tail + 1) % self.capacity
            self.size -= 1
            
            if self.size < self.capacity:
                self.not_full.set()
            return item

class NemesisHighThroughputServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.packet_size = 25 
        self.ring_buffer = RingBuffer(capacity=15) 
        self.file_path = "telemetria_nemesis.txt"
        
        # Limpa o arquivo antigo se existir para começar o teste zerado
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    async def bootstrap(self):
        server = await asyncio.start_server(self.processar_stream, self.host, self.port)
        print(f"[BOOTSTRAP] Nemesis Kernel Online. Escutando Sockets em {self.host}:{self.port}")
        
        # Ativa o Worker de Analytics e Persistência
        asyncio.create_task(self.worker_analytics())
        
        async with server:
            await server.serve_forever()

    async def processar_stream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        try:
            while True:
                try:
                    data_buffer = await reader.readexactly(self.packet_size)
                except asyncio.IncompleteReadError:
                    break

                header_magico, sensor_id, timestamp, temperatura, payload_extra = struct.unpack("=HIId7s", data_buffer)

                if header_magico != 0xDEAD:
                    print(f"[ALERTA SEGURANÇA] Cabeçalho Inválido de {addr}. Derrubando...")
                    break

                pacote = (sensor_id, temperatura, timestamp)
                await self.ring_buffer.put(pacote)

        except Exception as e:
            print(f"[FALHA] Erro crítico na stream de {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def worker_analytics(self):
        """Camada 3 & 4: Consumidor, Detector de Anomalias e Bulk Flush Engine."""
        print("[ENGINE] Worker de Analytics e Flush ativado...")
        batch_storage = []
        batch_limit = 10  # Descarrega no disco a cada 10 registros acumulados

        while True:
            sensor_id, temperatura, timestamp = await self.ring_buffer.get()
            
            # Simula processamento rápido de regras analíticas
            await asyncio.sleep(0.02) 

            status = "NOMINAL"
            if temperatura > 100.0:
                status = "CRÍTICO - SUPERAQUECIMENTO"
                print(f"[🚨 ANOMALIA] Sensor: {sensor_id} | Temp: {temperatura}°C | STATUS: {status}")
            else:
                print(f"[METRIC CORE] Ingestão Ok -> Sensor: {sensor_id} | Temp: {temperatura}°C")

            # Acumula no lote de memória
            batch_storage.append(f"{timestamp},{sensor_id},{temperatura},{status}\n")

            # CAMADA 4: BULK FLUSH ASSÍNCRONO
            if len(batch_storage) >= batch_limit:
                print(f"[💾 BULK FLUSH] Gravando lote de {len(batch_storage)} registros no disco...")
                
                # Executa a escrita de I/O de forma não bloqueante para não travar o loop de eventos
                await self.salvar_em_lote_assincrono(batch_storage.copy())
                batch_storage.clear()

    async def salvar_em_lote_assincrono(self, linhas):
        """Usa o pool de threads do asyncio para rodar a escrita em disco sem congelar a rede."""
        def escrita_bloqueante():
            with open(self.file_path, "a") as f:
                f.writelines(linhas)
                
        # Transforma o I/O síncrono do arquivo em uma tarefa assíncrona
        await asyncio.to_thread(escrita_bloqueante)
        print("[💾 BULK FLUSH] Gravação concluída com sucesso!")

if __name__ == "__main__":
    # Mudamos para 9990 para fugir do bloqueio atual
    engine = NemesisHighThroughputServer(host="127.0.0.1", port=9990) 
    try:
        asyncio.run(engine.bootstrap())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Servidor desligado de forma segura.")
        sys.exit(0)