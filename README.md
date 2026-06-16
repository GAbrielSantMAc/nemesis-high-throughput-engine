# ⚡ Nemesis High-Throughput Ingestion Engine

O **Nemesis Engine** é um motor de ingestão de dados de telemetria de alta performance e baixa latência construído em Python puro. O projeto foi arquitetado para resolver um dos problemas mais clássicos e complexos da engenharia de software distribuída: **o processamento estável de rajadas massivas de dados (High-Throughput Ingestion) sem comprometer a saúde da infraestrutura.**

---

## 🛑 O Cenário de Estresse e O Problema

Imagine um ecossistema com milhares de sensores IoT (como carros autônomos, turbinas de avião ou dispositivos médicos) enviando logs de telemetria a cada milissegundo através de conexões TCP estáveis. 

Neste cenário, ocorrem dois grandes gargalos:
1. **Assincronia de Velocidade:** O produtor (sensor) consegue gerar e enviar dados muito mais rápido do que o consumidor (servidor) consegue processá-los e interpretá-los.
2. **O Gargalo de I/O (Disco):** Salvar cada registro individualmente no disco rígido assim que ele chega destrói a performance do servidor, pois operações de entrada/saída (I/O) em disco são infinitamente mais lentas do que operações em memória RAM.

Se o servidor simplesmente aceitar todos os dados sem controle, o buffer de memória RAM vai inflar indefinidamente até disparar um erro de **Out Of Memory (OOM)**, derrubando o servidor e causando perda total dos dados em trânsito.

---

## 🏗️ A Solução: Arquitetura e Engenharia de Fluxo

Para mitigar esses problemas sem adicionar frameworks pesados (como Kafka ou RabbitMQ), o **Nemesis Engine** implementa dois padrões arquiteturais nativos a nível de aplicação:

### 1. Mecanismo Dinâmico de Backpressure (Controle de Vazão)
Em vez de deixar a memória RAM estourar, o servidor monitora ativamente o tamanho do seu buffer interno (`collections.deque`). 

* **Gatilho de Alta (High Watermark):** Quando o volume de mensagens acumuladas atinge o limite máximo de segurança, o servidor aciona o estado de `[⚠️ BACKPRESSURE]`.
* **Ação:** O motor do servidor bloqueia temporariamente a leitura do socket daquele cliente específico. Como o protocolo TCP possui sua própria janela de controle de fluxo (Window Size), o sistema operacional do cliente (sensor) detecta o canal obstruído e freia a velocidade de envio na origem.
* **Gatilho de Baixa (Low Watermark):** Assim que o servidor processa os pacotes represados e o buffer volta a um nível seguro, o estado de Backpressure é desativado e o fluxo volta à velocidade máxima.



### 2. Bulk Flush / Persistência Amortecida em Lote
Para contornar o gargalo de escrita em disco, o servidor utiliza uma estratégia de bufferização baseada em blocos:

* Os dados limpos e validados são mantidos em uma fila rápida na memória RAM.
* Quando essa fila atinge o tamanho exato do lote configurado (ex: blocos de 10 ou 20 mensagens), o servidor engatilha o `[💾 BULK FLUSH]`.
* Uma única chamada de sistema abre o arquivo `telemetria_nemesis.txt`, despeja todo o bloco de uma vez e fecha o descritor de arquivo. Isso reduz o número de operações de I/O em até **95%**, liberando a CPU para focar na rede.

---

## 🎨 O Fluxo de Dados (Data Pipeline)
