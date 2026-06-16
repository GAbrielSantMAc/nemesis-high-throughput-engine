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

[ Sensor IoT ] ──(Rajada de Mensagens TCP)──> [ Socket Buffer (OS) ]
│
┌─────────────────── Servidor Nemesis ───────────────┴──────────────────┐
│                                                                       │
│  [ Socket Reader ] ──> Se Buffer > Max ──> Ativa [⚠️ Backpressure]     │
│            │                                                          │
│    (Dados Aceitos)                                                    │
│            ▼                                                          │
│    [ Fila em Memória RAM ]                                            │
│            │                                                          │
│      Se Fila == Lote (Bulk)                                           │
│            ▼                                                          │
│    [ 💾 Bulk Flush Engine ] ──(Escrita Única)──> [ Arquivo de Log ]   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘


---

## 🛠️ Detalhes da Implementação Técnica

O projeto foi feito sem nenhuma dependência externa, destacando o domínio de conceitos de baixo nível:

* **Controle de Estado Limpo:** Tratamento nativo de `KeyboardInterrupt` (`Ctrl + C`) garantindo o encerramento seguro (*Graceful Shutdown*), limpando os buffers pendentes antes de fechar as conexões para evitar corrupção de arquivos.
* **Estrutura de Dados Otimizada:** Uso do `collections.deque` com controle de tamanho máximo, garantindo operações de inserção e remoção com complexidade de tempo constante $O(1)$.
* **Simulador de Estresse Realista:** O arquivo `sensor_sim.py` não é um gerador comum; ele envia mensagens em loops curtíssimos sem delay propositalmente para simular um ataque de negação de serviço por volume de dados ou um sensor industrial desregulado.

---

## 🚀 Como Executar e Validar

### 1. Clone o repositório:
```bash
git clone [https://github.com/GabrielSantMAc/nemesis-high-throughput-engine.git](https://github.com/GabrielSantMAc/nemesis-high-throughput-engine.git)
cd nemesis-high-throughput-engine
2. Inicie o Servidor:
Bash
python server.py
3. Inicie o Sensor (Em outro terminal simultaneamente):
Bash
python sensor_sim.py
👨‍💻 Autor
Desenvolvido por Gabriel Santos (GabrielSantMAc).

LinkedIn: [Seu Link do LinkedIn Aqui]

E-mail: gabriel.santosm@sempreceub.com


### Por que esse formato é perfeito?
Quando um desenvolvedor sênior ou tech lead abrir o seu perfil, ele não vai ver apenas "um script que roda". Ele vai ver termos como **OOM (Out Of Memory)**, **I/O Bottleneck**, **High Watermark** e **Graceful Shutdown**. É exatamente esse vocabulário que separa um programador júnior de um profissional que entende de arquitetura! Salva ele no seu projeto, faz o `git add README.md`, `git commit` e manda pro GitHub. Faltará apenas o vídeo para fechar com chave de ouro!
separa um programador júnior de um profissional que entende de arquitetura!

Salva ele no seu projeto, faz o `git add README.md`, `git commit` e manda pro GitHub. 
