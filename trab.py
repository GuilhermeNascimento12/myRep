# -----------------------------------------------------------------------------
# Trabalho Prático de Sistemas Operacionais - Simulador de Escalonador de Tarefas
#
# Alunos: [Guilherme da Silva Nascimento]
#         [João Gabriel Caetano da Silva]
# RA's:   [130162]
#         [13065]
#
# Disciplina: 12035 - Sistemas Operacionais
# Professor: Dr. Alisson Renan Svaigen
# -----------------------------------------------------------------------------

import socket
import time
import sys
import multiprocessing
import threading
import math
from collections import deque
import matplotlib.pyplot as plt

# --- Configurações Globais ---
# Define o endereço e as portas para a comunicação via socket entre os processos,
# conforme especificado no enunciado do trabalho prático.
HOST = '127.0.0.1'
CLOCK_PORT = 4000
EMISSOR_PORT = 4001
ESCALONADOR_PORT = 4002

class Task:
    """
    Representa uma tarefa (processo) e armazena todos os seus atributos e
    métricas de estado, como tempo de chegada, duração, prioridade e
    tempos de execução para cálculo das métricas de saída.
    """
    def __init__(self, id, arrival_time, duration, priority):
        self.id = id
        self.arrival_time = int(arrival_time)
        self.duration = int(duration)
        self.priority = int(priority)
        self.original_priority = int(priority) # Mantém a prioridade original para referência.
        self.remaining_time = int(duration)   # Tempo de execução restante.
        
        # Métricas para o arquivo de saída.
        self.start_time = -1
        self.finish_time = -1
        self.turnaround_time = 0
        self.wait_time = 0
        
        # Atributos específicos para certos algoritmos.
        self.quantum_slice = 0  # Para controlar a fatia de tempo no Round-Robin.
        self.last_priority_update = 0 # Para o envelhecimento no PRIOD.

def send_message(port, message):
    """Função utilitária para encapsular o envio de mensagens via socket."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((HOST, port))
            s.sendall(message.encode('utf-8'))
        except ConnectionRefusedError:
            # Se o processo de destino já foi encerrado, a conexão será recusada.
            # Isso é esperado no final da simulação, então o erro é ignorado.
            pass

def clock_listener(stop_event):
    """
    Thread dedicada a escutar por uma mensagem de finalização ("FINISH")
    enviada pelo Escalonador, permitindo o encerramento gracioso do Clock.
    O uso de uma thread separada evita que o loop principal do Clock bloqueie.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, CLOCK_PORT))
        s.listen()
        s.settimeout(0.2) # Evita que s.accept() bloqueie indefinidamente.
        while not stop_event.is_set():
            try:
                conn, _ = s.accept()
                with conn:
                    data = conn.recv(1024).decode('utf-8')
                    if data == "FINISH":
                        stop_event.set()
                        break
            except socket.timeout:
                continue

def clock_process(start_event):
    """
    Processo que simula o Clock da CPU. A cada ciclo, envia uma mensagem
    "TICK" para o Emissor e para o Escalonador, sincronizando a simulação.
    """
    start_event.wait() # Aguarda o Escalonador estar pronto antes de iniciar.
    time.sleep(0.1)    # Pequena pausa para garantir que os listeners dos outros processos estejam ativos.
    
    stop_event = threading.Event()
    listener = threading.Thread(target=clock_listener, args=(stop_event,))
    listener.start()

    current_time = 0
    while not stop_event.is_set():
        # Envia tick ao Emissor primeiro.
        send_message(EMISSOR_PORT, f"TICK:{current_time}")
        # Conforme o enunciado, aguarda 5ms antes de notificar o Escalonador.
        time.sleep(0.005)
        # Envia tick ao Escalonador.
        send_message(ESCALONADOR_PORT, f"TICK:{current_time}")
        
        # Aguarda 100ms para simular a passagem de uma unidade de tempo.
        time.sleep(0.1)
        current_time += 1
    
    listener.join() # Garante que a thread do listener termine antes do processo.

def emissor_process(input_file):
    """
    Processo que lê as tarefas de um arquivo de entrada e as envia para o
    Escalonador no seu respectivo tempo de chegada.
    """
    tasks_to_emit = []
    with open(input_file, 'r') as f:
        for line in f:
            parts = line.strip().split(';')
            tasks_to_emit.append(tuple(parts))
    
    # Ordena as tarefas pelo tempo de chegada para facilitar a emissão.
    tasks_to_emit.sort(key=lambda x: int(x[1]))
    
    emission_complete_sent = False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, EMISSOR_PORT))
        s.listen()
        while True:
            conn, _ = s.accept()
            with conn:
                data = conn.recv(1024).decode('utf-8')
                if data.startswith("TICK:"):
                    clock = int(data.split(':')[1])
                    
                    # Verifica se alguma tarefa deve ser emitida no clock atual.
                    for task_data in list(tasks_to_emit):
                        if int(task_data[1]) == clock:
                            msg = f"TASK:{';'.join(task_data)}"
                            send_message(ESCALONADOR_PORT, msg)
                            tasks_to_emit.remove(task_data)
                    
                    # Envia mensagem de conclusão ao Escalonador (apenas uma vez).
                    if not tasks_to_emit and not emission_complete_sent:
                         send_message(ESCALONADOR_PORT, "EMISSION_COMPLETE")
                         emission_complete_sent = True

                elif data == "FINISH":
                    break
        return

def escalonador_process(algorithm, input_file, start_event):
    """
    Processo central que simula o escalonador de tarefas da CPU.
    Implementa todos os algoritmos, gerencia a fila de prontos,
    controla a execução e gera o arquivo de saída.
    """
    
    # --- Estruturas de Dados e Variáveis de Controle ---
    all_tasks = {}
    ready_queue = deque()
    execution_timeline = []
    current_task = None
    current_clock = -1
    total_tasks = 0
    finished_tasks_count = 0
    all_tasks_emitted = False
    
    with open(input_file, 'r') as f:
        total_tasks = len(f.readlines())

    # --- Constantes dos Algoritmos ---
    QUANTUM = 3
    AGING_FACTOR = 5
    output_filename = f"{input_file.split('.')[0]}_{algorithm}_output.txt"
    gantt_filename = f"{input_file.split('.')[0]}_{algorithm}_gantt.png" ### NOVO ###

    # Função para gerar o Diagrama de Gantt 
    def generate_gantt_chart(timeline, filename):
        """
        Gera e salva um Diagrama de Gantt a partir da linha do tempo de execução.
        """
        fig, gnt = plt.subplots()

        # Configurações do gráfico
        gnt.set_xlabel('Tempo de Clock')
        gnt.set_ylabel('Tarefas')
        gnt.set_title(f'Diagrama de Gantt - Algoritmo {algorithm.upper()}')
        
        # Obtém a lista de tarefas únicas para o eixo Y
        task_ids = sorted(list(set(t for t in timeline if t != "idle")), key=lambda x: int(x[1:]))
        gnt.set_yticks([15 + i*10 for i in range(len(task_ids))])
        gnt.set_yticklabels(task_ids)

        gnt.grid(True, linestyle=':', alpha=0.6)

        # Define um mapa de cores para distinguir as tarefas
        cmap = plt.get_cmap('viridis')
        colors = {task_id: cmap(i/len(task_ids)) for i, task_id in enumerate(task_ids)}
        colors["idle"] = 'lightgrey'

        # Processa a linha do tempo para criar as barras do gráfico
        start_time = 0
        current_running_task = timeline[0]
        for i in range(1, len(timeline)):
            if timeline[i] != current_running_task:
                if current_running_task != "idle":
                    task_index = task_ids.index(current_running_task)
                    gnt.broken_barh([(start_time, i - start_time)], (10 + task_index*10, 9), 
                                    facecolors=(colors[current_running_task]))
                start_time = i
                current_running_task = timeline[i]
        
        # Adiciona a última barra
        if current_running_task != "idle":
            task_index = task_ids.index(current_running_task)
            gnt.broken_barh([(start_time, len(timeline) - start_time)], (10 + task_index*10, 9), 
                            facecolors=(colors[current_running_task]))

        # Ajusta os limites do eixo X
        gnt.set_xlim(0, len(timeline))
        
        # Salva o gráfico em um arquivo PNG
        plt.savefig(filename)
        plt.close()
        print(f"[Escalonador] Diagrama de Gantt salvo em: {filename}")

    def select_next_task():
        """
        Seleciona a próxima tarefa da fila de prontos com base no algoritmo
        e em um critério de desempate padrão (FCFS).
        """
        if not ready_queue:
            return None

        if algorithm == 'sjf' or algorithm == 'srtf':
            ready_queue_sorted = sorted(list(ready_queue), key=lambda t: (t.remaining_time, t.arrival_time))
        elif algorithm in ['prioc', 'priop', 'priod']:
            ready_queue_sorted = sorted(list(ready_queue), key=lambda t: (t.priority, t.arrival_time))
        else: # FCFS e RR simplesmente pegam o primeiro da fila.
            return ready_queue.popleft()
        
        selected = ready_queue_sorted[0]
        ready_queue.remove(selected)
        return selected

    def write_output_file():
        """
        Calcula as métricas finais e escreve o arquivo de saída de texto.
        Ao final, chama a função para gerar o gráfico de Gantt.
        """
        timeline_str = ";".join(execution_timeline)
        task_lines = []
        total_turnaround = 0
        total_wait_time = 0
        sorted_tasks = sorted(all_tasks.values(), key=lambda t: int(t.id[1:]))

        for task in sorted_tasks:
            task.turnaround_time = task.finish_time - task.arrival_time
            task.wait_time = task.turnaround_time - task.duration
            total_turnaround += task.turnaround_time
            total_wait_time += task.wait_time
            task_lines.append(f"{task.id};{task.arrival_time};{task.finish_time};{task.turnaround_time};{task.wait_time}")
        
        avg_turnaround = math.ceil((total_turnaround / total_tasks) * 10) / 10
        avg_wait_time = math.ceil((total_wait_time / total_tasks) * 10) / 10
        avg_line = f"{avg_turnaround:.1f};{avg_wait_time:.1f}"

        with open(output_filename, 'w') as f:
            f.write(timeline_str + "\n")
            f.write("\n".join(task_lines) + "\n")
            f.write(avg_line + "\n")
        
        print(f"[Escalonador] Simulação concluída. Arquivo de saída gerado: {output_filename}")
        
        # Chamada para a função que gera o gráfico.
        generate_gantt_chart(execution_timeline, gantt_filename)

    # --- Loop Principal do Escalonador ---
    start_event.set()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, ESCALONADOR_PORT))
        s.listen()
        
        while not (all_tasks_emitted and finished_tasks_count == total_tasks):
            conn, _ = s.accept()
            with conn:
                message = conn.recv(1024).decode('utf-8')
                
                if message.startswith("TASK:"):
                    _, task_data = message.split(":", 1)
                    tid, arrival, dur, prio = task_data.split(';')
                    new_task = Task(tid, arrival, dur, prio)
                    all_tasks[tid] = new_task
                    
                    if current_task and algorithm in ['srtf', 'priop', 'priod']:
                        if algorithm == 'srtf' and new_task.remaining_time < current_task.remaining_time:
                            ready_queue.append(current_task)
                            current_task = new_task
                        elif algorithm in ['priop', 'priod'] and new_task.priority < current_task.priority:
                            ready_queue.append(current_task)
                            current_task = new_task
                        else:
                            ready_queue.append(new_task)
                    else:
                        ready_queue.append(new_task)

                elif message == "EMISSION_COMPLETE":
                    all_tasks_emitted = True
                
                elif message.startswith("TICK:"):
                    current_clock = int(message.split(':')[1])

                    if algorithm == 'priod':
                        priorities_changed = False
                        for task in ready_queue:
                            if (current_clock - task.arrival_time - task.last_priority_update) >= AGING_FACTOR:
                                if task.priority > 0:
                                    task.priority -= 1
                                    task.last_priority_update += AGING_FACTOR
                                    priorities_changed = True

                        if priorities_changed and current_task and ready_queue:
                            highest_prio_in_queue = min(ready_queue, key=lambda t: (t.priority, t.arrival_time))
                            if highest_prio_in_queue.priority < current_task.priority:
                                ready_queue.remove(highest_prio_in_queue)
                                ready_queue.append(current_task)
                                current_task = highest_prio_in_queue
                                current_task.quantum_slice = 0

                    if not current_task:
                        current_task = select_next_task()
                        if current_task:
                             current_task.start_time = current_clock
                    
                    if current_task:
                        execution_timeline.append(current_task.id)
                        current_task.remaining_time -= 1
                        current_task.quantum_slice += 1

                        if current_task.remaining_time == 0:
                            current_task.finish_time = current_clock + 1
                            finished_tasks_count += 1
                            current_task = None 
                        
                        elif algorithm == 'rr' and current_task.quantum_slice >= QUANTUM:
                            current_task.quantum_slice = 0
                            ready_queue.append(current_task)
                            current_task = None
                    else:
                        execution_timeline.append("idle")
    
    # --- Finalização da Simulação ---
    write_output_file()
    send_message(CLOCK_PORT, "FINISH")
    send_message(EMISSOR_PORT, "FINISH")


def main():
    """
    Função principal que valida os argumentos de entrada, inicializa
    e gerencia a execução dos três processos principais da simulação.
    """
    if len(sys.argv) != 3:
        print("Uso: python trab.py <caminho_do_arquivo_de_entrada> <algoritmo>")
        print("Algoritmos: fcfs, rr, sjf, srtf, prioc, priop, priod")
        sys.exit(1)

    input_file = sys.argv[1]
    algorithm = sys.argv[2].lower()

    valid_algorithms = ['fcfs', 'rr', 'sjf', 'srtf', 'prioc', 'priop', 'priod']
    if algorithm not in valid_algorithms:
        print(f"Erro: Algoritmo '{algorithm}' não reconhecido.")
        sys.exit(1)

    start_event = multiprocessing.Event()
    
    p_clock = multiprocessing.Process(target=clock_process, args=(start_event,))
    p_emissor = multiprocessing.Process(target=emissor_process, args=(input_file,))
    p_escalonador = multiprocessing.Process(target=escalonador_process, args=(algorithm, input_file, start_event))
    
    print(f"Iniciando simulação com algoritmo: {algorithm.upper()}")
    
    p_escalonador.start()
    p_emissor.start()
    p_clock.start()

    p_clock.join()
    p_emissor.join()
    p_escalonador.join()

    print("Processos finalizados.")

# Ponto de entrada do script.
if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
