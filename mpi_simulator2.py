import typing
import time
import csv
import unittest
from multiprocessing import Process, Queue

number_of_processes_to_simulate = 4

MPI_ANY_SOURCE = -1

# Function to be executed by worker
def square_function(x):
    return x**2

# Unit test for square_function
class TestSquareFunction(unittest.TestCase):
    def test_square_function(self):
        self.assertEqual(square_function(2), 4)
        self.assertEqual(square_function(5), 25)

# Worker logic
def worker_logic(rank, recv_f, send_f):
    while True:
        data = recv_f(MPI_ANY_SOURCE)
        if data == 'exit':
            break
        else:
            result = square_function(data)
            send_f((rank, result), 0)

# Coordinator logic
def coordinator_logic(size, send_f, recv_f):
    inputs = list(range(10))
    with open('results.csv', 'w', newline='') as csvfile:
        result_writer = csv.writer(csvfile)
        for i in inputs:
            send_f(i, (i % (size - 1)) + 1)
        for i in inputs:
            result = recv_f(MPI_ANY_SOURCE)
            result_writer.writerow(result)
    for i in range(1, size):
        send_f('exit', i)

# MPI Application
def mpi_application(rank, size, send_f, recv_f):
    if rank == 0:
        coordinator_logic(size, send_f, recv_f)
    else:
        worker_logic(rank, recv_f, send_f)

# Simulator Code
def _run_app(process_rank, size, app_f, send_queues):
    send_f = _generate_send_f(process_rank, send_queues)
    recv_f = _generate_recv_f(process_rank, send_queues)
    app_f(process_rank, size, send_f, recv_f)

def _generate_recv_f(process_rank, send_queues):
    def recv_f(from_source:int):
        while send_queues[process_rank].empty():
            time.sleep(1)
        return send_queues[process_rank].get()[1]
    return recv_f

def _generate_send_f(process_rank, send_queues):
    def send_f(data, dest):
        send_queues[dest].put((process_rank,data))
    return send_f

def _simulate_mpi(n:int, app_f):
    send_queues = {}
    for process_rank in range(n):
        send_queues[process_rank] = Queue()
    ps = []
    for process_rank in range(n):
        p = Process(
            target=_run_app,
            args=(
                process_rank,
                n,
                app_f,
                send_queues
            )
        )
        p.start()
        ps.append(p)
    for p in ps:
        p.join()

if __name__ == "__main__":
    # Running unit tests
    unittest.main(argv=['first-arg-is-ignored'], exit=False)

    # Running MPI simulation
    _simulate_mpi(number_of_processes_to_simulate, mpi_application)
