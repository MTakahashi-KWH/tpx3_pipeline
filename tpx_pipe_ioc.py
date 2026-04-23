import numpy as np
import socket
import dask.dataframe as dd

from p4p.nt import NTNDArray, NTScalar, NTURI
from p4p.server import Server
from p4p.server.thread import SharedPV
import tpx3awkward as tpx

# from queue import Queue
# import threading
from multiprocessing import Queue, shared_memory, Process, Value

HOST = "tcp://localhost"
SERVAL = "8088"
BUFF_SIZE = 200 * 1024 * 1024
NUM_BUFFERS = 8
NUM_THREADS = 4
SID = Value(-1)
SCAN = Value(0)
ACTIVE = Value(False)

free_q = Queue()
full_q = Queue()
out_q = Queue()

# Preallocate buffers
buffers = []
for i in range(NUM_BUFFERS):
    shm = shared_memory.SharedMemory(create=True, size=BUFF_SIZE)
    buffers.append(shm)
    free_q.put(i)  # pass index, not data

def stream(sock):
    flag_end = False
    index = 0
    while True:
        buf_i = free_q.get()  # take ownership
        buf = buffers[buf_i]
        view = memoryview(buf)

        read_total = 0
        while read_total < BUFF_SIZE*.8:
            n = sock.recv_into(view[read_total:])
            if n == 0:
                if flag_end:
                    break
                flag_end = True
                print("0 packet received... suspecting eot... awaiting next update")
            read_total += n

        # hand off buffer (no copy)
        full_q.put((buf_i, read_total , index))
        index +=1
        if flag_end:
            return


def worker():
    while True:
        try:
            buf_i, size, index = full_q.get()  # take ownership
            buf = buffers[buf_i]
        except Queue.Empty:
            continue
        else:
            # zero-copy cast
            arr = np.frombuffer(memoryview(buf)[:size], dtype=np.int32)

            # do processing
            print(f"Processing {arr.size} ints")
            res = None
            out_q.put((index,res))
            # return buffer to pool
            free_q.put(buf_i)

            full_q.task_done()


def trigger():
    # Setup
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST,SERVAL))
        # Start reader (main thread or separate)
        stream(sock)
        full_q.join()
        # ddf = assemble(out_q)
        # callback(ddf)

    
def start_ioc():
    sid = SharedPV(nt=NTScalar('d'), initial = SID)
    path = SharedPV(nt=NTURI())
    active = SharedPV(nt=NTScalar('?'), initial = False)
    broadcast = SharedPV(NTNDArray)
    


if __name__ == "main":

    # for _ in range(NUM_BUFFERS):
    #     free_q.put(bytearray(BUFF_SIZE))
    # Start workers
    for i in range(NUM_THREADS):  # adjust based on CPU
        Process(target=worker, daemon=True,name=f"tpx_file_worker_{i}").start()

    
    start_ioc()
