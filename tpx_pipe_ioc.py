import numpy as np
import socket
import dask.dataframe as dd
import pandas as pd

from p4p.nt import NTNDArray, NTScalar, NTURI
from p4p.server import Server
from p4p.server.thread import SharedPV
import tpx3awkward.processing as tpx
# from tpx3awkward.processing import Tpx3Config  #TODO
from ctypes import c_int, c_bool
# from queue import Queue
import threading
from multiprocessing import JoinableQueue, shared_memory, Process, Value
from queue import Empty
import time
from pathlib import Path
Queue = JoinableQueue

HOST = "localhost"
SERVAL = 8088
BUFF_SIZE = 10 * 1024 * 1024
NUM_BUFFERS = 16
NUM_THREADS = 6

SID = Value(c_int,-1)
SCAN = Value(c_int,0)
ACTIVE = Value(c_bool,False)
data_dir = Path.cwd() / 'data'
data_dir.mkdir(exist_ok=True)
OUTPUT_DIR = data_dir
# TPX_CONFIG = Tpx3Config.from_defaults() #TODO

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
        buf = buffers[buf_i].buf
        # view = memoryview(buf)

        read_total = 0
        while (read_total < BUFF_SIZE*.8) or (read_total%8 != 0):
            n = sock.recv_into(buf[read_total:])
            if n == 0:
                if flag_end:
                    break
                flag_end = True
                print("0 packet received... suspecting eot... awaiting next update")
            read_total += n
        print("writing out sequence ", index, " to buffer ",buf_i)
        # hand off buffer (no copy)
        full_q.put((buf_i, read_total - (read_total%8) , index))
        index +=1
        if flag_end:
            return


def worker(buffers,queues,params):
    free_q, full_q, out_q = queues
    while True:
        try:
            buf_i, size, index = full_q.get()  # take ownership
            print("WORKER: claimed sequence item ", index, " of size ", size, " with tail(?) length ", size%8)
            buf = buffers[buf_i].buf
            # zero-copy cast
            arr = np.frombuffer(buf[:size], dtype="<u8")

            # do processing
            print(f"WORKER: Processing {arr.size} ints")
            res = tpx.decode_tpx3_binary(arr)#pd.DataFrame(tpx.ingest_raw_data(arr)).sort_values("t").reset_index(drop=True)

            clustered_df = tpx.cluster_raw_df(
                res,
                .3,
                3,
            )
            clustered_df.to_parquet(OUTPUT_DIR/ f"buff_{index}.parquet")
            out_q.put((index,clustered_df))
            # return buffer to pool
            free_q.put(buf_i)
            print("WORKER: finished")
            full_q.task_done()
        except Empty:
            continue


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
    Server.forever(providers=[{
    'tpx:pipe:sid':sid, # PV name only appears here
    'tpx:pipe:path':path,
    'tpx:pipe:active':active,
    'tpx:pipe:broadcast':broadcast,
    }])
    
def test_boot():
    trigger_e = threading.Event()
    print("PIPELINE: starting up")
    for i in range(NUM_THREADS):  # adjust based on CPU
        Process(target=worker, daemon=True,args=(buffers,(free_q,full_q,out_q),()),name=f"tpx_file_worker_{i}").start()
    print("PIPELINE: daemon processes deployed")
    def socket_listener():
        while True:
            trigger_e.wait()
            print("PIPELINE: Triggered: connecting...")
            for i in range(10):
                print("PIPELINE: connection attempt: ",i)
                try:
                    trigger()
                    # print("... sucessful")
                    break
                except OSError:
                    print("... failed")
                    time.sleep(0.05)
            trigger_e.clear()
    t = threading.Thread(target=socket_listener, daemon=True)
    t.start()

    return trigger_e, out_q


if __name__ == "__main__":

    # for _ in range(NUM_BUFFERS):
    #     free_q.put(bytearray(BUFF_SIZE))
    # Start workers
    print("DAEMON: entering daemon routine")
    for i in range(NUM_THREADS):  # adjust based on CPU
        Process(target=worker, daemon=True,args=(buffers,(free_q,full_q,out_q),()),name=f"tpx_file_worker_{i}").start()

    
    start_ioc()
