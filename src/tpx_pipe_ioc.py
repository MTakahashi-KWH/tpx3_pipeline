import sys

# import dask.dataframe as dd
# import pandas as pd

from p4p.nt import NTNDArray, NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV

# from tpx3awkward.processing import Tpx3Config  #TODO
from ctypes import c_int, c_bool

# from queue import Queue
# import threading
from multiprocessing import JoinableQueue, shared_memory, Process, Value, Manager, Event
import time
from pathlib import Path

from .file_worker import worker
from .socket_listener import socket_listener, BUFF_SIZE

Queue = JoinableQueue

NUM_THREADS = 6
NUM_BUFFERS = 16

SID = Value(c_int, -1)
SCAN = Value(c_int, -1)
ACTIVE = Value(c_bool, True)
data_dir = Path.cwd() / "data"
data_dir.mkdir(exist_ok=True)
OUTPUT_DIR = data_dir
# TPX_CONFIG = Tpx3Config.from_defaults() #TODO

free_q = Queue()
full_q = Queue()
out_q = Queue()
file_q = Queue()

# Preallocate buffers
buffers = []
for i in range(NUM_BUFFERS):
    shm = shared_memory.SharedMemory(create=True, size=BUFF_SIZE)
    buffers.append(shm)
    free_q.put(i)  # pass index, not data



def start_ioc(manager,triggerable):
    sid = SharedPV(nt=NTScalar("d"), initial=SID)

    @sid.put
    def scan_id(sid, op):
        sid.post(op.value())
        SID.value = op.value()
        SCAN.value = -1
        op.done()

    scan = SharedPV(nt=NTScalar("d"), initial=SCAN)

    @scan.put
    def scan_num(scan, op):
        if op.value() == -1:
            SCAN.value = SCAN.value + 1
            scan.post(SCAN.value)
        else:
            scan.post(op.value())
            SCAN.value = op.value()
        op.done()

    path = SharedPV(nt=NTScalar("s"), initial=manager["fpath"])

    @path.put
    def pathing(path, op):
        path.post(op.value())
        manager["fpath"] = Path(op.value())
        op.done()

    active = SharedPV(nt=NTScalar("?"), initial=ACTIVE.value)

    @active.put
    def activate(active, op):
        active.post(op.value())
        ACTIVE.value = op.value()
        op.done()

    start = SharedPV(nt=NTScalar("?"), initial=False)

    @start.put
    def starting(start, op):
        if ACTIVE.value:
            triggerable.set()
            SCAN.value = SCAN.value + 1
        op.done()

    # broadcast = SharedPV(nt=NTNDArray(),initial = np.zeros((257,257)))
    file_stream = SharedPV(nt=NTScalar("s"), initial=False)

    providers = [
        {
            "tpx:pipe:sid": sid,
            "tpx:pipe:scan": sid,
            "tpx:pipe:path": path,
            "tpx:pipe:active": active,
            "tpx:pipe:trigger": start,
            # "tpx:pipe:broadcast": broadcast,
        }
    ]
    Server.forever(providers=providers)


def test_boot():
    trigger_e = Event()
    print("[pipeline]\t starting up")
    deploy({"fpath": OUTPUT_DIR})
    print("[pipeline]\t daemon processes deployed")

    t = Process(target=socket_listener, args=(
                trigger_e,
                buffers,
                (free_q, full_q, out_q, file_q)),
                  daemon=True)
    t.start()

    return trigger_e, out_q, file_q


def deploy(data_host):
    print("DAEMON: entering daemon routine")
    for i in range(NUM_THREADS):  # adjust based on CPU
        Process(
            target=worker,
            daemon=True,
            args=(
                buffers,
                (free_q, full_q, out_q, file_q),
                (SID, SCAN, ACTIVE, data_host),
            ),
            name=f"tpx_file_worker_{i}",
        ).start()


if __name__ == "__main__":
    # for _ in range(NUM_BUFFERS):
    #     free_q.put(bytearray(BUFF_SIZE))
    # Start workers
    with Manager() as manager:
        if sys.argv[1] is not None:
            OUTPUT_DIR = sys.argv[1]
        # Create a shared dictionary
        shared_dict = manager.dict()
        shared_dict["fpath"] = OUTPUT_DIR
        deploy(shared_dict)
        start_ioc(shared_dict)
