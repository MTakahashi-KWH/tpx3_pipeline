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
from pathlib import Path

from .file_worker import worker
from .socket_listener import socket_listener, BUFF_SIZE

Queue = JoinableQueue

NUM_THREADS = 6
NUM_BUFFERS = 16

SID = Value(c_int, -1)
SCAN = Value(c_int, 0)
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

    scan = SharedPV(nt=NTScalar("d"), initial=SCAN)

    @scan.put
    def scan_num(scan, op):
        if op.value() == -1:
            SCAN.value = SCAN.value + 1
        else:
            SCAN.value = op.value()
        scan.post(SCAN.value)
        op.done()

    sid = SharedPV(nt=NTScalar("d"), initial=SID)

    @sid.put
    def scan_id(sid, op):
        if op.value() == -1:
            SID.value = SID.value + 1
        else:
            SID.value = op.value()
        sid.post(SID.value)
        SCAN.value = 0
        scan.post(SCAN.value)
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
            if op.value():
                triggerable.set()
                SCAN.value = SCAN.value + 1
                start.post(op.value())
            else:
                start.post(op.value())

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


def test_boot(alt_dir: Path =None):
    print("[pipeline]\t starting up")
    trigger_e = deploy({"fpath": OUTPUT_DIR if alt_dir is None else alt_dir})
    print("[pipeline]\t daemon processes deployed")

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
    
    trigger_e = Event()
    t = Process(target=socket_listener, args=(
                trigger_e,
                buffers,
                (free_q, full_q, out_q, file_q)),
                  daemon=True)
    t.start()
    return trigger_e

def close():
    full_q.join()
    file_q.join()
    out_q.join()
    for buf in buffers:
        buf.close()
        buf.unlink()

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
        close()
