import sys

import numpy as np
import socket
# import dask.dataframe as dd
# import pandas as pd

from p4p.nt import NTNDArray, NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV
import tpx3awkward.processing as tpx

# from tpx3awkward.processing import Tpx3Config  #TODO
from ctypes import c_int, c_bool

# from queue import Queue
import threading
from multiprocessing import JoinableQueue, shared_memory, Process, Value, Manager
from queue import Empty
import time
from pathlib import Path

Queue = JoinableQueue

HOST = "localhost"
SERVAL = 8088
BUFF_SIZE = 10 * 1024 * 1024
NUM_BUFFERS = 16
NUM_THREADS = 6

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


def stream(sock):
    flag_end = False
    index = 0
    while True:
        buf_i = free_q.get()  # take ownership
        buf = buffers[buf_i].buf
        # view = memoryview(buf)

        read_total = 0
        while (read_total < BUFF_SIZE * 0.8) or (read_total % 8 != 0):
            n = sock.recv_into(buf[read_total:])
            if n == 0:
                if flag_end:
                    break
                flag_end = True
                print(
                    "[socket]\t 0 packet received... suspecting eot... awaiting next update"
                )
            read_total += n
        print("[socket]\t writing out sequence ", index, " to buffer ", buf_i)
        # hand off buffer (no copy)
        full_q.put((buf_i, read_total - (read_total % 8), index))
        index += 1
        if flag_end:
            return


def worker(buffers, queues, params):
    free_q, full_q, out_q, file_q = queues
    sid, scan, active, handler = params
    while True:
        if active.value:
            try:
                buf_i, size, index = full_q.get()  # take ownership
                print(
                    "[worker]\t claimed sequence item ",
                    index,
                    " of size ",
                    size,
                    " with tail(?) length ",
                    size % 8,
                )
                if size == 0:
                    print("[worker]\t empty or terminal buffer, skipping")
                    free_q.put(buf_i)
                    full_q.task_done()
                    continue

                buf = buffers[buf_i].buf
                # zero-copy cast
                arr = np.frombuffer(buf[:size], dtype="<u8")
                # do processing
                print(f"[worker]\t Processing {arr.size} ints")
                res = tpx.decode_tpx3_binary(
                    arr
                )  # pd.DataFrame(tpx.ingest_raw_data(arr)).sort_values("t").reset_index(drop=True)

                clustered_df = tpx.cluster_raw_df( res, 0.3, 3,)
                
                path = (
                    handler["fpath"] / f"buff_{sid.value}_{scan.value}_{index}.parquet"
                )
                clustered_df.to_parquet(path)
                out_q.put((index, clustered_df))
                file_q.put((index, path))
                # return buffer to pool
                free_q.put(buf_i)
                print(f"[worker]\t finished saving {path}")
                full_q.task_done()
            except Empty:
                continue


def trigger():
    # Setup
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, SERVAL))
        # Start reader (main thread or separate)
        stream(sock)
        full_q.join()
        # ddf = assemble(out_q)
        # callback(ddf)


def start_ioc(manager):
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
        for i in range(10):
            print("[pipeline]\t connection attempt: ", i)
            try:
                trigger()
                # print("... sucessful")
                break
            except OSError as e:
                print("... failed", e)
                time.sleep(0.05)
        op.done()

    # broadcast = SharedPV(nt=NTNDArray(),initial = np.zeros((257,257)))

    Server.forever(
        providers=[
            {
                "tpx:pipe:sid": sid,
                "tpx:pipe:scan": sid,
                "tpx:pipe:path": path,
                "tpx:pipe:active": active,
                "tpx:pipe:trigger": start,
                # "tpx:pipe:broadcast": broadcast,
            }
        ]
    )


def test_boot():
    trigger_e = threading.Event()
    print("[pipeline]\t starting up")
    deploy({"fpath": OUTPUT_DIR})
    print("[pipeline]\t daemon processes deployed")

    def socket_listener():
        while True:
            trigger_e.wait()
            print("[pipeline]\t Triggered: connecting...")
            for i in range(10):
                print("[pipeline]\t connection attempt: ", i)
                try:
                    trigger()
                    # print("... sucessful")
                    return
                except OSError as e:
                    print("... failed", e)
                    time.sleep(0.05)
            trigger_e.clear()

    t = threading.Thread(target=socket_listener, daemon=True)
    t.start()

    return trigger_e, out_q


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
