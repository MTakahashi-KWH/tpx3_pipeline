from p4p.nt import NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV

# from tpx3awkward.processing import Tpx3Config  #TODO
from ctypes import c_int, c_bool

from multiprocessing import JoinableQueue, shared_memory, Process, Value, Manager, Event
from queue import Empty
import time
from pathlib import Path
from bisect import insort
import argparse

from .file_worker import worker
from .socket_listener import socket_listener, BUFF_SIZE
from .stream_dispatch import dispatcher

Queue = JoinableQueue

NUM_THREADS = 6
NUM_BUFFERS = 16

SID = Value(c_int, -1)
SCAN = Value(c_int, -1)
ACTIVE = Value(c_bool, True)
data_dir = Path.cwd() / "data"
data_dir.mkdir(exist_ok=True)
OUTPUT_DIR = data_dir
PREFIX = "tpx:pipe:"
# TPX_CONFIG = Tpx3Config.from_defaults() #TODO

free_q = Queue()
full_q = Queue()
out_q = Queue()
file_q = Queue()

# Preallocate buffers
buffers = []


def start_ioc(manager, triggerable):
    print("[DAEMON] booting ioc")
    scan = SharedPV(nt=NTScalar("d"), initial=SCAN.value)
    dname = "DAEMON"
    file_accum = []

    @scan.put
    def scan_num(scan, op):
        print(f"[{dname}] scan update: {op.value()}")
        if op.value() == -1:
            SCAN.value = SCAN.value + 1
        else:
            SCAN.value = int(op.value())
        scan.post(SCAN.value)
        op.done()

    sid = SharedPV(nt=NTScalar("d"), initial=SID.value)

    @sid.put
    def scan_id(sid, op):
        print(f"[{dname}] SID update: {op.value()}")
        if op.value() == -1:
            SID.value = SID.value + 1
        else:
            SID.value = int(op.value())
        sid.post(SID.value)
        SCAN.value = -1
        scan.post(SCAN.value)
        op.done()

    print("[DAEMON] host directory: ", manager["fpath"])
    path = SharedPV(nt=NTScalar("s"), initial=str(manager["fpath"]))

    @path.put
    def pathing(path, op):
        print(f"[{dname}] path update: {op.value()}")
        pth = Path(op.value())
        if pth.exists():
            path.post(op.value())
            manager["fpath"] = str(pth)
        op.done()

    active = SharedPV(nt=NTScalar("?"), initial=ACTIVE.value)

    @active.put
    def activate(active, op):
        print(f"[{dname}] actvity set update: {op.value()}")
        active.post(op.value())
        ACTIVE.value = op.value()
        op.done()

    start = SharedPV(nt=NTScalar("?"), initial=False)
    file_block = SharedPV(nt=NTScalar("as"), initial=[])

    @start.put
    def starting(start, op):
        print(f"[{dname}] trigger update: {op.value()}")
        if ACTIVE.value:
            if op.value():
                file_accum = []
                file_block.post([])
                triggerable.set()
                SCAN.value = SCAN.value + 1
                start.post(op.value())

        op.done()

    # broadcast = SharedPV(nt=NTNDArray(),initial = np.zeros((257,257)))
    file_stream = SharedPV(nt=NTScalar("s"), initial=False)

    # @file_stream.put
    # def post_file(pv,op):
    #     print(f"[{dname}] new file update: {op.value()}")
    #     pv.post(op.value())
    #     op.done()

    providers = [
        {
            f"{PREFIX}sid": sid,
            f"{PREFIX}scan": scan,
            f"{PREFIX}path": path,
            f"{PREFIX}active": active,
            f"{PREFIX}fire": start,
            f"{PREFIX}file": file_stream,
            f"{PREFIX}files": file_block,
            # "tpx:pipe:broadcast": broadcast,
        }
    ]
    print(f"[{dname}] broadcasting on preifx address: {PREFIX}")
    with Server(providers=providers) as serv:
        while True:
            try:
                index, path = file_q.get()
                file_stream.post(str(path))
                insort(
                    file_accum, str(path), key=lambda x: int(Path(x).stem.split("_")[3])
                )
                file_block.post(file_accum)
                file_q.task_done()

                if file_q.empty() and full_q.empty():
                    full_q.join()
                    if file_q.empty():
                        start.post(False)
            except Empty:
                time.sleep(0.1)
    # Server.forever(providers=providers)


def test_boot(alt_dir: Path = None):
    print("[pipeline]\t starting up")
    trigger_e = deploy({"fpath": OUTPUT_DIR if alt_dir is None else alt_dir})
    print("[pipeline]\t daemon processes deployed")

    return trigger_e, out_q, file_q


def deploy(data_host):
    print("[DAEMON] entering daemon routine")

    for i in range(NUM_BUFFERS):
        shm = shared_memory.SharedMemory(create=True, size=BUFF_SIZE)
        buffers.append(shm)
        free_q.put(i)  # pass index, not data
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
    Process(target=dispatcher,daemon=True,args=(out_q,(SID,SCAN)), name="tpx_dispatch_worker").start()

    trigger_e = Event()
    t = Process(
        target=socket_listener,
        args=(trigger_e, buffers, (free_q, full_q, out_q, file_q)),
        daemon=True,
    )
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
    parser = argparse.ArgumentParser(
        description="CLI deployment of python IOC for timepix3 pipeline"
    )
    parser.add_argument(
        "--path",
        type=str,
        dest="PATH",
        help="A user provided default output directory. Can be changed at runtime",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        dest="prefix",
        help="Used to set prefix of the epics channels, in case of name collision with multiple IOCs",
    )
    args = parser.parse_args()
    if args.PATH is not None:
        OUTPUT_DIR = args.PATH

    if args.prefix is not None:
        PREFIX = args.prefix

    with Manager() as manager:
        # Create a shared dictionary
        shared_dict = manager.dict()
        shared_dict["fpath"] = OUTPUT_DIR
        trigger = deploy(shared_dict)
        try:
            start_ioc(shared_dict, trigger)
        except KeyboardInterrupt:
            close()
        else:
            close()
