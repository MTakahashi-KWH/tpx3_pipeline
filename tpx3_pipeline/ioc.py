import argparse

# from tpx3awkward.processing import Tpx3Config  #TODO
from ctypes import c_bool, c_int
from multiprocessing import Event, JoinableQueue, Manager, Process, Value, shared_memory
from pathlib import Path
from queue import Empty

import cothread
from softioc import builder, softioc

from .file_worker import CONFIG_DIR, worker
from .socket_listener import BUFF_SIZE, socket_listener
from .stream_dispatch import dispatcher

Queue = JoinableQueue

NUM_THREADS = 8
NUM_BUFFERS = 20

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
str_q = Queue()
out_q = Queue()
file_q = Queue()

# Preallocate buffers
buffers = []
stream_bufs = []

def start_ioc(manager, triggerable):
    print("[DAEMON] booting ioc")
    builder.SetDeviceName(PREFIX.rstrip(":"))

    dname = "DAEMON"

    def scan_num(value):
        print(f"[{dname}] scan update: {value}")
        if int(value) == -1:
            SCAN.value = SCAN.value + 1
        else:
            SCAN.value = int(value)
        scan.set(SCAN.value)

    def scan_id(value):
        print(f"[{dname}] SID update: {value}")
        if int(value) == -1:
            SID.value = SID.value + 1
        else:
            SID.value = int(value)
        sid.set(SID.value)
        SCAN.value = -1
        scan.set(SCAN.value)

    print("[DAEMON] host directory: ", manager["fpath"])

    def pathing(value):
        print(f"[{dname}] path update: {value}")
        pth = Path(value)
        if pth.exists():
            path.set(str(pth))
            manager["fpath"] = str(pth)

    print("[DAEMON] configuration directory: ", manager["cfigpath"])

    def configuration(value):
        print(f"[{dname}] configuration update: {value}")
        pth = Path(value)
        if pth.exists() and pth.is_file() and pth.suffix == ".json":
            path.set(str(pth))
            manager["cfigpath"] = str(pth)

    def activate(value):
        print(f"[{dname}] actvity set update: {value}")
        ACTIVE.value = bool(value)
        active.set(ACTIVE.value)

    def starting(value):
        print(f"[{dname}] trigger update: {value}")
        if ACTIVE.value:
            if bool(value):
                triggerable.set()
                SCAN.value = SCAN.value + 1
                scan.set(SCAN.value)
                start.set(True)
        else:
            start.set(False)

    # Writable/readable control PVs
    scan = builder.longOut("scan", initial_value=SCAN.value, on_update=scan_num)
    sid = builder.longOut("sid", initial_value=SID.value, on_update=scan_id)
    path = builder.longStringOut(
        "path", initial_value=str(manager["fpath"]), on_update=pathing, length=512
    )
    cfig_path = builder.longStringOut(
        "config", initial_value=str(manager["cfigpath"]), on_update=configuration, length=512
    )
    active = builder.boolOut("active", initial_value=ACTIVE.value, on_update=activate)
    start = builder.boolOut("fire", initial_value=False, on_update=starting)

    # Read-only stream PV, updated by IOC internals only
    file_stream = builder.longStringIn("file", initial_value="", length=512)

    print(f"[{dname}] broadcasting on prefix address: {PREFIX}")

    builder.LoadDatabase()
    softioc.iocInit()
    def update():
        while True:
            try:
                _index, file_path = file_q.get(timeout=.01)
                print(f"[{dname}] posting new file {_index} {file_path}")
                file_stream.set(str(file_path))
                cothread.Yield()
                file_q.task_done()

                if file_q.empty() and full_q.empty():
                    full_q.join()
                    if file_q.empty():
                        start.set(False)
            except Empty:
                cothread.Sleep(.1)
    cothread.Spawn(update)
    # Finally leave the IOC running with an interactive shell.
    try:
        cothread.WaitForQuit() 
    except KeyboardInterrupt:
        cothread.quit()
    # softioc.interactive_ioc(globals())


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

    for i in range(NUM_BUFFERS):
        shm = shared_memory.SharedMemory(create=True, size=5*BUFF_SIZE)
        stream_bufs.append(shm)
        str_q.put(i)  # pass index, not data

    for i in range(NUM_THREADS):  # adjust based on CPU
        Process(
            target=worker,
            daemon=True,
            args=(
                (buffers,stream_bufs),
                (free_q, full_q, str_q, out_q, file_q),
                (SID, SCAN, ACTIVE, data_host),
            ),
            name=f"tpx_file_worker_{i}",
        ).start()
    Process(target=dispatcher,daemon=True,args=((out_q,str_q),(SID,SCAN),stream_bufs), name="tpx_dispatch_worker").start()

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
    
    for buf in stream_bufs:
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
        shared_dict["cfigpath"] = CONFIG_DIR
        trigger = deploy(shared_dict)
        try:
            start_ioc(shared_dict, trigger)
        except KeyboardInterrupt:
            close()
        else:
            close()
