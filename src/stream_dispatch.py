from p4p.nt import NTNDArray, NTScalar
from p4p.server import Server
from p4p.server.thread import SharedPV
from queue import Empty
import time

def file_stream_worker(file_queue):
    file_stream = SharedPV(nt=NTScalar("s"), initial=False)

    with Server(providers=[{"tpx:pipe:file":file_stream}]) as serv:
        while True:
            try:
                index, path = file_queue.get()
                file_stream.post(str(path))
            except Empty:
                time.wait(.1)