from queue import Empty
import time

def stream_worker(out_queue):
    while True:
        try:
            index, frame = out_queue.get()

        except Empty:
            time.wait(.1)