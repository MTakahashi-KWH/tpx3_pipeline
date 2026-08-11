import struct
from collections import OrderedDict
from queue import Empty

import pyarrow as pa
import zmq

# import time

DISPATCHER_PORT = 5557
_DISPATCHER_POLL_TIMEOUT = 0.2   # seconds to wait on out_q before checking for gaps
_DISPATCHER_GAP_RETRIES  = 6      # how many timeouts to tolerate before skipping a gap


def dispatcher(queues,params,buffs,zmq_port=DISPATCHER_PORT):
    """
    Process target: drain out_q, resequence frames up to disorder depth 4 with a simple dict, and
    publish in cleaner index order over a ZMQ PUB socket.

    Ordering policy
    ---------------
    - Frames whose index equals `next_expected` are emitted immediately.
    - Out-of-order frames are held in the dict until as close to their turn as possible
    """
    recovery_queue = OrderedDict()
    next_expected = 0
    pending_buffers = []
    dropout = set()
    out_q, str_q = queues
    SID, SCAN = params

    def pop_next(shelve=None):
        nonlocal recovery_queue,next_expected, dropout

        if shelve:
            ind, df = shelve
            recovery_queue[ind] = df

        if next_expected in recovery_queue:
            return next_expected, recovery_queue.pop(next_expected)

        standing = [idx for idx in recovery_queue if idx> next_expected]

        if standing:
            idx = min(standing)
            df = recovery_queue.pop(idx)
            dropout.add(idx)

            if idx > (next_expected+ 10): # packet is considered truly lost
                next_expected +=1 
            return idx,df

        idx = min(recovery_queue)
        df = recovery_queue.pop(idx)
        dropout = {idx}
        next_expected =0
        return idx, df

    def free_buf(ponder):
        ind, pend = ponder
        if pend.done:
            str_q.put(ind)
            return False
        return True

    # startup
    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    socket.setsockopt(zmq.SNDHWM, len(buffs)//2)   # drop for slow subscribers beyond HWM
    socket.bind(f"tcp://*:{zmq_port}")
    print(f"[dispatcher]\t ZMQ PUB bound on tcp://*:{zmq_port}")

    #core loop
    try:
        while True:
            pending_buffers = list(filter(free_buf,pending_buffers))
            try:
                index, str_ind,nbytes = out_q.get(timeout= _DISPATCHER_POLL_TIMEOUT)  # take ownership
                print(f"[DISPATCHER] received packet {index!s}, waiting for {next_expected}")

                if index != next_expected:
                    if len(recovery_queue) <= _DISPATCHER_GAP_RETRIES:
                        recovery_queue[index] = (str_ind,nbytes)
                        if next_expected not in recovery_queue:
                            continue

                    index, tup = pop_next((index,(str_ind,nbytes)))
                    str_ind,nbytes = tup
                    print(f"[DISPATCHER] popped packet {index!s}, waiting for {next_expected}")

            except Empty:
                if next_expected not in recovery_queue:
                    continue

                index, tup = pop_next()
                str_ind,nbytes = tup
                print(f"[DISPATCHER] popped target packet {index!s}")

            if index == next_expected:
                next_expected +=1             
            
            while next_expected in dropout:
                next_expected += 1

            print(f"[DISPATCHER] publishing frame of index: {index} while targeting {next_expected}")
            dropout.add(index)

            buf = buffs[str_ind].buf[:nbytes]
            header = struct.pack(">qqq", index, SID.value, SCAN.value)
            pend = socket.send_multipart(
                [b"tpx", header, buf],
                copy=False,
                track=True,
            )
            pending_buffers.append((str_ind,pend))

    except KeyboardInterrupt:
        socket.close()
        ctx.term()
        





        
    
