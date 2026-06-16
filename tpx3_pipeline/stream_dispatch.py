import struct
from collections import OrderedDict
import zmq
import pyarrow as pa
from queue import Empty
import time

DISPATCHER_PORT = 5557
_DISPATCHER_POLL_TIMEOUT = 0.2   # seconds to wait on out_q before checking for gaps
_DISPATCHER_GAP_RETRIES  = 6      # how many timeouts to tolerate before skipping a gap

def _publish(socket, idx, sid_val, scan_val, df):
    """Serialize df as Arrow IPC and send as a ZMQ PUB multipart message."""
    batch  = pa.RecordBatch.from_pandas(df, preserve_index=False)
    sink   = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    # arrow_bytes = sink.getvalue().to_pybytes()
    header = struct.pack(">qqq", idx, sid_val, scan_val)
    # Explicit memoryview
    buf = sink.getvalue()
    socket.send_multipart(
        [b"tpx", header, memoryview(buf)],
        copy=False
    )
    # socket.send_multipart([b"tpx", header, arrow_bytes])

def dispatcher(out_q,params,zmq_port=DISPATCHER_PORT):
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
    SID, SCAN = params

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    socket.setsockopt(zmq.SNDHWM, 1000)   # drop for slow subscribers beyond HWM
    socket.bind(f"tcp://*:{zmq_port}")
    print(f"[dispatcher]\t ZMQ PUB bound on tcp://*:{zmq_port}")
    try:
        while True:
            try:
                index, clustered_df = out_q.get(timeout= _DISPATCHER_POLL_TIMEOUT)  # take ownership
            except Empty:
                if next_expected in recovery_queue.keys():
                    index, clustered_df = next_expected, recovery_queue.pop(next_expected)
                else:
                    f = [str(i) for i in recovery_queue.keys()]
                    print(f"[DISPATCHER] holding onto keys: {' '.join(f)}" )
                    continue

            if index == next_expected:
                next_expected +=1 
            elif index < next_expected:
                if any([idx >= next_expected for idx in recovery_queue.keys()]):
                    recovery_queue[index] = clustered_df
                    if len(recovery_queue) <= _DISPATCHER_GAP_RETRIES:
                        continue
                    
                    index, clustered_df = recovery_queue.popitem(last=False)
                else:
                    next_expected = index +1 
            else:
                recovery_queue[index] = clustered_df
                if len(recovery_queue) <= _DISPATCHER_GAP_RETRIES:
                    continue
                
                index, clustered_df = recovery_queue.popitem(last=False)
            print(f"[DISPATCHER] publishing frame of index: {index}")
            _publish(socket=socket,idx=index,sid_val=SID.value,scan_val=SCAN.value,df=clustered_df)
    except KeyboardInterrupt:
        socket.close()
        ctx.term()
        





        
    
