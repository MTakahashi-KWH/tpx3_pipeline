import json
import time
from pathlib import Path
from queue import Empty
from uuid import uuid6 as uid

import numpy as np
import pyarrow as pa
import tpx3awkward as tpx
from tpx3awkward.processing.config import Tpx3Config

CONFIG_DIR = Path.cwd()/"tpx3.json"


def worker(buffers, queues, params):
    in_bufs, str_bufs = buffers
    free_q, full_q, str_q, out_q, file_q = queues
    sid, scan, active, handler = params
    cf_path = None
    tpx3_config = None
    claim = False
    while True:
        try:
            buf_i, size, index = full_q.get()  # take ownership
            claim = True
            print(f"[worker]\tclaimed sequence item {index} of size {size} with tail(?) length {size % 8}")

            if size == 0:
                print("[worker]\tempty or terminal buffer, skipping")
                free_q.put(buf_i)
                full_q.task_done()
                continue

            # zero-copy cast
            buf = in_bufs[buf_i].buf
            arr = np.frombuffer(buf[:size], dtype="<u8")

            # do processing
            print(f"[worker]\tProcessing {arr.size} ints")
            if cf_path != handler["cfigpath"]:
                cf_path = Path(handler["cfigpath"])
                try:
                    with Path.open(cf_path) as f:
                        tpx3config_json = json.load(f)
                    tpx3_config = Tpx3Config(**tpx3config_json)
                except FileNotFoundError:
                    tpx3_config = Tpx3Config.from_defaults()
            clustered_df = tpx.convert_tpx3_binary(arr,config=tpx3_config)
            free_q.put(buf_i)               # return buffer to pool
            print(f"[worker]\tgenerated dataframe of shape {clustered_df.shape!s}")

            # file saving
            path = Path(handler["fpath"]) / f"buff_{sid.value}_{scan.value}_{index}.parquet"
            if path.exists():
                path = path.with_stem(path.stem+"_"+str(uid()))

            clustered_df.to_parquet(path)
            file_q.put((index, path))
            print(f"[worker]\tfinished saving {path}")
            claim = False
            full_q.task_done()

            # pass out to zmq stream through arrow ipc buffer
            str_ind = str_q.get()
            str_q.task_done()
            out_buf = str_bufs[str_ind].buf
            pybuf = pa.py_buffer(out_buf)
            with pa.output_stream(pybuf) as sink:
                batch  = pa.RecordBatch.from_pandas(clustered_df, preserve_index=False)
                with pa.ipc.new_stream(sink,batch.schema) as writer:
                    writer.write(batch)
                nbytes = sink.tell()
            print(f"[worker]\twrote {nbytes} bytes into buffer giving scaling {nbytes/size}")
            out_q.put((index,str_ind, nbytes))

        except Empty:
            time.sleep(.2)
            continue
        except KeyboardInterrupt:
            print(f"[worker]\tkeyboard interrupt with memory claim {claim}")
            if claim:
                full_q.task_done()
            return
        except Exception as e:
            print(f"[worker]\tpassed exception if not caught by logstream:\n {e}")
            raise e from None
