import time

import numpy as np
import tpx3awkward as tpx
from tpx3awkward.processing.config import Tpx3Config
import json
from pathlib import Path
from queue import Empty
from uuid import uuid6 as uid
import pyarrow as pa

CONFIG_DIR = Path.cwd()/"tpx3.json"


def worker(buffers, queues, params):
    in_bufs, str_bufs = buffers
    free_q, full_q, str_q, out_q, file_q = queues
    sid, scan, active, handler = params
    cf_path = None
    tpx3_config = None
    while True:
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

            buf = in_bufs[buf_i].buf
            # zero-copy cast
            arr = np.frombuffer(buf[:size], dtype="<u8")
            # do processing
            print(f"[worker]\t Processing {arr.size} ints")
            if cf_path != handler["cfigpath"]:
                cf_path = Path(handler["cfigpath"])
                try:
                    with Path.open(cf_path) as f:
                        tpx3config_json = json.load(f)
                    tpx3_config = Tpx3Config(**tpx3config_json)
                except Exception:
                    tpx3_config = Tpx3Config.from_defaults()
            clustered_df = tpx.convert_tpx3_binary(arr,config=tpx3_config)

            path = (
                Path(handler["fpath"]) / f"buff_{sid.value}_{scan.value}_{index}.parquet"
            )
            if path.exists():
                path = path.with_stem(path.stem+"_"+str(uid()))
            clustered_df.to_parquet(path)
            file_q.put((index, path))

            str_ind = str_q.get()
            out_buf = str_bufs[str_ind].buf
            pybuf = pa.py_buffer(out_buf)
            batch  = pa.RecordBatch.from_pandas(clustered_df, preserve_index=False)
            writer = pa.FixedSizeBufferWriter(pybuf)
            writer.write(batch)
            out_q.put((index, clustered_df))
            # return buffer to pool
            free_q.put(buf_i)
            print(f"[worker]\t finished saving {path}")
            full_q.task_done()
        except Empty:
            time.sleep(.2)
            continue
