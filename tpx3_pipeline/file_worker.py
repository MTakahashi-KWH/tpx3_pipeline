import time

import numpy as np
import tpx3awkward.processing as tpx
from pathlib import Path
from queue import Empty
from uuid import uuid6 as uid


def worker(buffers, queues, params):
    free_q, full_q, out_q, file_q = queues
    sid, scan, active, handler = params
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
                Path(handler["fpath"]) / f"buff_{sid.value}_{scan.value}_{index}.parquet"
            )
            if path.exists():
                path = path.with_stem(path.stem+"_"+str(uid()))
            clustered_df.to_parquet(path)
            out_q.put((index, clustered_df))
            file_q.put((index, path))
            # return buffer to pool
            free_q.put(buf_i)
            print(f"[worker]\t finished saving {path}")
            full_q.task_done()
        except Empty:
            time.sleep(.2)
            continue
