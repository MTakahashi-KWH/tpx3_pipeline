import tpx_pipe_ioc as tpi
import dask.dataframe as dd
import threading
import socket
from pathlib import Path
import numpy as np
import time
import pandas as pd
import tpx3awkward as tpx
import sys
import requests
# from urllib.parse import urlparse


if __name__ == "__main__":
    trigger, out_q = tpi.test_boot()
    if sys.argv[1] is not None:
        requests.get(str(sys)+"/measurement/start")
    else:
        requests.get('https://'+tpi.HOST +":"  + tpi.SERVAL+"/measurement/start")

    trigger.set()
    while trigger.is_set():
        print("[monitor]\t stream is still set (yay) waiting")
        time.sleep(5)



