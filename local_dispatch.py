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
        requests.get('https://'+str(sys.argv[1])+"/measurement/start")
    else:
        requests.get('https://'+tpi.HOST +":"  + tpi.SERVAL+"/measurement/start")

    trigger.set()
    cycler = 0
    while trigger.is_set():
        if cycler == 0:
            print("[monitor]\t stream is still set (yay) waiting")
        cycler += 1
        cycler %= 30
        time.sleep(1)
    
    print("[monitor]\t stream has ended")



