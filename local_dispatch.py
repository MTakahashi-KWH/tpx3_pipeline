import tpx_pipeline.tpx_pipe_ioc as tpi
import tpx_pipeline.socket_listener as tpl
import time
import sys
import requests
# from urllib.parse import urlparse


if __name__ == "__main__":
    trigger, out_q = tpi.test_boot()
    if len(sys.argv) > 1:
        servurl = 'https://'+str(sys.argv[1])
    else:
        servurl = 'https://'+tpl.HOST +":"+ str(tpl.SERVAL)
    
    requests.get(servurl+"/measurement/start")

    trigger.set()
    cycler = 0
    while trigger.is_set():
        if cycler == 0:
            print("[monitor]\t stream is still set (yay) waiting")
        cycler += 1
        cycler %= 30
        time.sleep(1)
    
    print("[monitor]\t stream has ended")
    requests.get(servurl+"/measurement/stop")
    tpi.close()




