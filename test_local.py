import os
import runpy

if __name__== "__main__":
    os.environ["EPICS_CA_ADDR_LIST"] = "127.0.0.1" # Replace with your subnet
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"
    os.environ["EPICS_CAS_INTF_ADDR_LIST"] = "127.0.0.1"
    os.environ["EPICS_CAS_BEACON_ADDR_LIST"] = "127.0.0.1"
    os.environ["EPICS_CAS_AUTO_BEACON_ADDR_LIST"] = "NO"



    runpy.run_module("tpx3_pipeline.ioc", run_name="__main__")