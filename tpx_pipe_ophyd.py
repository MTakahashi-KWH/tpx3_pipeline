from ophyd import Device, Component, DeviceStatus, EpicsSignal, EpicsSignalRO, Signal
from ophyd.status import SubscriptionStatus
from pathlib import Path
from bisect import insort
from bluesky.protocols import Flyable

class tpx3_pipe(Device):
    sid = Component(EpicsSignal,"sid")
    scan = Component(EpicsSignal,"scan")
    path = Component(EpicsSignal,"path")
    active = Component(EpicsSignal,"active")
    fire = Component(EpicsSignal,"fire")
    file_stream = Component(EpicsSignalRO,"file",string=True)
    files = Component(Signal, value=[])

    def stage(self):
        self._fileset = set()
        self._files = []
        self.sid.put(-1)
        def file_monitor(obj,value=None,old_value=None,**kwargs):
            print(value)
            if value not in self._fileset:
                insort(self._files,value,
                              key=lambda x:int(Path(x).stem.split("_")[3]))
                self._fileset.add(value)
                self.files.put(self._files)
        self.sub_id = self.file_stream.subscribe(file_monitor,run=False)

    def await_revert(self,old_value=None,value=None,**kwargs):
        return (not value)

    def trigger(self):
        self._fileset = set()
        self._files = []
        self.files.put(self._files)
        status = SubscriptionStatus(self.fire,self.await_revert,run=False)
        self.fire.put(True)
        return status
    
    def kickoff(self):
        self._fileset = set()
        self._files = []
        self.files.put(self._files)
        self.fire.put(True)
        return SubscriptionStatus(self.fire,lambda *args, **kwargs: not self.await_revert(*args,**kwargs),run=False)
        
    def complete(self):
        return SubscriptionStatus(self.fire,self.await_revert,run=False)
        

    def unstage(self):
        self.file_stream.unsubscribe(self.sub_id)


pipeline = tpx3_pipe("tpx:pipe:",name="tpx_pipe")
