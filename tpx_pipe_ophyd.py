from ophyd import Device, Component, DeviceStatus, EpicsSignal, EpicsSignalRO, Signal
from ophyd.status import SubscriptionStatus
from pathlib import Path
from bisect import insort
from bluesky.protocols import Flyable

class tpx3_pipe(Device,Flyable):
    sid = Component(EpicsSignal,"sid")
    scan = Component(EpicsSignal,"scan")
    path = Component(EpicsSignal,"path")
    config = Component(EpicsSignal,"config")
    active = Component(EpicsSignal,"active")
    fire = Component(EpicsSignal,"fire")
    file_stream = Component(EpicsSignalRO,"file")
    files = Component(Signal, value=[])

    def stage(self):
        self._fileset = {}
        self._files = []
        def file_monitor(obj,value=None,old_value=None,**kwargs):
            if value not in self._fileset:
                insort(self._files,value, key=lambda x:int(Path(x).stem.split("_")[3]))
                self._fileset.add(value)
                self.files.put(self._files)
        self.sub_id = self.file_stream.subscribe(file_monitor,run=False)

    def await_revert(self,old_value=None,value=None,**kwargs):
        return (not value)

    def trigger(self):
        self._fileset = {}
        self._files = []
        self.files.put(self._files)
        status = SubscriptionStatus(self.fire,self.await_revert,run=False)
        self.fire.set(True)
        return status
    
    def kickoff(self):
        self._fileset = {}
        self._files = []
        self.files.put(self._files)
        self.fire.set(True)
        return SubscriptionStatus(self.fire,lambda *args, **kwargs: not self.await_revert(*args,**kwargs),run=False)
        
    def complete(self):
        # you should really think of triggering suspend of the timepix sampling if you are using this interface
        # also collectable should be an interface outside of read at some point
        return SubscriptionStatus(self.fire,self.await_revert,run=False)
        

    def unstage(self):
        self.file_stream.unsubscribe(self.sub_id)


pipeline = tpx3_pipe("tpx:pipe:",name="tpx_pipe")
