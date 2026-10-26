"""Independent 200 ms NVML acquisition, including device idle after source exit."""
import argparse
import csv
import ctypes as c
import json
import os
import signal
import time
from pathlib import Path


class Memory(c.Structure):
    _fields_=[('total',c.c_ulonglong),('free',c.c_ulonglong),('used',c.c_ulonglong)]
class MemoryV2(c.Structure):
    _fields_=[('version',c.c_uint),('total',c.c_ulonglong),('reserved',c.c_ulonglong),('free',c.c_ulonglong),('used',c.c_ulonglong)]
class Util(c.Structure):
    _fields_=[('gpu',c.c_uint),('memory',c.c_uint)]
class Process(c.Structure):
    _fields_=[('pid',c.c_uint),('usedGpuMemory',c.c_ulonglong),('gpuInstanceId',c.c_uint),('computeInstanceId',c.c_uint)]


class NVML:
    def __init__(self):
        self.lib=c.CDLL('libnvidia-ml.so.1')
        if self.lib.nvmlInit_v2()!=0: raise RuntimeError('NVML init failed')
    def handle(self,index):
        h=c.c_void_p()
        if self.lib.nvmlDeviceGetHandleByIndex_v2(c.c_uint(index),c.byref(h))!=0: raise RuntimeError('NVML handle failed')
        return h
    def scalar(self,h,name,*args,scale=1):
        value=c.c_uint(); rc=getattr(self.lib,name)(h,*args,c.byref(value))
        return value.value/scale if rc==0 else None
    def processes(self,h):
        count=c.c_uint(128); array=(Process*128)()
        rc=self.lib.nvmlDeviceGetComputeRunningProcesses_v3(h,c.byref(count),array)
        if rc!=0: raise RuntimeError(f'NVML process query error {rc}')
        return [{'pid':array[i].pid,'used_bytes':array[i].usedGpuMemory} for i in range(count.value)]
    def row(self,index,role):
        h=self.handle(index); uuid=c.create_string_buffer(100)
        self.lib.nvmlDeviceGetUUID(h,uuid,c.c_uint(100))
        memory=MemoryV2();memory.version=(2<<24)|c.sizeof(MemoryV2);util=Util()
        mr=self.lib.nvmlDeviceGetMemoryInfo_v2(h,c.byref(memory)) if hasattr(self.lib,'nvmlDeviceGetMemoryInfo_v2') else -1
        memory_api='v2';reserved=memory.reserved/1048576 if mr==0 else None
        if mr!=0:
            memory=Memory();mr=self.lib.nvmlDeviceGetMemoryInfo(h,c.byref(memory));memory_api='v1 (used includes reserved)'
        ur=self.lib.nvmlDeviceGetUtilizationRates(h,c.byref(util))
        return {'role':role,'gpu_index':index,'gpu_uuid':uuid.value.decode(),
            'monotonic_s':time.monotonic(),'wall_ns':time.time_ns(),
            'power_w':self.scalar(h,'nvmlDeviceGetPowerUsage',scale=1000),
            'memory_mib':memory.used/1048576 if mr==0 else None,
            'reserved_memory_mib':reserved,'nvml_memory_api':memory_api,
            'utilization_pct':util.gpu if ur==0 else None,
            'temperature_c':self.scalar(h,'nvmlDeviceGetTemperature',c.c_uint(0)),
            'sm_clock_mhz':self.scalar(h,'nvmlDeviceGetClockInfo',c.c_uint(1)),
            'memory_clock_mhz':self.scalar(h,'nvmlDeviceGetClockInfo',c.c_uint(2)),
            'power_limit_w':self.scalar(h,'nvmlDeviceGetPowerManagementLimit',scale=1000),
            'compute_pids':json.dumps(self.processes(h),separators=(',',':'))}


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
    nv=NVML(); running=True
    def stop(*_):
        nonlocal running;running=False
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    path=Path(a.output);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',newline='') as f:
        first=nv.row(0,'S');writer=csv.DictWriter(f,fieldnames=list(first));writer.writeheader()
        due=time.monotonic()
        while running:
            for index,role in ((0,'S'),(1,'D')):
                writer.writerow(nv.row(index,role))
            f.flush();due+=.2;time.sleep(max(0,due-time.monotonic()))


if __name__=='__main__':main()
