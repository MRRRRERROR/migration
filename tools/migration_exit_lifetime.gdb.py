"""GDB lifecycle trace. Break/return observations do not alter native code."""
import gdb
import json
import os
import time

path=os.environ['MIGRATION_EXIT_GDB_TRACE']
stream=open(path,'x',buffering=1)
active={}
thread_roles={}
tracked_objects=set()
delete_breakpoints=[]

def frames(limit=50):
    result=[];frame=gdb.newest_frame()
    while frame and len(result)<limit:
        result.append({'function':frame.name(),'pc':hex(int(frame.pc()))})
        frame=frame.older()
    return result

def emit(event,**fields):
    thread=gdb.selected_thread()
    row={'event':event,'monotonic_ns':time.monotonic_ns(),'wall_ns':time.time_ns(),
         'inferior_pid':gdb.selected_inferior().pid,'thread_ptid':thread.ptid if thread else None,**fields}
    stream.write(json.dumps(row,default=str)+'\n')

def address_this(function):
    try:return int(gdb.parse_and_eval('this'))
    except gdb.error:
        try:return int(gdb.parse_and_eval('$rsi' if 'ToString' in function else '$rdi'))
        except gdb.error:return 0

def memory(address,size=64):
    try:return bytes(gdb.selected_inferior().read_memory(address,size)).hex()
    except gdb.error:return None

class Return(gdb.FinishBreakpoint):
    def __init__(self,key,function,obj):
        super().__init__(gdb.newest_frame(),internal=True)
        self.key=key;self.function=function;self.obj=obj
    def stop(self):
        emit('function_return',function=self.function,object=hex(self.obj),return_value=str(self.return_value),rax=str(gdb.parse_and_eval('$rax')),
             memory=memory(self.obj))
        active.pop(self.key,None)
        return False
    def out_of_scope(self):
        emit('return_out_of_scope',function=self.function,object=hex(self.obj));active.pop(self.key,None)

class Entry(gdb.Breakpoint):
    def __init__(self,function):
        super().__init__(function,internal=False);self.function=function
    def stop(self):
        obj=address_this(self.function);thread=gdb.selected_thread();key=(thread.ptid,self.function,obj)
        if obj and ('XChannelTcpImpl' in self.function or 'XContextTcpImpl' in self.function):
            tracked_objects.add(obj)
        if obj and ('~TcpDevice' in self.function or '~XDevice' in self.function):
            tracked_objects.add(obj)
        if obj and 'ToString' in self.function:
            raw=memory(obj)
            if raw:
                device=int.from_bytes(bytes.fromhex(raw)[48:56],'little')
                if device:
                    tracked_objects.add(device)
                    emit('channel_device_reference',channel=hex(obj),device=hex(device),device_memory=memory(device),
                         type_evidence='SDK ToString +0x1eb loads channel+0x30 then virtual XDevice::GetName')
        if os.path.exists(os.environ['MIGRATION_EXIT_FINISHED_MARKER']):
            for breakpoint in delete_breakpoints:
                breakpoint.condition=' || '.join('$rdi == '+hex(address) for address in tracked_objects) or '0'
                breakpoint.enabled=True
        active[key]={'function':self.function,'object':hex(obj),'thread_ptid':thread.ptid}
        emit('function_enter',function=self.function,object=hex(obj),memory=memory(obj),stack=frames(8))
        if 'ToString' in self.function or 'XChannelTcpImpl::Close' in self.function:
            emit('channel_registers',object=hex(obj),registers=gdb.execute('info registers',to_string=True))
        try:Return(key,self.function,obj)
        except gdb.error as error:emit('finish_breakpoint_unavailable',function=self.function,error=str(error))
        return False

class Delete(gdb.Breakpoint):
    def __init__(self,function):
        super().__init__(function,internal=False);self.function=function;self.enabled=False
    def stop(self):
        pointer=int(gdb.parse_and_eval('$rdi'))
        emit('operator_delete_enter',function=self.function,object=hex(pointer),memory=memory(pointer),stack=frames(15))
        try:Return((gdb.selected_thread().ptid,self.function,pointer),self.function,pointer)
        except gdb.error:pass
        return False

def thread_snapshot(event):
    selected=gdb.selected_thread();rows=[]
    for thread in gdb.selected_inferior().threads():
        try:
            thread.switch();stack=frames()
            text=' '.join(row['function'] or '' for row in stack)
            if any(name in text for name in ('barex','asio','blade_llm')):
                rows.append({'ptid':thread.ptid,'name':thread.name,'frames':stack})
                thread_roles[thread.ptid]=text
        except gdb.error as error:rows.append({'ptid':thread.ptid,'error':str(error)})
    if selected:selected.switch()
    emit(event,threads=rows,active_calls=list(active.values()))

def stopped(event):
    if isinstance(event,gdb.SignalEvent):
        emit('signal',signal=event.stop_signal,stack=frames(),instruction=gdb.execute('x/12i $pc-16',to_string=True),
             registers=gdb.execute('info registers',to_string=True))
        thread_snapshot('fault_native_threads')

gdb.events.stop.connect(stopped)
gdb.events.exited.connect(lambda event:emit('inferior_exit',exit_code=getattr(event,'exit_code',None)))
if hasattr(gdb.events,'thread_exited'):
    gdb.events.thread_exited.connect(lambda event:emit('thread_exited',ptid=event.inferior_thread.ptid,
        prior_role=thread_roles.get(event.inferior_thread.ptid)))
gdb.execute('set breakpoint pending on')
gdb.execute('set pagination off')
gdb.execute('handle SIGTERM nostop noprint pass')
gdb.execute('handle SIGSEGV stop print pass')
gdb.execute('handle SIGABRT stop print pass')
functions=[
    'blade_llm::BarexCtx::~BarexCtx()',
    'blade_llm::TCPServer::CtxCallback::OnRecvCall',
    'accl::barex::XChannelTcpImpl::XChannelTcpImpl',
    'accl::barex::XChannelTcpImpl::Close()',
    'accl::barex::XChannelTcpImpl::ToString[abi:cxx11]()',
    'accl::barex::XChannelTcpImpl::~XChannelTcpImpl()',
    'accl::barex::XContextTcpImpl::Shutdown()',
    'accl::barex::XContextTcpImpl::WaitStop()',
    'accl::barex::XContextTcpImpl::~XContextTcpImpl()',
    'accl::barex::XContextCommonImpl::~XContextCommonImpl()',
    'accl::barex::TcpDeviceManagerImpl::~TcpDeviceManagerImpl()',
    'accl::barex::TcpDevice::~TcpDevice()',
    'accl::barex::XDevice::~XDevice()',
    'boost::asio::detail::scheduler::~scheduler()',
    'boost::asio::execution_context::~execution_context()',
    'blade_llm::XThreadpoolDeleter::operator()(accl::barex::XThreadpool*)',
    'blade_llm::XListenerDeleter::operator()(accl::barex::XListener*)',
    'std::thread::join()',
]
for function in functions:
    try:Entry(function)
    except gdb.error as error:emit('breakpoint_install_error',function=function,error=str(error))
for function in ('operator delete(void*)','operator delete(void*, unsigned long)'):
    try:delete_breakpoints.append(Delete(function))
    except gdb.error as error:emit('breakpoint_install_error',function=function,error=str(error))
thread_snapshot('attach_native_threads')
emit('debugger_ready',functions=functions)
