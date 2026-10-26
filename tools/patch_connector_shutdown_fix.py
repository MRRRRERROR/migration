"""Ownership repair and awaitable connector thread shutdown, retaining checks."""
from pathlib import Path
import sys
folder=Path(sys.argv[1]);cpp=folder/'barex_protocol.cpp';header=folder/'barex_protocol.h'
c=cpp.read_text(encoding='utf-8');h=header.read_text(encoding='utf-8')
h=h.replace('  bool const dereg_; // TCP hostbuffers need release but not deregistration',
    '  bool const dereg_; // TCP hostbuffers need release but not deregistration\n  bool const borrowed_; // tensor storage belongs to PyTorch, not the Barex pool')
h=h.replace('bool r, bool d) noexcept:', 'bool r, bool d, bool borrowed = false) noexcept:',1)
h=h.replace('      dereg_(d) { ShutdownTrace', '      dereg_(d), borrowed_(borrowed) { ShutdownTrace',1)
h=h.replace(' public:\n  static BarexMRGuard RelDeregGuard',
    ' public:\n  static BarexMRGuard BorrowedTensorView(accl::barex::memp_t &&mr, accl::barex::XSimpleMempool *mp) noexcept {\n    return BarexMRGuard(std::move(mr), mp, false, false, true);\n  }\n  static BarexMRGuard RelDeregGuard',1)
h=h.replace('      dereg_(other.dereg_) {','      dereg_(other.dereg_), borrowed_(other.borrowed_) {',1)
c=c.replace('  BarexResult result;\n  if (release_ && dereg_)', '  BarexResult result;\n  if (release_ && dereg_)',1)
needle='  const char* operation = release_ ?'
pos=c.index(needle,c.index('BarexMRGuard::~BarexMRGuard'))
c=c[:pos]+'''  if (borrowed_) {
    // A borrowed descriptor never allocated or registered storage in this pool.
    // Validate its category; owned/registered guards below still check every rc.
    RTASSERT(!release_ && !dereg_);
    RTASSERT(self.mr_.allocator == nullptr && self.mr_.released == nullptr);
    RTASSERT(self.mr_.mr == nullptr && !self.mr_.adopted);
    ShutdownTrace("borrowed_tensor_view_end", this, self.mp_, &self.mr_, "owner=PyTorch;NoBarexRelease", 0);
    return;
  }
'''+c[pos:]
c=c.replace('layer_mrs.emplace_back(BarexMRGuard::ReleaseGuard(std::move(out), self.mp()));',
            'layer_mrs.emplace_back(BarexMRGuard::BorrowedTensorView(std::move(out), self.mp()));',1)
assert 'RTASSERT(result == accl::barex::BAREX_SUCCESS)' in c and 'BorrowedTensorView' in c
cpp.write_text(c,encoding='utf-8');header.write_text(h,encoding='utf-8')

utils=folder/'utils.py';u=utils.read_text(encoding='utf-8')
u=u.replace('def _asyncio_loop_main(loop, vllm_config, local_rank):','_loop_threads = {}\n_rpc_servers = []\n_conn_pools = []\n\ndef _asyncio_loop_main(loop, vllm_config, local_rank):',1)
u=u.replace('    threading.Thread(\n        target=_asyncio_loop_main,','    thread = threading.Thread(\n        target=_asyncio_loop_main,',1)
u=u.replace('        daemon=True,\n    ).start()\n    return loop','        daemon=True,\n    )\n    _loop_threads[loop] = thread\n    thread.start()\n    return loop',1)
position=u.index('# A simple RPC implementation')
u=u[:position]+'''def stop_asyncio_thread(loop, timeout=30):
    """Called after connector request/native work is drained, never at atexit."""
    thread = _loop_threads.get(loop)
    if thread is None or not thread.is_alive():
        return
    if thread is threading.current_thread():
        raise RuntimeError("Cannot join connector loop from its own thread")
    async def drain():
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current]
        logger.info("KVT_THREAD_SHUTDOWN begin name=%s tasks=%d", thread.name, len(pending))
        for task in pending:
            task.cancel()
        outcomes = await asyncio.gather(*pending, return_exceptions=True)
        errors = [outcome for outcome in outcomes if isinstance(outcome, BaseException)
                  and not isinstance(outcome, asyncio.CancelledError)]
        if errors:
            raise RuntimeError("Connector task shutdown failed: " + repr(errors))
        for server in _rpc_servers:
            if getattr(server, "_loop", None) is not loop:
                continue
            listener = getattr(server, "_server", None)
            if listener:
                listener.close()
                await listener.wait_closed()
            for writer in list(server._writers):
                writer.close()
                await writer.wait_closed()
            server._writers.clear()
        for pool in _conn_pools:
            for writer in list(pool._writers):
                if writer._loop is loop:
                    writer.close()
                    await writer.wait_closed()
                    pool._writers.discard(writer)
        await loop.shutdown_asyncgens()
        await loop.shutdown_default_executor()
        logger.info("KVT_THREAD_SHUTDOWN drained name=%s tasks=%d", thread.name, len(asyncio.all_tasks())-1)
    future = asyncio.run_coroutine_threadsafe(drain(), loop)
    future.result(timeout=timeout)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout)
    if thread.is_alive():
        raise RuntimeError("Connector loop did not exit: " + thread.name)
    loop.close()
    _loop_threads.pop(loop, None)
    logger.info("KVT_THREAD_SHUTDOWN joined name=%s", thread.name)


'''+u[position:]
u=u.replace('        self._methods: dict[int, _RpcMethodType] = {}',
    '        self._methods: dict[int, _RpcMethodType] = {}\n        self._writers = set()\n        _rpc_servers.append(self)',1)
u=u.replace('    def start(self, loop):\n        coro', '    def start(self, loop):\n        self._loop = loop\n        coro',1)
u=u.replace('        # Create multiple connections for concurrency,','        self._writers.add(writer)\n        # Create multiple connections for concurrency,',1)
u=u.replace('        server = await asyncio.start_server(self._client_main, "0.0.0.0", p)',
    '        server = await asyncio.start_server(self._client_main, "0.0.0.0", p)\n        self._server = server',1)
u=u.replace('        self._conns: list[Conn] = []','        self._conns: list[Conn] = []\n        self._writers = set()\n        _conn_pools.append(self)',1)
u=u.replace('        return await asyncio.open_connection(self._host, self._port)',
    '        conn = await asyncio.open_connection(self._host, self._port)\n        self._writers.add(conn[1])\n        return conn',1)
utils.write_text(u,encoding='utf-8')
m=folder/'hybrid_modules.py';text=m.read_text(encoding='utf-8')
pos=text.index('    ############################################################',text.index('class HybridConnector('))
text=text[:pos]+'''    def shutdown(self):
        if getattr(self, "_shutdown_complete", False):
            return
        from .utils import stop_asyncio_thread
        side = self._worker if self._worker is not None else self._sched
        if side is not None:
            if getattr(side, "_saving", None) or getattr(side, "_loading", None):
                raise RuntimeError("Cannot close connector with active KV operations")
            stop_asyncio_thread(side.loop)
        self._shutdown_complete = True

'''+text[pos:]
m.write_text(text,encoding='utf-8')
for p in (utils,m):compile(p.read_text(encoding='utf-8'),str(p),'exec')
