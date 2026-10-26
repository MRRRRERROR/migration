"""Candidate normal-shutdown entry for native globals; retains all assertions."""
from pathlib import Path
import sys

folder=Path(sys.argv[1])
binding=folder/'kvtransfer_pybind.cpp'
text=binding.read_text(encoding='utf-8')
text=text.replace('#include "server.h"','#include "server.h"\n#include "protocol/barex_protocol.h"',1)
position=text.index('const std::vector<TransferProtocol> &support_transfer_protocols()')
text=text[:position]+'''// Called only after the serving controller and connector have drained requests.
// Release globals before interpreter/CUDA/library static teardown. The server
// owns a listener and must end it while its Context and callback are still live.
void shutdown_kv_transfer() {
  ShutdownTrace("native_shutdown_begin", KV_SERVER, nullptr, nullptr, "ExplicitShutdown", -1);
  if (KV_CLIENT) {
    ShutdownTrace("native_client_stop_begin", KV_CLIENT.get(), nullptr, nullptr, "reset", -1);
    KV_CLIENT.reset();
    ShutdownTrace("native_client_stop_end", nullptr, nullptr, nullptr, "reset", 0);
  }
  if (KV_SERVER) {
    auto* server = KV_SERVER;
    ShutdownTrace("native_server_stop_begin", server, nullptr, nullptr, "delete_owned_server_listener", -1);
    delete server;
    KV_SERVER = nullptr;
    ShutdownTrace("native_server_stop_end", server, nullptr, nullptr, "delete_owned_server_listener", 0);
  }
  KV_SERVER_CTX.reset();
  ShutdownTrace("native_shutdown_end", nullptr, nullptr, nullptr, "ExplicitShutdown", 0);
}

'''+text[position:]
text=text.replace('  // common\n','  // common\n  m.def("shutdown_kv_transfer", &blade_llm::shutdown_kv_transfer, "Drain completed native connector resources before interpreter teardown;");\n',1)
binding.write_text(text,encoding='utf-8')
cpp=folder/'barex_protocol.cpp'
text=cpp.read_text(encoding='utf-8')
for cls,label in (('XThreadpool','threadpool'),('XListener','listener'),('XConnector','connector')):
    old=f'''void {cls}Deleter::operator()(accl::barex::{cls} *tp) {{
  tp->Shutdown();
  tp->WaitStop();
  delete tp;
}}'''
    new=f'''void {cls}Deleter::operator()(accl::barex::{cls} *tp) {{
  ShutdownTrace("{label}_shutdown_begin", tp, nullptr, nullptr, "Shutdown", -1);
  auto shutdown_rc = tp->Shutdown();
  auto wait_rc = tp->WaitStop();
  ShutdownTrace("{label}_shutdown_result", tp, nullptr, nullptr, "Shutdown", (int)shutdown_rc);
  ShutdownTrace("{label}_wait_result", tp, nullptr, nullptr, "WaitStop", (int)wait_rc);
  RTASSERT(shutdown_rc == accl::barex::BAREX_SUCCESS);
  RTASSERT(wait_rc == accl::barex::BAREX_SUCCESS);
  delete tp;
}}'''
    assert old in text
    text=text.replace(old,new,1)
cpp.write_text(text,encoding='utf-8')
module=folder/'hybrid_modules.py'
text=module.read_text(encoding='utf-8')
old='''            stop_asyncio_thread(side.loop)
        self._shutdown_complete = True'''
new='''            stop_asyncio_thread(side.loop)
        if self._worker is not None:
            from blade_kvt import kvtransfer_ops
            kvtransfer_ops.shutdown_kv_transfer()
        self._shutdown_complete = True'''
assert old in text
module.write_text(text.replace(old,new,1),encoding='utf-8')
compile(module.read_text(encoding='utf-8'),str(module),'exec')
