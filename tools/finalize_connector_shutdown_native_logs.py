"""Capture real shutdown return codes and check them; never print assumed rc."""
from pathlib import Path
import sys
folder=Path(sys.argv[1]);cpp=folder/'barex_protocol.cpp';header=folder/'barex_protocol.h'
text=cpp.read_text(encoding='utf-8');h=header.read_text(encoding='utf-8')
for variable,event,pool in [('ctx','context_delete_end','nullptr'),('mempool','pool_delete_end','mempool')]:
    old=f'  {variable}->Shutdown();\n  {variable}->WaitStop();\n  ShutdownTrace("{event}", {variable}, {pool}, nullptr, "WaitStop", 0);'
    new=f'  auto shutdown_rc = {variable}->Shutdown();\n  auto wait_rc = {variable}->WaitStop();\n  ShutdownTrace("{event}", {variable}, {pool}, nullptr, "WaitStop", (int)wait_rc);\n  RTASSERT(shutdown_rc == accl::barex::BAREX_SUCCESS);\n  RTASSERT(wait_rc == accl::barex::BAREX_SUCCESS);'
    assert old in text;text=text.replace(old,new,1)
text=text.replace('  ShutdownTrace("barex_context_shutdown_result", this, mp_, nullptr, "Shutdown", (int)shutdown_rc);',
    '  ShutdownTrace("barex_context_shutdown_result", this, mp_, nullptr, "Shutdown", (int)shutdown_rc);\n  RTASSERT(shutdown_rc == accl::barex::BAREX_SUCCESS);\n  RTASSERT(wait_rc == accl::barex::BAREX_SUCCESS);',1)
h=h.replace('r ? (d ? "owned_alloc_registered" : "release_only") : "registered_user"',
    'borrowed ? "borrowed_pytorch_tensor" : (r ? (d ? "owned_alloc_registered" : "release_only") : "registered_user")',1)
cpp.write_text(text,encoding='utf-8');header.write_text(h,encoding='utf-8')
