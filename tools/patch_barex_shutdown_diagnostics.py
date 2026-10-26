"""Add diagnostics only. Keep all operations, their return checks and assertions."""
from pathlib import Path
import sys

folder=Path(sys.argv[1]);cpp=folder/'barex_protocol.cpp';header=folder/'barex_protocol.h'
text=cpp.read_text();h=header.read_text()
text=text.replace('#include <sys/epoll.h>', '#include <sys/epoll.h>\n#include <dlfcn.h>\n#include <sys/syscall.h>\n#include <time.h>\n#include <cstdio>')
marker='void XContextDeleter::operator()'
position=text.index(marker)
helper=r'''
// No CUDA runtime calls: driver queries do not create a context during teardown.
void ShutdownTrace(const char* event, const void* object, const void* pool,
                   const memp_t* mr, const char* operation, int result) {
  timespec mono{}, wall{}; clock_gettime(CLOCK_MONOTONIC, &mono);
  clock_gettime(CLOCK_REALTIME, &wall);
  using Current = int (*)(void**);
  using Primary = int (*)(int, unsigned*, int*);
  auto current = reinterpret_cast<Current>(dlsym(RTLD_DEFAULT, "cuCtxGetCurrent"));
  auto primary = reinterpret_cast<Primary>(dlsym(RTLD_DEFAULT, "cuDevicePrimaryCtxGetState"));
  void* ctx = nullptr; unsigned flags = 0; int active = -1;
  int crc = current ? current(&ctx) : -1;
  int prc = (primary && mr) ? primary(mr->device_id, &flags, &active) : -1;
  fprintf(stderr, "KVT_SHUTDOWN event=%s pid=%d tid=%ld mono_ns=%lld wall_ns=%lld object=%p pool=%p buf=%p base=%p len=%llu dtype=%d device=%d native_mr=%p released_flag=%p allocator=%p adopted=%d operation=%s result=%d cuda_query_rc=%d cuda_context=%p primary_query_rc=%d primary_active=%d\n",
    event, getpid(), syscall(SYS_gettid),
    (long long)mono.tv_sec*1000000000+mono.tv_nsec,
    (long long)wall.tv_sec*1000000000+wall.tv_nsec, object, pool,
    mr ? mr->buf : nullptr, mr ? mr->base : nullptr,
    mr ? (unsigned long long)mr->buf_len : 0,
    mr ? (int)mr->d_type : -1, mr ? mr->device_id : -1,
    mr ? mr->mr : nullptr, mr ? mr->released : nullptr,
    mr ? mr->allocator.get() : nullptr, mr ? (int)mr->adopted : -1,
    operation, result, crc, ctx, prc, active);
  fflush(stderr);
}

'''
text=text[:position]+helper+text[position:]
text=text.replace('  ctx->Shutdown();\n  ctx->WaitStop();\n  delete ctx;',
    '  ShutdownTrace("context_delete_begin", ctx, nullptr, nullptr, "Shutdown", -1);\n  ctx->Shutdown();\n  ctx->WaitStop();\n  ShutdownTrace("context_delete_end", ctx, nullptr, nullptr, "WaitStop", 0);\n  delete ctx;',1)
text=text.replace('  mempool->Shutdown();\n  mempool->WaitStop();\n  delete mempool;',
    '  ShutdownTrace("pool_delete_begin", mempool, mempool, nullptr, "Shutdown", -1);\n  mempool->Shutdown();\n  mempool->WaitStop();\n  ShutdownTrace("pool_delete_end", mempool, mempool, nullptr, "WaitStop", 0);\n  delete mempool;',1)
text=text.replace('  BarexResult result;\n  if (release_ && dereg_)',
    '  const char* operation = release_ ? (dereg_ ? "ReleaseAndDeregBuffer" : "ReleaseBuffer") : "DeregUserMr";\n  ShutdownTrace("guard_release_begin", this, self.mp_, &self.mr_, operation, -1);\n  BarexResult result;\n  if (release_ && dereg_)',1)
text=text.replace('  RTASSERT(result == accl::barex::BAREX_SUCCESS);\n}\n\n// GPU 0',
    '  ShutdownTrace("guard_release_result", this, self.mp_, &self.mr_, operation, (int)result);\n  RTASSERT(result == accl::barex::BAREX_SUCCESS);\n}\n\n// GPU 0',1)
text=text.replace('BarexCtx::~BarexCtx() {\n  this->xctx_->Shutdown();\n  this->xctx_->WaitStop();\n}',
    'BarexCtx::~BarexCtx() {\n  ShutdownTrace("barex_context_shutdown_begin", this, mp_, nullptr, "Shutdown", -1);\n  auto shutdown_rc = this->xctx_->Shutdown();\n  auto wait_rc = this->xctx_->WaitStop();\n  ShutdownTrace("barex_context_wait_stopped", this, mp_, nullptr, "WaitStop", (int)wait_rc);\n  ShutdownTrace("barex_context_shutdown_result", this, mp_, nullptr, "Shutdown", (int)shutdown_rc);\n}')
text=text.replace('        out.d_type = GPU;', '        out.device_id = ctx->device_id();\n        out.d_type = GPU;',1)
text=text.replace('        layer_mrs.emplace_back(BarexMRGuard::ReleaseGuard(std::move(out), self.mp()));',
    '        ShutdownTrace("tcp_layer_tensor_alias", ctx, self.mp(), &out, "NoAllocNoRegistration", 0);\n        layer_mrs.emplace_back(BarexMRGuard::ReleaseGuard(std::move(out), self.mp()));',1)
h=h.replace('class BarexMRGuard : public noncopyable {', 'void ShutdownTrace(const char*, const void*, const void*, const accl::barex::memp_t*, const char*, int);\n\nclass BarexMRGuard : public noncopyable {',1)
h=h.replace('      dereg_(d) {}', '      dereg_(d) { ShutdownTrace("guard_construct", this, mp_, &mr_, r ? (d ? "owned_alloc_registered" : "release_only") : "registered_user", 0); }',1)
h=h.replace('    other.mp_ = nullptr;', '    ShutdownTrace("guard_move", this, mp_, &mr_, "move_from", 0);\n    other.mp_ = nullptr;',1)
assert 'guard_release_result' in text and 'barex_context_wait_stopped' in text
cpp.write_text(text);header.write_text(h)
