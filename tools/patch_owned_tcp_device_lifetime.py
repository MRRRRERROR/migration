"""Keep TCP network descriptors alive through channel/context/mempool cleanup."""
from pathlib import Path
import sys

folder=Path(sys.argv[1]);cpp=folder/'barex_protocol.cpp';header=folder/'barex_protocol.h'
c=cpp.read_text(encoding='utf-8');h=header.read_text(encoding='utf-8')
c=c.replace('    XDevice* lovely_nic = nullptr;', '    std::shared_ptr<XDevice> device_owner;\n    XDevice* lovely_nic = nullptr;',1)
c=c.replace('std::pair<XDevice*, XSimpleMempool*> get_gpu_ctx', 'std::tuple<XDevice*, XSimpleMempool*, std::shared_ptr<XDevice>> get_gpu_ctx',1)
c=c.replace('std::pair<XDevice*, XSimpleMempool*> MpManager::get_gpu_ctx', 'std::tuple<XDevice*, XSimpleMempool*, std::shared_ptr<XDevice>> MpManager::get_gpu_ctx',1)
c=c.replace('return {iter->second.lovely_nic, iter->second.mp.get()};', 'return {iter->second.lovely_nic, iter->second.mp.get(), iter->second.device_owner};')
c=c.replace('return {iter2->second.lovely_nic, iter2->second.mp.get()};', 'return {iter2->second.lovely_nic, iter2->second.mp.get(), iter2->second.device_owner};')
old='''    ctx.lovely_nic = all_nic_devs[0];'''
new='''    // SDK singleton device ownership ends before our module globals at exit.
    // Clone only the public TCP descriptor (same id/name/IP); never free an SDK
    // device. Context leases plus GPUCtx keep this descriptor alive until both
    // the context/channels and their memory pool have finished cleanup.
    auto* original = dynamic_cast<accl::barex::TcpDevice*>(all_nic_devs[0]);
    RTASSERT(original != nullptr && original->GetType() == accl::barex::XDT_TCP);
    ctx.device_owner = std::shared_ptr<XDevice>(
        new accl::barex::TcpDevice(original->GetId(), original->GetName(), original->GetIpAddress()),
        [](XDevice* owned) {
          ShutdownTrace("owned_tcp_device_delete_begin", owned, nullptr, nullptr, "delete_owned_descriptor", -1);
          delete owned;
          ShutdownTrace("owned_tcp_device_delete_end", nullptr, nullptr, nullptr, "delete_owned_descriptor", 0);
        });
    ctx.lovely_nic = ctx.device_owner.get();
    ShutdownTrace("owned_tcp_device_created", ctx.lovely_nic, original, nullptr, "clone_public_descriptor;SDK_original_not_owned", 0);
'''
assert old in c;c=c.replace(old,new,1)
c=c.replace('auto [nic_dev, mp] = g_mp_manager.get_gpu_ctx(ctx->device_id(), kind);',
    'auto [nic_dev, mp, device_owner] = g_mp_manager.get_gpu_ctx(ctx->device_id(), kind);\n  self.device_owner_ = std::move(device_owner);',1)
c=c.replace('auto [_, mp] = g_mp_manager.get_gpu_ctx(gpu_id, TransferProtocol::Kind::RDMA_DIRECT);',
    'auto [_, mp, device_owner] = g_mp_manager.get_gpu_ctx(gpu_id, TransferProtocol::Kind::RDMA_DIRECT);',1)
h=h.replace('  accl::barex::XSimpleMempool* mp_ = nullptr;',
    '  std::shared_ptr<accl::barex::XDevice> device_owner_; // outlives xctx_/channels and borrowed TCP descriptor\n  accl::barex::XSimpleMempool* mp_ = nullptr;',1)
assert 'RTASSERT(result == accl::barex::BAREX_SUCCESS)' in c
cpp.write_text(c,encoding='utf-8');header.write_text(h,encoding='utf-8')
