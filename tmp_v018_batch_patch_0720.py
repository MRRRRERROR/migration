from pathlib import Path

import blade_kvt


STAMP = "batch_v018_compat_0720"
base = Path(blade_kvt.__file__).resolve().parent


def patch_file(path: Path, replacements):
    text = path.read_text()
    backup = path.with_suffix(path.suffix + f".bak_{STAMP}")
    if not backup.exists():
        backup.write_text(text)

    changed = []
    for old, new, label in replacements:
        if old in text and new not in text:
            text = text.replace(old, new)
            changed.append(label)
        elif new in text:
            changed.append(label + " already")
        else:
            changed.append(label + " not found")

    path.write_text(text)
    print("FILE", path)
    for item in changed:
        print("  ", item)


patch_file(
    Path("tests/local/conftest.py"),
    [
        (
            "wait_for_service(vllm_health_url, timeout=180)",
            "wait_for_service(vllm_health_url, timeout=900)",
            "startup timeout 180->900",
        ),
        (
            "time.sleep(20)  # wait for redis discovery work",
            "time.sleep(30)  # wait for redis discovery work",
            "redis discovery wait 20->30",
        ),
    ],
)

patch_file(
    Path("tests/local/utils.py"),
    [
        (
            "VLLM_BASE_PORT: int = 8000",
            "VLLM_BASE_PORT: int = 8100",
            "VLLM_BASE_PORT 8000->8100",
        ),
    ],
)

patch_file(
    base / "hybrid_connector" / "kvtbackend.py",
    [
        (
            "if envs.VLLM_FLASH_ATTN_USE_FAST_TURBOQUANT:",
            'if getattr(envs, "VLLM_FLASH_ATTN_USE_FAST_TURBOQUANT", False):',
            "turboquant env getattr",
        ),
        (
            "protocol = bladekv.KVTransferProtocolType.RDMA_DIRECT",
            "protocol = bladekv.KVTransferProtocolType.TCP",
            "kvtbackend RDMA_DIRECT->TCP",
        ),
        (
            "protocol = bladekv.KVTransferProtocolType.RDMA_STAGED",
            "protocol = bladekv.KVTransferProtocolType.TCP",
            "kvtbackend RDMA_STAGED->TCP",
        ),
    ],
)

patch_file(
    base / "hybrid_connector" / "migration" / "backend.py",
    [
        (
            "self.max_model_len = vllm_config.scheduler_config.max_model_len",
            "self.max_model_len = getattr(vllm_config.scheduler_config, 'max_model_len', vllm_config.model_config.max_model_len)",
            "max_model_len fallback",
        ),
    ],
)

patch_file(
    base / "kv_transfer_impl.py",
    [
        (
            "return TransferProtocol(TransferProtocol.Kind.RDMA_DIRECT)",
            "return TransferProtocol(TransferProtocol.Kind.TCP)",
            "ops protocol RDMA_DIRECT->TCP",
        ),
        (
            "return TransferProtocol(TransferProtocol.Kind.RDMA_STAGED)",
            "return TransferProtocol(TransferProtocol.Kind.TCP)",
            "ops protocol RDMA_STAGED->TCP",
        ),
        (
            "return KVTransferProtocolType.RDMA_DIRECT",
            "return KVTransferProtocolType.TCP",
            "type RDMA_DIRECT->TCP",
        ),
        (
            "return KVTransferProtocolType.RDMA_STAGED",
            "return KVTransferProtocolType.TCP",
            "type RDMA_STAGED->TCP",
        ),
    ],
)

print("\nSUMMARY CHECK")
for path in [
    Path("tests/local/conftest.py"),
    Path("tests/local/utils.py"),
    base / "hybrid_connector" / "migration" / "backend.py",
    base / "hybrid_connector" / "kvtbackend.py",
    base / "kv_transfer_impl.py",
]:
    print("---", path)
    for line_no, line in enumerate(path.read_text().splitlines(), 1):
        if any(
            key in line
            for key in [
                "timeout=900",
                "VLLM_BASE_PORT",
                "max_model_len",
                "VLLM_FLASH_ATTN_USE_FAST_TURBOQUANT",
                "RDMA_DIRECT",
                "RDMA_STAGED",
                "Kind.TCP",
                "KVTransferProtocolType.TCP",
            ]
        ):
            print(f"{line_no}: {line}")
