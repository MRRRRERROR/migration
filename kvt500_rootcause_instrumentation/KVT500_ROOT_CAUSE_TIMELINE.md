# KVT500 Root Cause Timeline

本轮未复现 KVT500。以下为两个停滞 attempt 的真实时间线。

## `cmpl-config_e_0013-0#mig1`

```text
PREALLOC_ACK code=0
    ↓
SOURCE_TRANSFER_SUBMIT
    ↓
SCHED_GET_RAW_BLOCKS       [65]
    ↓
SCHED_GET_ADAPTED_BLOCKS   [65]
    ↓
AFTER_SOURCE_BLOCK_LOOKUP  [65]
    ↓
MIGRATION_BYPASS_BLOCKS    [65]
    ↓
PREQMETA_BLOCKS            [65]
    ↓
no SEND_BATCH / no PYTHON_SEND_BOUNDARY / no KVTResp
    ↓
about two hours without progress
    ↓
diagnostic termination cleanup: FINISH -> FREE -> REMOVE
```

## `cmpl-config_e_0034-0#mig1`

```text
PREALLOC_ACK code=0
    ↓
SOURCE_TRANSFER_SUBMIT
    ↓
SCHED_GET_RAW_BLOCKS       [61]
    ↓
SCHED_GET_ADAPTED_BLOCKS   [61]
    ↓
AFTER_SOURCE_BLOCK_LOOKUP  [61]
    ↓
MIGRATION_BYPASS_BLOCKS    [61]
    ↓
PREQMETA_BLOCKS            [61]
    ↓
no SEND_BATCH / no PYTHON_SEND_BOUNDARY / no KVTResp
    ↓
no further progress until diagnostic termination
```
