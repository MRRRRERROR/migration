# KVT500 事件时间线

## 口径

- 原始证据来自 `KVT500_CONTEXT_FULL.md`、诊断包内 `migration_efficiency_events.jsonl` 与 `vllm_0.log` / `vllm_2.log` / `vllm_3.log`。
- `stage_copy_begin.block_count` 来自 destination migration backend 的 `blockids`（`patch_phase2_live_events_0721.py` 中记录 `sum(len(x) for x in blockids)`），不是 source block table 的观测值。
- `tx_stub.cpp:222 FinishedReq` 是 KVT 发送任务完成通知；失败时 `State=2`。它不是 vLLM 用户请求完成事件。
- 日志未打印 `finish_request`、free 或 scheduler remove 的 request 级事件；这些列只能标记为“未见”，不能标记为“不可能发生”。

## cmpl-config_e_0082-0

| timestamp (+08:00) | component / source | event and fields | status / block evidence |
|---|---|---|---|
| 19:01:06.640708 | `kvt_migration_frontend.py:363,117`; `migration_efficiency_events.jsonl:598` | LCR selected；`migration_start` / `migration_initiated`，src `('127.0.0.1',31103)`，dst `10.112.184.62:31102` | running snapshot: prompt=144, generated=20 |
| 19:01:06.642099 | `migration/backend.py`; jsonl:599 | `prealloc_begin`, prealloc=20 | destination operation |
| 19:01:06.642502 | `migration/backend.py`; jsonl:600 | `prealloc_ack`, code=0 | destination allocation succeeded；不证明 source blocks 非空 |
| 19:01:06.645785 | `migration/backend.py`; jsonl:601 | `stage_copy_begin`, cached=0, max=164, block_count=11 | 11 是 destination blocks |
| 19:01:06.647060 | jsonl:602 | `final_copy_begin` | `kvt_suspend` stage 0 |
| 19:01:06.649877 | `client.cpp:288` | `create_step_tasks`, freeze=1, seen_tokens=0, new_tokens=164 | source send task 已创建 |
| 19:01:06.650103 | `tx_stub.cpp:199` | `KVT tx_stub fail to send data`; `vector::_M_range_check: __n (0) >= size (0)` | source-side send consumes an empty vector；首个可见 empty |
| 19:01:06.650145 | `tx_stub.cpp:222` | State=2, `FinishedReq=...0082-0` | KVT task failed after empty-vector error |
| 19:01:06.652471 | destination `migration/backend.py`; jsonl:603 | `KVTResp(code=500,cached=164,computed=0)` / `stage_copy_end` | migration copy failed |
| 19:01:06.653190 | jsonl:605 | `request_finished`, generated_len=0, dst set | target-side failed migrated copy cleanup |
| 19:01:07.639 | `kvt_migration_frontend.py:363` | source snapshot still has generated=50 | source request continued |
| 19:01:07.862185 | jsonl:649 | `request_finished`, generated_len=59, total_context_len=203 | original request final success |

## cmpl-config_e_0203-0

| timestamp (+08:00) | component / source | event and fields | status / block evidence |
|---|---|---|---|
| 19:02:20.899486 | `kvt_migration_frontend.py:363,117`; jsonl:2453 | selected；`migration_initiated`, src 31103 → dst 31100 | prompt=71, generated=3 |
| 19:02:20.900140 / .900747 | jsonl:2454-2455 | `prealloc_begin` / `prealloc_ack code=0`, prealloc=3 | destination success |
| 19:02:20.911093 | jsonl:2456 | `stage_copy_begin`, cached=16, max=74, block_count=5 | destination block count=5 |
| 19:02:20.911551 | jsonl:2457 | `final_copy_begin` | no source block IDs logged |
| 19:02:20.928947 | `client.cpp:288` | `create_step_tasks`, freeze=1, seen=16, new=58 | source task |
| 19:02:20.929096 | `tx_stub.cpp:199` | range check `__n=1`, vector `size=0` | first empty |
| 19:02:20.929124 | `tx_stub.cpp:222` | State=2, `FinishedReq` | follows error |
| 19:02:20.931545 | jsonl:2458 | `KVTResp(code=500,cached=74,computed=0)` | migration failed |
| 19:02:21.230254 | `client.cpp:366` | later `submit_req_send`, seen=16,new=58,has_last_token=1 | later metadata path retried/advanced; not a successful migration commit |
| 19:02:22.140–19:02:26.558 | `kvt_migration_frontend.py:363` | source generated 50→299 | source continued |
| 19:02:26.868274 | jsonl:2652 | final generated_len=309 | success |

## cmpl-config_e_0240-0

| timestamp (+08:00) | component / source | event and fields | status / block evidence |
|---|---|---|---|
| 19:02:41.640164 | frontend / jsonl:3046 | selected；src 31103 → dst 31102 | prompt=76, generated=200 |
| 19:02:41.641610 / .642013 | jsonl:3047-3048 | prealloc begin / ack code=0, prealloc=200 | destination success |
| 19:02:41.644981 | jsonl:3049 | `stage_copy_begin`, cached=16,max=276,block_count=18 | destination blocks=18 |
| 19:02:41.660923 | `client.cpp:288` | create task, seen=16,new=260 | source send task |
| 19:02:41.661120 | `tx_stub.cpp:199` | `__n=1 >= size=0` | first empty |
| 19:02:41.661180 | `tx_stub.cpp:222` | State=2, FinishedReq | follows error |
| 19:02:41.662588 | jsonl:3051 | KVTResp 500 | migration failed |
| 19:02:42.141–19:02:45.140 | frontend | source generated 216→409 | source continued |
| 19:02:45.915264 | jsonl:3207 | final generated_len=456 | success |

## cmpl-config_e_0328-0

| timestamp (+08:00) | component / source | event and fields | status / block evidence |
|---|---|---|---|
| 19:03:44.139835 | frontend / jsonl:4661 | selected；src 31103 → dst 31102 | prompt=93, generated=125 |
| 19:03:44.140796 / .141165 | jsonl:4662-4663 | prealloc begin / ack code=0, prealloc=125 | destination success |
| 19:03:44.143761 | jsonl:4664 | `stage_copy_begin`, cached=16,max=218,block_count=14 | destination blocks=14 |
| 19:03:44.158779 | `client.cpp:288` | create task, seen=16,new=202 | source send task |
| 19:03:44.159018 | `tx_stub.cpp:199` | `__n=1 >= size=0` | first empty |
| 19:03:44.159075 | `tx_stub.cpp:222` | State=2, FinishedReq | follows error |
| 19:03:44.162871 | jsonl:4666 | KVTResp 500 | migration failed |
| 19:03:44.641–19:03:47.140 | frontend | source generated 137→281 | source continued |
| 19:03:47.900125 | jsonl:4780 | final generated_len=329 | success |

## cmpl-config_e_0365-0

| timestamp (+08:00) | component / source | event and fields | status / block evidence |
|---|---|---|---|
| 19:04:07.141493 | frontend / jsonl:5225 | selected；src 31103 → dst 31100 | prompt=109, generated=2 |
| 19:04:07.142606 / .143527 | jsonl:5226-5227 | prealloc begin / ack code=0, prealloc=2 | destination success |
| 19:04:07.147877 | jsonl:5228 | stage begin, cached=16,max=111,block_count=7 | destination blocks=7 |
| 19:04:07.165665 | `client.cpp:288` | create task, seen=16,new=95 | source task |
| 19:04:07.165942 | `tx_stub.cpp:199` | `__n=1 >= size=0` | first empty |
| 19:04:07.165988 | `tx_stub.cpp:222` | State=2, FinishedReq | follows error |
| 19:04:07.167812 | jsonl:5230 | KVTResp 500 | migration failed |
| 19:04:08.184363 | `client.cpp:366` | later `submit_req_send`, seen=16,new=95,last=1 | late/stale send path |
| 19:04:10.640–19:04:15.639 | frontend | source generated 117→385 | source continued |
| 19:04:17.341305 | jsonl:5493 | final generated_len=477 | success |

## 共同顺序

五次均为：source running snapshot 被选择 → destination prealloc code=0 → destination block_count 非零 → source `create_step_tasks(freeze=1)` → `tx_stub.cpp:199` 对 size=0 的 vector 做 `.at(seen_tokens/16)` → `FinishedReq State=2` → KVTResp 500。empty 前没有 request 级 FinishedReq、finish_request、free 或 scheduler remove 证据。
