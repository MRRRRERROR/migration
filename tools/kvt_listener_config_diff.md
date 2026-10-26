# KVT Listener Configuration Diff

## Compared Results

- Successful formal run: `/data/home/zylu/llumnix-v018-main/results/5090_4inst_formal_20260721_014617`
- Failed load-sweep smoke: `/data/home/zylu/llumnix-v018-main/results/5090_load_sweep_20260721_191608`

## Configuration Comparison

| Item | Successful formal run | Failed migration smoke |
|---|---|---|
| Instances | 4 | 4 |
| Connector | HybridConnector | HybridConnector |
| Protocol/backend | vllm-kvt / blade_kvt TCP | vllm-kvt / blade_kvt TCP |
| `CUDA_VISIBLE_DEVICES` | `0,1,2,3` | `0,1,2,3` |
| `VLLM_HOST_IP` | Explicitly `127.0.0.1` | Unset; discovery fell back to `10.112.184.62` |
| Discovery entrypoint/KVT IP | `127.0.0.1` | `10.112.184.62` |
| KVT ports | `36218`, `36226`, `36234`, `36242` | `36218`, `36226`, `36234`, `36242` |
| KVT endpoint for instance 3 | `127.0.0.1:36242` | Advertised as `10.112.184.62:36242` |
| Listener readiness wait | Effective loopback endpoint configuration; successful KVT transfer recorded | API health plus fixed sleep only; no KVT endpoint readiness gate |

## Required Findings

1. The successful formal experiment used KVT listeners at `127.0.0.1:36218`, `127.0.0.1:36226`, `127.0.0.1:36234`, and `127.0.0.1:36242`.
2. The failed smoke source attempted to connect to `10.112.184.62:36242`.
3. The successful run set `VLLM_HOST_IP=127.0.0.1`; the failed smoke left it unset and advertised `10.112.184.62`.
4. There is a confirmed `127.0.0.1` versus `10.112.184.62` mismatch. Barex selected device `lo` and physically listened on loopback while worker metadata advertised the host address.
5. Benchmark startup was not the primary failure. In the failed smoke, the KVT server initialization preceded the migration attempt by about 20 seconds. A separate no-request runtime probe after all API endpoints were ready still found `127.0.0.1:36242` connectable and `10.112.184.62:36242` refused.

## Classification

**B. Listener started, but the bind address and connect address did not match.**

The experiment runner must use one single-node strategy consistently: `VLLM_HOST_IP=127.0.0.1`, loopback discovery endpoints, and loopback KVT readiness checks.

## Evidence Sources

- `5090_4inst_formal_20260721_014617/ENV_AND_PATCH_SUMMARY.md`
- Formal `migration_run*.console.log`
- Failed smoke `01_smoke/smoke_migration/vllm_3.log`
- Runtime probe `01_smoke/kvt_listener_runtime_check/vllm_3.log`
- `04_step_summaries/kvt_listener_runtime_check.md`
