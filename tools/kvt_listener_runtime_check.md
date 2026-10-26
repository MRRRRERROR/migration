# KVT Listener Runtime Check

## Probe Conditions

- Services started without benchmark traffic.
- `VLLM_HOST_IP` was intentionally left unset to reproduce the failed smoke configuration.
- All four vLLM API health endpoints became ready before inspection.

## Runtime Results

| Check | Result |
|---|---|
| `VLLM_HOST_IP` | `UNSET` |
| Instance KVT ports | `36218`, `36226`, `36234`, `36242` |
| Actual listeners | `127.0.0.1:36218`, `127.0.0.1:36226`, `127.0.0.1:36234`, `127.0.0.1:36242` |
| Socket connect to `127.0.0.1:36242` | Success |
| Socket connect to `10.112.184.62:36242` | Connection refused |
| Advertised endpoint in instance 3 log | `10.112.184.62:36242` |
| Barex TCP device in instance 3 log | `lo`, IP `127.0.0.1` |
| KVT server initialization | Present before API readiness |
| Listener crash | Not observed |

## Log Evidence

- `XContextTcpImpl::Init ... device=[TcpDevice ... name=lo, ip=127.0.0.1]`
- `TCPServer.start_server: ip=10.112.184.62 port=36242`
- `init_kv_transfer_server ... 10.112.184.62:36242`
- Actual accepted connection: `local server 127.0.0.1:36242`

The `TCPServer.start_server` message reports the advertised endpoint, but the underlying Barex device binds the loopback interface. Therefore source instances connecting to `10.112.184.62:36242` receive `ECONNREFUSED`.

## Fault Classification

**B. Listener started, but bind and connect addresses differ.**

- Not A: listeners exist on all four loopback KVT ports.
- Not C: listener initialization completed before request traffic, and the mismatch remains after API readiness.
- Not D: the port is consistently `36242`; only the host differs.
- Not E: no listener startup traceback or listener process crash was found.

## Script-Level Correction

The experiment runner will:

1. Set `VLLM_HOST_IP=127.0.0.1` before starting scheduler, gateway, discovery, and vLLM processes.
2. Wait up to 120 seconds for every API endpoint and every KVT endpoint.
3. Require a matching `init_kv_transfer_server ... 127.0.0.1:<port>` log entry and successful TCP connection for each KVT port before sending benchmark requests.
4. Stop the smoke without issuing requests when readiness fails.

No Llumnix, vLLM, CUDA, PyTorch, or blade_kvt source was modified.
