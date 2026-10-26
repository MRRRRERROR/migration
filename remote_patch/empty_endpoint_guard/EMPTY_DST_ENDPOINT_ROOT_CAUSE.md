# Empty destination endpoint root cause

## Failure signature

The failed Figure 12 retry selected a real source request, then called the migration frontend with `dst_host=''` and `dst_port=0`. `KVTMigrationFrontend._rpc()` passed that tuple to `ConnManager.acquire_conn()`, which eventually called `asyncio.open_connection('', 0)` and emitted two `socket.gaierror` tracebacks because the frontend retried twice.

## Endpoint generation and registration

- Discovery publishes each instance's KV transfer endpoint from `--kv_transfer_ip` and `--kv_transfer_port`.
- Redis/CMS stores the endpoint in instance metadata and exposes it to the scheduler as `IpKvs/KvtPort` (and the newer `IpKvt` field in the metadata schema).
- `ReschedulingPolicy.executeMigrations()` copies the selected destination metadata into `MigrateRequest.DstEngineIp/DstEnginePort`.
- The Python gRPC server forwards those fields through `Llumlet.migrate()`, the ZMQ migration client, and the EngineCore migration frontend without validation.

## Runtime evidence

At the failure timestamp, CMS refresh logs showed all four instances alive with API ports 8100-8103, KVT ports 31100-31103, and `ip_kvt='127.0.0.1'`. The API and KVT readiness gate had passed. The source request later completed locally. Therefore this incident is not a persistent listener startup failure; the empty values appeared transiently between scheduler selection and the frontend boundary.

The stale-migration guard does not modify destination endpoint fields. It only ignores worker updates whose request ID is no longer in `_infly_kvt`, so it is not the producer of the empty endpoint.

## Missing validation

No layer rejected an empty host or a zero port. The final frontend boundary retried an endpoint that can never be valid, turning a recoverable migration rejection into tracebacks. The scheduler's healthy-instance filtering and the experiment readiness gate cover normal liveness, but neither replaces validation of the actual endpoint carried by a specific migration request.

## Minimal repair

`KVTMigrationFrontend.migrate()` now validates the exact host and port immediately before request serialization and any connection attempt. An invalid endpoint:

- logs `event=invalid_migration_endpoint` at warning level;
- emits a structured migration-efficiency event with `migration_status=failed_migration`;
- returns a non-success migration response (`code=1002`);
- never calls `ConnManager` or opens a socket;
- is cleaned from source-side migration bookkeeping by the existing rejected-request path;
- does not abort the source request and is never counted as a successful migration.

The runner counts `invalid_migration_endpoint` and `stale_migration_update` as failed migrations. Listener readiness and destination health remain enforced by the existing API/KVT readiness gate and scheduler health filters; no migration-selection algorithm was changed.
