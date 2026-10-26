from pathlib import Path
import re
import shutil
import time


STAMP = time.strftime("%Y%m%d_%H%M%S")


def backup(path: str) -> Path:
    p = Path(path)
    bak = p.with_name(p.name + f".bak_0714_batch_{STAMP}")
    if p.exists() and not bak.exists():
        shutil.copy2(p, bak)
    return p


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        print(f"[skip] {label}")
        return text
    if old not in text:
        print(f"[miss] {label}")
        return text
    print(f"[patch] {label}")
    return text.replace(old, new, 1)


def patch_api_server() -> None:
    p = backup("llumnix/entrypoints/vllm_v1/api_server.py")
    s = p.read_text()

    if "from vllm.v1.request import EngineCoreRequest" not in s:
        s = s.replace(
            "from vllm.sampling_params import SamplingParams\n",
            "from vllm.sampling_params import SamplingParams\n"
            "from vllm.v1.request import EngineCoreRequest\n"
            "from transformers import AutoTokenizer\n",
            1,
        )
        print("[patch] api_server imports EngineCoreRequest/AutoTokenizer")

    if "llumnix_tokenizer = None" not in s:
        s = s.replace(
            "llumnix_client: LlumnixClientVLLMV1 = None\n",
            "llumnix_client: LlumnixClientVLLMV1 = None\n"
            "llumnix_tokenizer = None\n",
            1,
        )
        print("[patch] api_server tokenizer global")

    helper = r'''
def iter_llumnix_results(results_generator):
    return results_generator.generator() if hasattr(results_generator, "generator") else results_generator


def make_engine_core_request(prompt: str, sampling_params: SamplingParams, request_id: str) -> EngineCoreRequest:
    prompt_token_ids = llumnix_tokenizer.encode(prompt, add_special_tokens=False)
    return EngineCoreRequest(
        request_id=request_id,
        prompt_token_ids=prompt_token_ids,
        mm_features=None,
        sampling_params=sampling_params,
        pooling_params=None,
        arrival_time=time.time(),
        lora_request=None,
        cache_salt=None,
        data_parallel_rank=None,
        prompt_embeds=None,
        prompt_is_token_ids=None,
        client_index=0,
        current_wave=0,
        priority=0,
        trace_headers=None,
        resumable=False,
        external_req_id=request_id,
        reasoning_ended=None,
        reasoning_parser_kwargs=None,
        abort_immediately=False,
    )

'''
    if "def make_engine_core_request(" not in s:
        s = s.replace("app = FastAPI(lifespan=lifespan)\n", helper + "\napp = FastAPI(lifespan=lifespan)\n", 1)
        print("[patch] api_server EngineCoreRequest helper")

    s = s.replace("async for request_output in results_generator.generator():", "async for request_output in iter_llumnix_results(results_generator):")

    if "engine_core_request=make_engine_core_request(prompt, sampling_params, request_id)" not in s:
        s = s.replace(
            "results_generator = await llumnix_client.generate(prompt, sampling_params, request_id)",
            "results_generator = await llumnix_client.generate(\n"
            "        prompt, sampling_params, request_id,\n"
            "        engine_core_request=make_engine_core_request(prompt, sampling_params, request_id),\n"
            "    )",
            1,
        )
        print("[patch] api_server benchmark passes EngineCoreRequest")

    if "llumnix_tokenizer = AutoTokenizer.from_pretrained" not in s:
        target = "entrypoints_context = setup_llumnix(entrypoints_args, manager_args, instance_args, vllm_engine_args, launch_args)\n"
        s = s.replace(
            target,
            target +
            "        llumnix_tokenizer = AutoTokenizer.from_pretrained(engine_args.model, trust_remote_code=True)\n",
            1,
        )
        print("[patch] api_server load tokenizer")

    # vLLM 0.24 AsyncMPClient constructor changed. For this smoke path, do not initialize it.
    s = s.replace(
        "llumnix_client = LlumnixClientVLLMV1(entrypoints_context, loop)",
        "llumnix_client = LlumnixClientVLLMV1(entrypoints_context, loop, None, None, False)",
    )

    p.write_text(s)


def patch_client() -> None:
    p = backup("llumnix/entrypoints/vllm_v1/client.py")
    s = p.read_text()

    # Avoid hard import failure on vLLM 0.24 where AsyncStream moved/vanished.
    if "try:\n    from vllm.engine.async_llm_engine import AsyncStream" not in s:
        s = s.replace(
            "from vllm.engine.async_llm_engine import AsyncStream\n",
            "try:\n    from vllm.engine.async_llm_engine import AsyncStream\n"
            "except Exception:\n    AsyncStream = object\n",
            1,
        )
        print("[patch] client AsyncStream fallback")

    old_init = "        AsyncMPClient.__init__(self, vllm_config, executor_class, log_stats, client_addresses, client_index, driver_tensor_queue_union)\n"
    new_init = (
        "        self.client_index = client_index\n"
        "        self.outputs_queue = asyncio.Queue()\n"
        "        if vllm_config is not None and executor_class is not None:\n"
        "            AsyncMPClient.__init__(self, vllm_config, executor_class, log_stats, client_addresses, client_index, driver_tensor_queue_union)\n"
        "        else:\n"
        "            logger.warning(\"Skip AsyncMPClient init for vLLM 0.24 Llumnix smoke path.\")\n"
    )
    s = replace_once(s, old_init, new_init, "client optional AsyncMPClient init")

    gen_pattern = re.compile(
        r"    async def generate\(self,.*?\n    # pylint: disable=arguments-differ\n    async def _generate_by_manager",
        re.S,
    )
    gen_new = r'''    async def generate(self,
                       prompt: str,
                       sampling_params: SamplingParams,
                       request_id: str,
                       *args,
                       **kwargs):
        if sampling_params.n > 1:
            raise ValueError("Unsupported feature: multiple sequence decoding")
        logger.info("Client received request {}".format(request_id))
        request_processing_context: RequestProcessingContext = RequestProcessingContext.deepcopy_from_server_info(
            server_info=self.server_info,
            enable_trace=kwargs.get(LLUMNIX_TRACE_REQUEST, False),
        )
        # vLLM 0.24 smoke path: bypass manager and send EngineCoreRequest directly to a local instance.
        prefill_instance_id, decode_instance_id = await self._generate_by_instance(
            request_id,
            request_processing_context,
            prompt,
            sampling_params,
            *args,
            **kwargs
        )
        self.request_instances[request_id].append(prefill_instance_id)
        self.instance_requests[prefill_instance_id].add(request_id)
        if decode_instance_id and decode_instance_id == prefill_instance_id:
            self.request_instances[request_id].append(decode_instance_id)
            self.instance_requests[decode_instance_id].add(request_id)

        async def llumnix_output_generator():
            while True:
                engine_outputs = await self.outputs_queue.get()
                for request_output in engine_outputs.outputs:
                    if request_output.request_id != request_id:
                        continue
                    yield request_output
                    if getattr(request_output, "finished", False):
                        return

        return llumnix_output_generator()

    # pylint: disable=arguments-differ
    async def _generate_by_manager'''
    if "vLLM 0.24 smoke path: bypass manager" not in s:
        s, n = gen_pattern.subn(gen_new, s, count=1)
        print(f"[patch] client generate smoke path n={n}")

    instance_recover = r'''
            if not self.instance_num_requests:
                from llumnix.ray_utils import update_cluster_actor_handles, LlumnixActor
                recovered_instances = update_cluster_actor_handles(LlumnixActor.INSTANCE, {})
                if recovered_instances:
                    self.instances.update(recovered_instances)
                    self.cached_cluster_instances.update(recovered_instances)
                    for recovered_instance_id in recovered_instances:
                        self.instance_num_requests.setdefault(recovered_instance_id, 0)
                    logger.warning("Recovered local instances from Ray named actors: %s", list(recovered_instances.keys()))
'''
    if "Recovered local instances from Ray named actors" not in s:
        s = s.replace("        try:\n            # pylint: disable=no-else-return\n", "        try:\n" + instance_recover + "            # pylint: disable=no-else-return\n", 1)
        print("[patch] client recover local instances")

    p.write_text(s)


def patch_core() -> None:
    p = backup("llumnix/backends/vllm_v1/core.py")
    s = p.read_text()

    # Robust imports for moving vLLM APIs.
    if "EngineShutdownState" not in s.split("logger = init_logger")[0]:
        lines = s.splitlines()
        inserted = False
        for i, line in enumerate(lines):
            if line.startswith("from vllm.v1.engine.core import "):
                lines.insert(i + 1, "from vllm.v1.engine.core import EngineShutdownState")
                inserted = True
                break
        if inserted:
            s = "\n".join(lines) + "\n"
            print("[patch] core EngineShutdownState import")

    if "def ensure_engine_core_request_compat(" not in s:
        helper = r'''
def ensure_engine_core_request_compat(request):
    """vLLM 0.24 EngineCoreRequest is slot-based; Llumnix expects older request fields."""
    request_cls = type(request)

    def _extra_args(obj):
        sampling_params = getattr(obj, "sampling_params", None)
        extra_args = getattr(sampling_params, "extra_args", None) if sampling_params is not None else None
        if extra_args is None and sampling_params is not None:
            try:
                sampling_params.extra_args = {}
                extra_args = sampling_params.extra_args
            except Exception:
                extra_args = None
        return extra_args if isinstance(extra_args, dict) else {}

    def _extra_property(name, default=None):
        def _getter(self):
            return _extra_args(self).get(name, default)
        def _setter(self, value):
            _extra_args(self)[name] = value
        return property(_getter, _setter)

    if not hasattr(request_cls, "kv_transfer_params"):
        def _get_kv_transfer_params(self):
            return _extra_args(self).get("kv_transfer_params", None)
        def _set_kv_transfer_params(self, value):
            _extra_args(self)["kv_transfer_params"] = value
        request_cls.kv_transfer_params = property(_get_kv_transfer_params, _set_kv_transfer_params)

    if not hasattr(request_cls, "status"):
        try:
            from vllm.v1.request import RequestStatus as VLLMRequestStatus
            default_status = VLLMRequestStatus.WAITING
        except Exception:
            default_status = "WAITING"
        def _get_status(self):
            return _extra_args(self).get("_llumnix_vllm_status", default_status)
        def _set_status(self, value):
            _extra_args(self)["_llumnix_vllm_status"] = value
        request_cls.status = property(_get_status, _set_status)

    for name, default in {
        "num_computed_tokens": 0,
        "num_output_tokens": 0,
        "output_token_ids": [],
        "finished": False,
        "stop_reason": None,
    }.items():
        if not hasattr(request_cls, name):
            setattr(request_cls, name, _extra_property(name, default))

    if not hasattr(request_cls, "num_prompt_tokens"):
        request_cls.num_prompt_tokens = property(lambda self: len(getattr(self, "prompt_token_ids", None) or []))
    if not hasattr(request_cls, "num_tokens"):
        request_cls.num_tokens = property(
            lambda self: len(getattr(self, "prompt_token_ids", None) or []) +
                         len(_extra_args(self).get("output_token_ids", []) or [])
        )
    return request

'''
        s = s.replace("logger = init_logger(__name__)\n", "logger = init_logger(__name__)\n\n" + helper, 1)
        print("[patch] core request compatibility helper")

    # vLLM 0.24 EngineCoreProcWrapper state fields.
    needle = "        super().__init__(vllm_config, on_head_node, handshake_address, executor_class, log_stats, engine_index)\n"
    insert = (
        "        self.shutdown_state = EngineShutdownState.RUNNING  # vLLM 0.24 compatibility\n"
        "        self.engines_running = False  # vLLM 0.24 compatibility\n"
        "        self.process_input_queue_block = False  # vLLM 0.24 compatibility\n"
    )
    if insert not in s and needle in s:
        s = s.replace(needle, needle + insert, 1)
        print("[patch] core wrapper state fields")

    # Make add_request sanitize request before vLLM sees it and add broad debug.
    if "request = ensure_engine_core_request_compat(request)" not in s:
        s = s.replace("        super().add_request(request)\n", "        request = ensure_engine_core_request_compat(request)\n        logger.warning(\"DEBUG_ENGINE_ADD_REQUEST compat request_id=%s cls=%s\", getattr(request, \"request_id\", None), type(request))\n        super().add_request(request)\n", 1)
        print("[patch] core add_request compat before super")
    if "DEBUG_AFTER_SUPER_ADD" not in s:
        s = s.replace(
            "        super().add_request(request)\n",
            "        super().add_request(request)\n"
            "        try:\n"
            "            logger.warning(\"DEBUG_AFTER_SUPER_ADD request_id=%s waiting=%s running=%s\", getattr(request, \"request_id\", None), len(getattr(self.scheduler, \"waiting\", [])), len(getattr(self.scheduler, \"running\", [])))\n"
            "        except Exception:\n"
            "            logger.exception(\"DEBUG_AFTER_SUPER_ADD failed\")\n",
            1,
        )
        print("[patch] core after-super debug")

    # Guard empty output batches; old code assumes non-empty outputs.
    if "DEBUG_PUT_OUTPUTS empty outputs" not in s:
        s = s.replace(
            "        set_timestamp(engine_core_output_all, 'engine_step_timestamp_begin', self.step_begin_time)\n",
            "        if not engine_core_output_all:\n"
            "            logger.debug(\"DEBUG_PUT_OUTPUTS empty outputs\")\n"
            "            return\n\n"
            "        set_timestamp(engine_core_output_all, 'engine_step_timestamp_begin', self.step_begin_time)\n",
            1,
        )
        print("[patch] core empty output guard")

    # Backend add_request should tolerate missing engine_core_request during non-smoke paths.
    if "kwargs.get(\"engine_core_request\")" not in s:
        s = s.replace("        request: EngineCoreRequest = kwargs[\"engine_core_request\"]\n", "        request: EngineCoreRequest = kwargs.get(\"engine_core_request\")\n        if request is None:\n            raise ValueError(\"engine_core_request is required for vLLM 0.24 Llumnix path\")\n", 1)
        print("[patch] backend add_request explicit request guard")

    p.write_text(s)


def patch_scheduler() -> None:
    p = backup("llumnix/backends/vllm_v1/scheduler.py")
    s = p.read_text()
    old = "                if req.status != RequestStatus.RUNNING:\n                    continue\n"
    new = "                req_status = getattr(req, 'status', RequestStatus.RUNNING)\n                if str(req_status).split('.')[-1] != 'RUNNING':\n                    continue\n"
    s = replace_once(s, old, new, "scheduler tolerant request status")
    p.write_text(s)


def main() -> None:
    patch_api_server()
    patch_client()
    patch_core()
    patch_scheduler()
    print("\n[done] batch compatibility patch written. Now run:")
    print("python -m py_compile llumnix/entrypoints/vllm_v1/api_server.py llumnix/entrypoints/vllm_v1/client.py llumnix/backends/vllm_v1/core.py llumnix/backends/vllm_v1/scheduler.py")


if __name__ == "__main__":
    main()
