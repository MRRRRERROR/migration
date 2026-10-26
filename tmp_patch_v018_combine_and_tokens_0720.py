from pathlib import Path

core = Path("/data/home/zylu/.conda/envs/llumnix5090_vllm018/lib/python3.12/site-packages/vllm/v1/engine/core.py")
s = core.read_text()
bak = core.with_suffix(".py.bak_combine_outputs_0720")
if not bak.exists():
    bak.write_text(s)

if "def combine_outputs(" not in s:
    marker = (
        "def _add_output(\n"
        "    output: dict[int, EngineCoreOutputs], input: dict[int, list[EngineCoreOutput]]\n"
        "):\n"
        "    for client_index, kvconn_output in input.items():\n"
        "        if client_index in output:\n"
        "            output[client_index].outputs.extend(kvconn_output)\n"
        "        else:\n"
        "            output[client_index] = EngineCoreOutputs(outputs=kvconn_output)\n"
        "    return\n\n"
    )
    insert = marker + (
        "\n"
        "def combine_outputs(\n"
        "    left: dict[int, list[EngineCoreOutput]] | None,\n"
        "    right: dict[int, list[EngineCoreOutput]] | None,\n"
        ") -> dict[int, list[EngineCoreOutput]] | None:\n"
        "    \"\"\"Compatibility helper expected by blade_kvt HybridConnector.\"\"\"\n"
        "    if not left:\n"
        "        return right\n"
        "    if not right:\n"
        "        return left\n"
        "    for client_index, outputs in right.items():\n"
        "        left.setdefault(client_index, []).extend(outputs)\n"
        "    return left\n\n"
    )
    if marker not in s:
        raise SystemExit("core _add_output marker not found")
    s = s.replace(marker, insert, 1)
    core.write_text(s)
    print("patched vllm core combine_outputs")
else:
    print("vllm core combine_outputs already exists")

test = Path("/data/home/zylu/llumnix-v018-main/tests/local/vllm_e2e.py")
t = test.read_text()
bak2 = test.with_suffix(".py.bak_max_tokens_0720")
if not bak2.exists():
    bak2.write_text(t)
t2 = t.replace('"max_tokens": 1000,', '"max_tokens": 512,')
if t2 != t:
    test.write_text(t2)
    print("patched migration test max_tokens 512")
else:
    print("migration test max_tokens already patched or pattern missing")
