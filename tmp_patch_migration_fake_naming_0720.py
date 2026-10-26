from pathlib import Path

import blade_kvt


p = (
    Path(blade_kvt.__file__).resolve().parent
    / "hybrid_connector"
    / "migration"
    / "backend.py"
)
s = p.read_text()
bak = p.with_suffix(".py.bak_fake_naming_0720")
if not bak.exists():
    bak.write_text(s)

old = '''        self._naming_cli = ncli
        if self._naming_cli is None:
            self._naming_url = vllm_config.kv_transfer_config.get_from_extra_config(
                "naming_url", "badbad"
            )
            self._naming_cli = connect_naming(self._inst_id, self._naming_url)
            reg_naming(self._naming_cli, vllm_config)

        self._conn_mgr = ConnManager()
        self._pmgr = PeerManager(
            self._naming_cli, None, self._conn_mgr
        )  # None means ALL, empty connpool
        self._pmgr.start(self._loop)
'''

new = '''        self._naming_cli = ncli
        self._naming_url = vllm_config.kv_transfer_config.get_from_extra_config(
            "naming_url", "fake://"
        )
        if self._naming_cli is None and self._naming_url != "fake://":
            self._naming_cli = connect_naming(self._inst_id, self._naming_url)
            reg_naming(self._naming_cli, vllm_config)

        self._conn_mgr = ConnManager()
        if self._naming_cli is None:
            self._pmgr = None
        else:
            self._pmgr = PeerManager(
                self._naming_cli, None, self._conn_mgr
            )  # None means ALL, empty connpool
            self._pmgr.start(self._loop)
'''

if old not in s and new not in s:
    raise SystemExit("target init block not found")
if old in s:
    s = s.replace(old, new, 1)

s = s.replace(
    "self._pmgr._running.get_peerid(srcinfo),",
    "self._pmgr._running.get_peerid(srcinfo) if self._pmgr is not None else srcinfo,",
)

p.write_text(s)
print("patched", p)
for i, line in enumerate(p.read_text().splitlines(), 1):
    if 160 <= i <= 182 or 354 <= i <= 361:
        print(i, line)
