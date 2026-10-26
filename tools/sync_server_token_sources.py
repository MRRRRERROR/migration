"""Read-only SSH export of the server's V1/token sources into a local snapshot.

Does not deploy, overwrite the legacy simulator, or run GPU workloads.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile


REMOTE_EXPORT = r'''
import datetime, hashlib, importlib.metadata, io, json, pathlib, subprocess, sys, tarfile
root = pathlib.Path('/data/home/zylu/llumnix-v018-main')
vllm = pathlib.Path('/data/home/zylu/.conda/envs/llumnix5090_vllm018/lib/python3.12/site-packages/vllm')
overlay = pathlib.Path('/data/home/zylu/llumnix-kvt500-2gpu/runtime_overlay')
prior = root / 'results/token_migration_20261008_01/delivery_20261009_route_01/manifest.json'
extensions = {'.py', '.sh', '.yaml', '.yml', '.json', '.toml', '.md', '.txt', '.proto', '.go'}
files = {}
def add_tree(base, prefix):
    if not base.exists(): return
    for path in sorted(base.rglob('*')):
        rel = path.relative_to(base)
        if any(part in {'__pycache__', '.git', 'node_modules', 'results', 'backups', 'runner_backups'} for part in rel.parts): continue
        if path.is_symlink() or not path.is_file() or path.suffix not in extensions: continue
        if '.bak' in path.name or path.stat().st_size > 4 * 1024 * 1024: continue
        files[prefix + '/' + rel.as_posix()] = path
for rel in ('python/llumnix', 'python/discovery', 'tools', 'tests/local', 'configs', 'requirements'):
    add_tree(root / rel, 'project/' + rel)
for name in ('README.md', 'AI_HANDOFF.md', 'pyproject.toml', 'setup.py', 'setup.cfg', 'requirements.txt', '.gitmodules'):
    path = root / name
    if path.is_file(): files['project/' + name] = path
# Export installed Python sources: migration patches also live outside the repository.
add_tree(vllm, 'dependencies/vllm')
add_tree(overlay / 'blade_kvt', 'runtime_overlay/blade_kvt')
evidence = root / 'results/token_migration_20261008_01'
for name in ('manifest.json', 'test_protocol.py', 'test_gateway.py', 'test_http.py', 'PROGRESS_20261009_ROUTE.md', 'CHECKPOINT_20261009_ROUTE_ACCEPTED.md'):
    path = evidence / name
    if path.is_file(): files['reference_evidence/' + name] = path
def git(*args):
    p = subprocess.run(['git', '-C', str(root), *args], capture_output=True, text=True)
    return {'returncode': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}
before = git('status', '--porcelain=v1')
manifest = []
changed_during_export = []
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|gz') as archive:
    for name, path in sorted(files.items()):
        stat_before = path.stat()
        data = path.read_bytes()
        stat_after = path.stat()
        if (stat_before.st_mtime_ns, stat_before.st_size) != (stat_after.st_mtime_ns, stat_after.st_size):
            changed_during_export.append(str(path))
        entry = tarfile.TarInfo(name)
        entry.size = len(data); entry.mode = 0o644; entry.mtime = stat_after.st_mtime
        archive.addfile(entry, io.BytesIO(data))
        manifest.append({'path': name, 'remote_path': str(path), 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    # Re-read every exported file to detect concurrent source edits.
    for row in manifest:
        if hashlib.sha256(pathlib.Path(row['remote_path']).read_bytes()).hexdigest() != row['sha256']:
            changed_during_export.append(row['remote_path'])
    after = git('status', '--porcelain=v1')
    versions = {}
    # Read distribution metadata without importing torch/vllm or initializing GPUs.
    site = vllm.parent
    for dist in importlib.metadata.distributions(path=[str(site)]):
        name = dist.metadata.get('Name', '')
        if name.lower() in {'vllm', 'torch', 'transformers', 'ray', 'llumnix', 'blade-kvt'}:
            versions[name] = dist.version
    metadata = {'exported_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'remote_root': str(root), 'git_head': git('rev-parse', 'HEAD'),
        'git_status_before': before, 'git_status_after': after,
        'tracked_diff': git('diff', 'HEAD', '--', 'python/llumnix', 'python/discovery', 'tests/local', 'tools'),
        'versions': versions, 'files': manifest,
        'changed_during_export': sorted(set(changed_during_export)),
        'previous_release_manifest': json.loads(prior.read_text()) if prior.exists() else None}
    data = json.dumps(metadata, indent=2).encode()
    entry = tarfile.TarInfo('sync_manifest.json'); entry.size = len(data); entry.mode = 0o644
    archive.addfile(entry, io.BytesIO(data))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--identity', type=Path, required=True)
    parser.add_argument('--known-hosts', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    dest = args.destination.resolve()
    dest.mkdir(parents=True, exist_ok=False)
    archive_path = dest / 'server_sources.tar.gz'
    command = ['ssh', '-i', str(args.identity.resolve()), '-o', 'BatchMode=yes',
               '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(args.known_hosts.resolve()),
               '-o', 'ConnectTimeout=15', 'zylu@10.112.184.62', 'python3 -']
    with archive_path.open('wb') as output:
        process = subprocess.run(command, input=REMOTE_EXPORT.encode(), stdout=output,
                                 stderr=subprocess.PIPE, timeout=180)
    if process.returncode:
        raise RuntimeError(process.stderr.decode(errors='replace'))
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive.getmembers():
            parts = PurePosixPath(member.name)
            if parts.is_absolute() or '..' in parts.parts or not member.isfile():
                raise ValueError('Unsafe archive member: ' + member.name)
            target = dest.joinpath(*parts.parts).resolve()
            if not target.is_relative_to(dest):
                raise ValueError('Archive path escaped snapshot')
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open('wb') as output:
                output.write(source.read())
    metadata = json.loads((dest / 'sync_manifest.json').read_text(encoding='utf-8'))
    if metadata['changed_during_export']:
        raise RuntimeError('Server sources changed during export; snapshot not accepted')
    for row in metadata['files']:
        data = (dest / row['path']).read_bytes()
        if hashlib.sha256(data).hexdigest() != row['sha256']:
            raise RuntimeError('Hash mismatch: ' + row['path'])
    mapping = {row['remote_path']: row for row in metadata['files']}
    comparison = []
    for previous in metadata['previous_release_manifest'] or []:
        current = mapping.get(previous['path'])
        status = 'missing' if not current else ('unchanged' if current['sha256'] == previous['after_sha256'] else 'changed')
        comparison.append({'remote_path': previous['path'], 'status': status,
                           'snapshot_path': current['path'] if current else None,
                           'previous_sha256': previous['after_sha256'],
                           'current_sha256': current['sha256'] if current else None})
    (dest / 'previous_release_comparison.json').write_text(json.dumps(comparison, indent=2), encoding='utf-8')
    syntax_failures = []
    python_count = 0
    for row in metadata['files']:
        if row['path'].endswith('.py'):
            python_count += 1
            try:
                ast.parse((dest / row['path']).read_bytes(), filename=row['path'])
            except (SyntaxError, ValueError) as exc:
                syntax_failures.append({'path': row['path'], 'error': str(exc)})
    verification = {'verified_file_count': len(metadata['files']), 'python_syntax_checked': python_count,
                    'syntax_failures': syntax_failures,
                    'archive_sha256': hashlib.sha256(archive_path.read_bytes()).hexdigest(),
                    'previous_release_counts': {status: sum(x['status'] == status for x in comparison)
                                                for status in ('unchanged', 'changed', 'missing')}}
    (dest / 'verification.json').write_text(json.dumps(verification, indent=2), encoding='utf-8')
    diff = metadata['tracked_diff']['stdout']
    (dest / 'server_working_tree.patch').write_text(diff, encoding='utf-8')
    lines = ['# 服务器 token migration 源码同步', '',
             '- 本地快照：`' + str(dest) + '`',
             '- 服务器：`' + metadata['remote_root'] + '`',
             '- Git HEAD：`' + metadata['git_head']['stdout'].strip() + '`',
             '- 导出 UTC 时间：`' + metadata['exported_at_utc'] + '`',
             '- 校验文件数：' + str(len(metadata['files'])),
             '- Python 语法检查文件数：' + str(python_count),
             '- 语法错误数：' + str(len(syntax_failures)),
             '- 相比 2026-10-09 交付：`' + json.dumps(verification['previous_release_counts']) + '`', '',
             '## 快照布局', '',
             '- `project/`：当前 V1 Llumnix 源码、工具、配置和本地测试。',
             '- `dependencies/vllm/`：实际安装环境中的 vLLM Python 源码，包含环境内补丁。',
             '- `runtime_overlay/blade_kvt/`：运行时 overlay 的 Python 源码。',
             '- `reference_evidence/`：此前验收记录与协议测试；这些记录不代表本次重新验收。',
             '- `sync_manifest.json`：远端路径、逐文件 SHA256、Git 状态、版本信息。',
             '- `server_working_tree.patch`：服务器当前 tracked 文件相对 HEAD 的改动。', '',
             '## 使用范围', '',
             '这是独立源码快照；原有 legacy 仿真代码没有被覆盖。',
             '未同步模型权重、密钥、日志全集、native 二进制或完整 conda 环境。',
             '本次完成传输完整性与 Python 语法检查，未启动 GPU、未运行模型或重新验收迁移功能。',
             '要在现有模拟器中运行 token migration，仍需适配状态机、重建调度和性能模型。', '',
             '## 相比此前交付的变更文件', '']
    changed = [row for row in comparison if row['status'] != 'unchanged']
    lines.extend('- `' + row['remote_path'] + '`：' + row['status'] for row in changed)
    if not changed: lines.append('此前交付清单中的文件均与服务器当前版本一致。')
    if syntax_failures:
        lines.extend(['', '## 语法检查限制', '', '详见 `verification.json`。'])
    (dest / 'SYNC_REPORT.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps({'destination': str(dest), **verification}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
