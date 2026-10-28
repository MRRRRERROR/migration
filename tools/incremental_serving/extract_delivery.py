"""Safely extract the immutable server archive and materialize a local mirror.

Directory symlinks are recorded rather than created on Windows. The accepted
run's shared service directory is materialized as ordinary NTFS hardlinked files
to avoid duplicating multi-GB logs. No raw existing file is silently replaced.
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
from pathlib import Path,PurePosixPath
import tarfile


PREFIX='incremental_runtime_migration_20261027_01'


def link_or_copy(src,dest):
    """Windows sandbox policy can deny hardlinks within a writable directory."""
    try:os.link(src,dest)
    except PermissionError:shutil.copyfile(src,dest)


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as inp:
        for data in iter(lambda:inp.read(1024*1024),b''):h.update(data)
    return h.hexdigest()


def main():
    p=argparse.ArgumentParser();p.add_argument('archive');p.add_argument('snapshot');p.add_argument('mirror');a=p.parse_args()
    snapshot=Path(a.snapshot).resolve();mirror=Path(a.mirror).resolve();snapshot.mkdir(exist_ok=False)
    hashes=[];links=[];hard=[]
    def target(name):
        rel=PurePosixPath(name).relative_to(PREFIX)
        if '..' in rel.parts:raise ValueError('Archive parent traversal')
        out=snapshot.joinpath(*rel.parts)
        if not out.resolve().is_relative_to(snapshot):raise ValueError('Archive escapes snapshot')
        return out
    with tarfile.open(a.archive,'r:gz') as archive:
        for member in archive:
            out=target(member.name)
            if member.isdir():out.mkdir(parents=True,exist_ok=True)
            elif member.isfile():
                out.parent.mkdir(parents=True,exist_ok=True);h=hashlib.sha256()
                with archive.extractfile(member) as inp,out.open('xb') as dest:
                    for data in iter(lambda:inp.read(1024*1024),b''):dest.write(data);h.update(data)
                hashes.append(dict(path=str(out.relative_to(snapshot)),bytes=member.size,sha256=h.hexdigest()))
            elif member.issym():links.append(dict(path=str(out.relative_to(snapshot)),target=member.linkname))
            elif member.islnk():hard.append((out,target(member.linkname)))
            else:raise ValueError('Unexpected special archive member: '+member.name)
    for out,src in hard:
        if not src.is_file():raise ValueError('Missing archive hardlink target')
        out.parent.mkdir(parents=True,exist_ok=True);link_or_copy(src,out)
        hashes.append(dict(path=str(out.relative_to(snapshot)),bytes=src.stat().st_size,sha256=digest(src)))
    with (snapshot/'SNAPSHOT_FILE_SHA256.csv').open('x',encoding='utf-8-sig',newline='') as out:
        writer=csv.DictWriter(out,fieldnames=['path','bytes','sha256']);writer.writeheader();writer.writerows(hashes)
    (snapshot/'SNAPSHOT_SYMLINKS.json').write_text(json.dumps(links,indent=2),encoding='utf-8')
    mirrored=0;unchanged=0;skipped=[]
    def synchronize(src,dest):
        nonlocal mirrored,unchanged
        dest.mkdir(parents=True,exist_ok=True)
        for file in src.rglob('*'):
            if not file.is_file():continue
            rel=file.relative_to(src);out=dest/rel
            if '__pycache__' in rel.parts:skipped.append(str(rel));continue
            out.parent.mkdir(parents=True,exist_ok=True)
            if out.exists():
                if file.stat().st_size!=out.stat().st_size or digest(file)!=digest(out):
                    raise ValueError('Existing raw mirror file differs; preserved both: '+str(out))
                unchanged+=1
            else:link_or_copy(file,out);mirrored+=1
    synchronize(snapshot/'paired_abc_01',mirror)
    synchronize(snapshot/'services_01',mirror/'services_01')
    result=dict(files_in_archive=len(hashes),recorded_directory_symlinks=links,mirrored_new_files=mirrored,
        existing_identical_files=unchanged,skipped_cache_files=skipped,
        raw_archive=str(Path(a.archive).resolve()),snapshot=str(snapshot),mirror=str(mirror),
        note='Mirror files may be hardlinked to immutable snapshot; do not modify raw data. Original compressed archive retained with SHA256.')
    (mirror/'snapshot_provenance.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('recorded_directory_symlinks','skipped_cache_files')}))


if __name__=='__main__':main()
