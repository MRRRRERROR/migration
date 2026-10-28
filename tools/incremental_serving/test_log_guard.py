"""Incremental guard must retain fatal detection across writes and truncation."""
from pathlib import Path
import tempfile
import unittest
from runner import Incremental

class LogGuardTest(unittest.TestCase):
    def make(self,root):
        h=Incremental.__new__(Incremental);h.service=Path(root);h.processes=[]
        h.log_positions={};h.log_inodes={};h.log_tails={};return h
    def test_fatal_split_between_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            h=self.make(tmp);p=Path(tmp)/'vllm_S.log'
            p.write_bytes(b'benign history\nEngineCore encountered a fatal')
            h.runtime_check()
            with p.open('ab') as f:f.write(b' error.\n')
            with self.assertRaisesRegex(RuntimeError,'Critical runtime error'):h.runtime_check()
    def test_log_loss_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            h=self.make(tmp);p=Path(tmp)/'vllm_D.log';p.write_text('original log\n'*10)
            h.runtime_check();p.write_text('x')
            with self.assertRaisesRegex(RuntimeError,'rotated/truncated'):h.runtime_check()

if __name__=='__main__':unittest.main(verbosity=2)
