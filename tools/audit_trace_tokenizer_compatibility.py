#!/usr/bin/env python3
"""Audit whether a pre-tokenized JSONL trace is safe to decode with another tokenizer."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import logging
import math
import random
from collections import Counter
from pathlib import Path
from typing import Any

from transformers import AutoTokenizer
from transformers.utils.hub import cached_file


TOKENIZER_FILES = ("tokenizer.json", "tokenizer_config.json", "special_tokens_map.json")


def percentile(values: list[int], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * p / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return float(ordered[lo])
    return ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_tokenizer_file(model: str, filename: str) -> Path | None:
    local = Path(model) / filename
    if local.is_file():
        return local.resolve()
    try:
        resolved = cached_file(model, filename, _raise_exceptions_for_missing_entries=False)
    except Exception as exc:
        logging.warning("Cannot resolve %s/%s: %r", model, filename, exc)
        return None
    return Path(resolved).resolve() if resolved else None


def tokenizer_info(tok: Any) -> dict[str, Any]:
    special_map = getattr(tok, "special_tokens_map", {}) or {}
    added = list(getattr(tok, "additional_special_tokens", None) or
                 special_map.get("additional_special_tokens", []) or
                 tok.init_kwargs.get("additional_special_tokens", []) or [])
    return {
        "class": type(tok).__name__,
        "name_or_path": tok.name_or_path,
        "vocab_size_property": tok.vocab_size,
        "len_tokenizer": len(tok),
        "eos_token_id": tok.eos_token_id,
        "bos_token_id": tok.bos_token_id,
        "pad_token_id": tok.pad_token_id,
        "additional_special_tokens": added,
        "additional_special_token_ids": tok.convert_tokens_to_ids(added),
        "chat_template_exists": bool(tok.chat_template),
        "chat_template": tok.chat_template,
    }


def short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="surrogatepass")).hexdigest()


def preview(value: str, limit: int = 240) -> str:
    escaped = value.replace("\n", "\\n").replace("\r", "\\r")
    return escaped[:limit]


def read_trace(path: Path) -> tuple[list[dict[str, Any]], Counter[str]]:
    rows: list[dict[str, Any]] = []
    keys: Counter[str] = Counter()
    with path.open(encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            row["__line_number"] = line_no
            rows.append(row)
            keys.update(k for k in row if not k.startswith("__"))
    return rows, keys


def write_html(output: Path, summary: dict[str, Any]) -> None:
    stats = summary["trace_statistics"]
    sample = summary["sample_audit"]
    mapping = summary["trace_token_id_mapping"]
    structural = summary["structural_comparison"]
    hashes = summary["hashes"]
    length_rows = "".join(
        f"<tr><td>{html.escape(name)}</td><td>{html.escape(str(vals['source']['sha256']))}</td>"
        f"<td>{html.escape(str(vals['target']['sha256']))}</td><td>{vals['same']}</td></tr>"
        for name, vals in hashes["files"].items()
    )
    body = f"""<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>
<title>Tokenizer 一致性审计</title><style>body{{font-family:Arial,'Microsoft YaHei';max-width:1180px;margin:30px auto;line-height:1.55;color:#172033}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccd3df;padding:7px;text-align:left;word-break:break-all}}code,pre{{background:#eef2f7;padding:3px 6px}}.verdict{{padding:14px;border-left:6px solid #c47b00;background:#fff5d9}}</style></head><body>
<h1>Trace Tokenizer 一致性审计</h1><p class='verdict'><strong>{html.escape(summary['verdict']['level'])}</strong>：{html.escape(summary['verdict']['text'])}</p>
<h2>来源</h2><pre>{html.escape(json.dumps(summary['provenance'], indent=2, ensure_ascii=False))}</pre>
<h2>Trace</h2><p>总行数 {stats['total_rows']}；含 input_tok_ids {stats['rows_with_input_tok_ids']}；长度 min/P50/P90/P95/max = {stats['input_length']['min']}/{stats['input_length']['p50']}/{stats['input_length']['p90']}/{stats['input_length']['p95']}/{stats['input_length']['max']}。</p>
<h2>静态结构</h2><pre>{html.escape(json.dumps(structural, indent=2, ensure_ascii=False))}</pre>
<h2>文件哈希</h2><table><tr><th>文件</th><th>Source SHA256</th><th>Target SHA256</th><th>相同</th></tr>{length_rows}</table>
<h2>Trace 实际 token-id</h2><p>唯一 ID {mapping['unique_token_ids']}；一致 {mapping['matching']}；不一致 {mapping['different']}；不一致比例 {mapping['different_ratio']:.6%}。</p>
<h2>固定样本审计</h2><pre>{html.escape(json.dumps(sample, indent=2, ensure_ascii=False))}</pre>
<h2>Chat template</h2><pre>{html.escape(json.dumps(summary['chat_template_risk'], indent=2, ensure_ascii=False))}</pre>
</body></html>"""
    (output / "report.html").write_text(body, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", required=True)
    parser.add_argument("--source-model", required=True)
    parser.add_argument("--target-model", required=True)
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--generation-script", default=None)
    parser.add_argument("--generation-command", default=None)
    args = parser.parse_args()

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(output / "audit.log", encoding="utf-8"), logging.StreamHandler()],
    )

    trace_path = Path(args.trace).resolve()
    rows, field_counts = read_trace(trace_path)
    with_ids = [row for row in rows if isinstance(row.get("input_tok_ids"), list)]
    lengths = [len(row["input_tok_ids"]) for row in with_ids]
    if not with_ids:
        raise RuntimeError("Trace has no rows with input_tok_ids")

    logging.info("Loading source tokenizer: %s", args.source_model)
    source = AutoTokenizer.from_pretrained(args.source_model, trust_remote_code=True)
    logging.info("Loading target tokenizer: %s", args.target_model)
    target = AutoTokenizer.from_pretrained(args.target_model, trust_remote_code=True)
    source_info, target_info = tokenizer_info(source), tokenizer_info(target)
    source_vocab, target_vocab = source.get_vocab(), target.get_vocab()
    common_tokens = source_vocab.keys() & target_vocab.keys()
    id_mismatch_tokens = [t for t in common_tokens if source_vocab[t] != target_vocab[t]]
    source_only = sorted(source_vocab.keys() - target_vocab.keys())
    target_only = sorted(target_vocab.keys() - source_vocab.keys())
    rng = random.Random(args.seed)
    vocab_diff_pool = (
        [(t, source_vocab.get(t), target_vocab.get(t), "id_mismatch") for t in id_mismatch_tokens]
        + [(t, source_vocab[t], None, "source_only") for t in source_only]
        + [(t, None, target_vocab[t], "target_only") for t in target_only]
    )
    vocab_diff_examples = rng.sample(vocab_diff_pool, min(30, len(vocab_diff_pool)))

    special_ids_equal = all(source_info[k] == target_info[k] for k in (
        "eos_token_id", "bos_token_id", "pad_token_id", "additional_special_token_ids"))
    structural = {
        "vocab_exactly_equal": source_vocab == target_vocab,
        "chat_template_exactly_equal": source.chat_template == target.chat_template,
        "special_token_ids_equal": special_ids_equal,
        "source": source_info,
        "target": target_info,
        "vocab": {
            "common_token_count": len(common_tokens),
            "common_token_id_mismatch_count": len(id_mismatch_tokens),
            "source_only_token_count": len(source_only),
            "target_only_token_count": len(target_only),
            "difference_examples": [
                {"token": t, "source_id": s, "target_id": d, "kind": kind}
                for t, s, d, kind in vocab_diff_examples
            ],
        },
    }

    hashes: dict[str, Any] = {"files": {}}
    for filename in TOKENIZER_FILES:
        src_path = resolve_tokenizer_file(args.source_model, filename)
        dst_path = resolve_tokenizer_file(args.target_model, filename)
        src = {"path": str(src_path) if src_path else None, "sha256": sha256(src_path) if src_path else None}
        dst = {"path": str(dst_path) if dst_path else None, "sha256": sha256(dst_path) if dst_path else None}
        hashes["files"][filename] = {"source": src, "target": dst, "same": bool(src["sha256"] and src["sha256"] == dst["sha256"])}
    (output / "hashes.json").write_text(json.dumps(hashes, indent=2, ensure_ascii=False), encoding="utf-8")

    unique_ids = sorted({int(token_id) for row in with_ids for token_id in row["input_tok_ids"]})
    mapping_diffs: list[dict[str, Any]] = []
    matching = 0
    for token_id in unique_ids:
        src_token = source.convert_ids_to_tokens(token_id)
        dst_token = target.convert_ids_to_tokens(token_id)
        if src_token == dst_token:
            matching += 1
        else:
            mapping_diffs.append({"token_id": token_id, "source_token": src_token, "target_token": dst_token})
    with (output / "token_id_mapping_diff.csv").open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=["token_id", "source_token", "target_token"])
        writer.writeheader()
        writer.writerows(mapping_diffs)

    sample_count = min(args.sample_size, len(with_ids))
    sample_indices = sorted(rng.sample(range(len(with_ids)), sample_count))
    sample_records: list[dict[str, Any]] = []
    for sample_order, idx in enumerate(sample_indices):
        row = with_ids[idx]
        ids = list(map(int, row["input_tok_ids"]))
        src_raw = source.decode(ids, skip_special_tokens=False)
        dst_raw = target.decode(ids, skip_special_tokens=False)
        src_visible = source.decode(ids, skip_special_tokens=True)
        dst_visible = target.decode(ids, skip_special_tokens=True)
        src_reencoded = source.encode(src_raw, add_special_tokens=False)
        dst_reencoded = target.encode(src_raw, add_special_tokens=False)
        sample_records.append({
            "sample_order": sample_order,
            "trace_line": row["__line_number"],
            "input_token_count": len(ids),
            "decoded_text_exactly_equal": src_raw == dst_raw,
            "visible_text_exactly_equal": src_visible == dst_visible,
            "original_ids_equal_source_reencoded": ids == src_reencoded,
            "original_ids_equal_target_reencoded": ids == dst_reencoded,
            "source_reencoded_equal_target_reencoded": src_reencoded == dst_reencoded,
            "source_decoded_sha256": short_hash(src_raw),
            "target_decoded_sha256": short_hash(dst_raw),
            "source_visible_sha256": short_hash(src_visible),
            "target_visible_sha256": short_hash(dst_visible),
            "source_decoded_preview": preview(src_raw),
            "target_decoded_preview": preview(dst_raw),
        })
    with (output / "sample_comparison.csv").open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(sample_records[0]))
        writer.writeheader()
        writer.writerows(sample_records)

    def passed(field: str) -> int:
        return sum(bool(record[field]) for record in sample_records)

    trace_has_original_text = any(k in field_counts for k in (
        "prompt", "text", "conversation", "conversations", "messages"))
    source_special_ids = getattr(source, "all_special_ids", None)
    if source_special_ids is None:
        source_special_ids = [value for value in (
            source_info["eos_token_id"], source_info["bos_token_id"], source_info["pad_token_id"],
            *source_info["additional_special_token_ids"],
        ) if isinstance(value, int)]
    special_ids_in_trace = sorted(set(unique_ids) & set(source_special_ids))
    trace_includes_chat_template = bool(special_ids_in_trace)
    trace_mapping_all_equal = not mapping_diffs
    decode_all_equal = passed("decoded_text_exactly_equal") == sample_count
    source_roundtrip_all = passed("original_ids_equal_source_reencoded") == sample_count
    target_roundtrip_all = passed("original_ids_equal_target_reencoded") == sample_count
    templates_equal = source.chat_template == target.chat_template

    if (source_vocab == target_vocab and trace_mapping_all_equal and decode_all_equal
            and source_roundtrip_all and target_roundtrip_all
            and (templates_equal or not trace_includes_chat_template)):
        verdict_level = "A. 完全兼容"
        verdict_text = "该 trace 的 input_tok_ids 可直接使用 Qwen2.5-7B-Instruct tokenizer 解码，未观察到 token 映射或模板语义不一致。"
        direct_safe = True
    elif trace_mapping_all_equal and decode_all_equal and source_roundtrip_all and target_roundtrip_all:
        verdict_level = "C. 部分兼容"
        verdict_text = ("本 trace 实际使用的 token-id 和固定样本均兼容，但两个 tokenizer 的完整静态结构不完全一致。"
                        "当前文件可直接解码；为避免未来 trace 使用到差异 token，生成或恢复流程仍应显式使用源 tokenizer。")
        direct_safe = True
    elif mapping_diffs and len(mapping_diffs) / len(unique_ids) >= 0.01:
        verdict_level = "D. 明确不兼容"
        verdict_text = "当前 natural drain workload 的输入恢复流程存在 tokenizer 不匹配风险，建议修正请求生成流程，并重新执行关键实验。"
        direct_safe = False
    else:
        verdict_level = "C. 部分兼容"
        verdict_text = "不建议直接使用 Qwen2.5 tokenizer 解码 input_tok_ids。建议使用源 tokenizer 解码为文本，再交给 Qwen2.5 tokenizer 和 chat template。"
        direct_safe = False

    summary = {
        "provenance": {
            "trace": str(trace_path),
            "source_model": args.source_model,
            "target_model": args.target_model,
            "generation_script": args.generation_script,
            "generation_command": args.generation_command,
            "generator_behavior": "tokenizer(input_text, add_special_tokens=False); no chat template",
        },
        "trace_statistics": {
            "total_rows": len(rows),
            "rows_with_input_tok_ids": len(with_ids),
            "field_counts": dict(sorted(field_counts.items())),
            "has_original_text_or_conversation": trace_has_original_text,
            "input_length": {
                "min": min(lengths), "p50": percentile(lengths, 50),
                "p90": percentile(lengths, 90), "p95": percentile(lengths, 95), "max": max(lengths),
            },
        },
        "structural_comparison": structural,
        "hashes": hashes,
        "trace_token_id_mapping": {
            "unique_token_ids": len(unique_ids),
            "matching": matching,
            "different": len(mapping_diffs),
            "different_ratio": len(mapping_diffs) / len(unique_ids),
            "first_50_differences": mapping_diffs[:50],
        },
        "sample_audit": {
            "seed": args.seed, "requested": args.sample_size, "checked": sample_count,
            "decoded_text_exactly_equal": passed("decoded_text_exactly_equal"),
            "visible_text_exactly_equal": passed("visible_text_exactly_equal"),
            "original_ids_equal_source_reencoded": passed("original_ids_equal_source_reencoded"),
            "original_ids_equal_target_reencoded": passed("original_ids_equal_target_reencoded"),
            "source_reencoded_equal_target_reencoded": passed("source_reencoded_equal_target_reencoded"),
        },
        "chat_template_risk": {
            "templates_exactly_equal": templates_equal,
            "trace_contains_special_token_ids": trace_includes_chat_template,
            "special_token_ids_observed": special_ids_in_trace,
            "trace_generator_applied_chat_template": False,
            "trace_has_original_text_or_conversation": trace_has_original_text,
            "assessment": ("Trace was encoded as plain accumulated conversation text with add_special_tokens=False; "
                           "different tokenizer chat_template strings do not by themselves alter these stored ids."),
        },
        "verdict": {"level": verdict_level, "text": verdict_text, "direct_qwen25_decode_safe_for_this_trace": direct_safe},
    }
    (output / "audit_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    write_html(output, summary)
    logging.info("Audit complete: %s", verdict_level)
    print(json.dumps({"output_dir": str(output), "verdict": summary["verdict"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
