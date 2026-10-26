import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "all_experiment_raw_data.csv"
MANIFEST = ROOT / "all_experiment_raw_data_manifest.csv"

EXPERIMENTS = [
    {
        "method": "baseline_rr_no_migration",
        "experiment_family": "baseline",
        "experiment_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246",
        "group_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "rr_no_migration",
        "shared_files": [
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "env.txt",
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "run.log",
        ],
    },
    {
        "method": "baseline_load_no_migration",
        "experiment_family": "baseline",
        "experiment_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246",
        "group_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "load_no_migration",
        "shared_files": [
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "env.txt",
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "run.log",
        ],
    },
    {
        "method": "baseline_load_migration_defrag",
        "experiment_family": "baseline",
        "experiment_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246",
        "group_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "load_migration_defrag",
        "shared_files": [
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "env.txt",
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "run.log",
        ],
    },
    {
        "method": "llumnix_load_migration",
        "experiment_family": "llumnix",
        "experiment_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246",
        "group_dir": ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "llumnix_sim_migration",
        "shared_files": [
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "env.txt",
            ROOT / "enhanced_sim_opt-1.3b_20260701_212246" / "run.log",
        ],
    },
]

FIELDNAMES = [
    "method",
    "experiment_family",
    "experiment_dir",
    "source_file",
    "source_file_name",
    "source_file_type",
    "record_type",
    "source_row_number",
    "source_column_name",
    "source_column_index",
    "value",
    "raw_text",
]


def rel(path):
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def source_type(path):
    name = path.name.lower()
    if name == "env.txt":
        return "env"
    if name == "run.log":
        return "run_log"
    if name == "benchmark.log":
        return "benchmark_log"
    if name == "migration.log":
        return "migration_log"
    if name == "service.log":
        return "service_log"
    if name == "server_instance.csv":
        return "server_instance_csv"
    if name in {"ready.log", "ready_instances.tmp"}:
        return "ready_log"
    return path.suffix.lstrip(".").lower() or "unknown"


def write_missing(writer, exp, path):
    row = base_row(exp, path)
    row.update(
        {
            "record_type": "missing_file",
            "source_row_number": "",
            "source_column_name": "",
            "source_column_index": "",
            "value": "",
            "raw_text": "",
        }
    )
    writer.writerow(row)
    return 1


def base_row(exp, path):
    return {
        "method": exp["method"],
        "experiment_family": exp["experiment_family"],
        "experiment_dir": rel(exp["experiment_dir"]),
        "source_file": rel(path),
        "source_file_name": path.name,
        "source_file_type": source_type(path),
    }


def write_file_rows(writer, exp, path):
    if not path.exists():
        return write_missing(writer, exp, path)

    if path.suffix.lower() == ".csv":
        count = 0
        with path.open("r", encoding="utf-8", errors="replace", newline="") as file:
            reader = csv.reader(file)
            try:
                header = next(reader)
            except StopIteration:
                row = base_row(exp, path)
                row.update(
                    {
                        "record_type": "empty_csv",
                        "source_row_number": "",
                        "source_column_name": "",
                        "source_column_index": "",
                        "value": "",
                        "raw_text": "",
                    }
                )
                writer.writerow(row)
                return 1
            for row_number, values in enumerate(reader, start=2):
                raw_text = ",".join(values)
                for index in range(max(len(header), len(values))):
                    row = base_row(exp, path)
                    row.update(
                        {
                            "record_type": "csv_cell",
                            "source_row_number": row_number,
                            "source_column_name": header[index]
                            if index < len(header)
                            else f"extra_col_{index}",
                            "source_column_index": index,
                            "value": values[index] if index < len(values) else "",
                            "raw_text": raw_text,
                        }
                    )
                    writer.writerow(row)
                    count += 1
        return count

    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as file:
        for row_number, line in enumerate(file, start=1):
            text = line.rstrip("\n")
            row = base_row(exp, path)
            row.update(
                {
                    "record_type": "text_line",
                    "source_row_number": row_number,
                    "source_column_name": "raw_line",
                    "source_column_index": 0,
                    "value": text,
                    "raw_text": text,
                }
            )
            writer.writerow(row)
            count += 1
    if count:
        return count
    row = base_row(exp, path)
    row.update(
        {
            "record_type": "empty_text_file",
            "source_row_number": "",
            "source_column_name": "raw_line",
            "source_column_index": 0,
            "value": "",
            "raw_text": "",
        }
    )
    writer.writerow(row)
    return 1


def main():
    summary = []
    total = 0
    with OUTPUT.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        for exp in EXPERIMENTS:
            files = list(exp["shared_files"])
            for name in [
                "benchmark.log",
                "migration.log",
                "ready.log",
                "ready_instances.tmp",
                "server_instance.csv",
                "service.log",
            ]:
                files.append(exp["group_dir"] / name)
            seen = set()
            for path in files:
                if path in seen:
                    continue
                seen.add(path)
                count = write_file_rows(writer, exp, path)
                total += count
                summary.append((exp["method"], rel(path), count))

    with MANIFEST.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["method", "source_file", "rows_written_to_all_experiment_raw_data_csv"]
        )
        writer.writerows(summary)

    print(OUTPUT)
    print(MANIFEST)
    print(f"rows_without_header={total}")


if __name__ == "__main__":
    main()
