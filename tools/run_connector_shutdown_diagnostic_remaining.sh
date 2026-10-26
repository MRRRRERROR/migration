#!/usr/bin/env bash
set -u
project=/data/home/zylu/llumnix-v018-main
root=$project/results/connector_shutdown_repair_20261007_01
for method in api_sigterm project_tree; do
  name=diag_single_three_requests_$method
  status=0
  bash "$project/tools/run_connector_shutdown_cases.sh" --root "$root/$name" --instances 1 --requests 3 --stop "$method" > "$root/$name.console.log" 2>&1 || status=$?
  printf '%s\n' "$status" > "$root/$name.exitcode"
  test -f "$root/$name/result.json" || exit 1
done
