#!/usr/bin/env bash
set -euo pipefail
project=/data/home/zylu/llumnix-v018-main
root=$project/results/connector_shutdown_repair_20261007_01
stage=${1:-fix1}
for specification in '1 0 api_sigterm' '1 3 api_sigterm' '1 0 project_tree' '1 3 project_tree' '2 0 api_sigterm' '2 3 api_sigterm'; do
  read -r instances requests stop <<< "$specification"
  name="${stage}_${instances}instance_${requests}requests_$stop"
  bash "$project/tools/run_connector_shutdown_cases.sh" --root "$root/$name" --instances "$instances" --requests "$requests" --stop "$stop" > "$root/$name.console.log" 2>&1
done
