#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

if ! command -v java >/dev/null 2>&1; then
  echo "Java 17+ is required for PySpark. Install a JDK and retry." >&2
  exit 1
fi

PYTHON="${PYTHON:-python3}"
"$PYTHON" -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

# Optional DT=YYYY-MM-DD or first CLI arg --dt / bare date
DT_ARG=()
if [[ -n "${DT:-}" ]]; then
  DT_ARG=(--dt "$DT")
elif [[ "${1:-}" == "--dt" && -n "${2:-}" ]]; then
  DT_ARG=(--dt "$2")
elif [[ "${1:-}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  DT_ARG=(--dt "$1")
fi

python jobs/pipeline.py "${DT_ARG[@]}"
