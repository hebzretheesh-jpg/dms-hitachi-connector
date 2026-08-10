#!/usr/bin/env bash
set -e

# This script starts only the connector binary.
# Configure HITACHI_ENDPOINT to point at your existing Hitachi-compatible storage.

echo "🔧 Setting environment variables..."
export HITACHI_ENDPOINT=http://127.0.0.1:9000
export HITACHI_ACCESS_KEY_ID=minioadmin
export HITACHI_SECRET_ACCESS_KEY=minioadmin
export HITACHI_BUCKET=dms-connector
export HITACHI_OBJECT_PREFIX=/connector/dms
export WORKER_ID=0

# export HITACHI_ENDPOINT=https://cditcmo.objstg-data.ksdc2.in:8000/
# export HITACHI_ACCESS_KEY_ID=Y2RpdGNtb191c2Vy
# export HITACHI_SECRET_ACCESS_KEY=995c72ed100640e69fe8f191c9709f5c
# export HITACHI_BUCKET=dms-connector
# export HITACHI_OBJECT_PREFIX=/connector/dms
# export WORKER_ID=0
export LISTEN_ADDR=":9081"   # 👈 IMPORTANT

echo "▶️ Starting Go server..."
./bin/dms-hitachi-connector
