#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://127.0.0.1:9082}"
TMP_FILE=$(mktemp /tmp/dms-smoke-XXXXXX.pdf)
cat > "$TMP_FILE" <<'PDF'
%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj
3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R>>endobj
4 0 obj<</Length 44>>stream
BT /F1 18 Tf 50 100 Td (hello hitachi) Tj ET
endstream endobj
trailer<</Root 1 0 R>>
%%EOF
PDF
UPLOAD_JSON=$(curl -sS -F "bin=@${TMP_FILE}" "${BASE_URL}/dss/api/put/test-tenant")
echo "$UPLOAD_JSON"
