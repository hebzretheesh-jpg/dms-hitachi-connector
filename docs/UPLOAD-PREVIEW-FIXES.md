# Large Upload and PDF Preview Fixes (November 2026)

This document records the issues found in the test deployment on `CMO-test1`, how they were diagnosed, and what was changed in the connector.

## Environment

- Tomcat 7 application `cmocmdrf` calls the connector directly at `http://127.0.0.1:9081/dss` (`WEB-INF/classes/server-config.xml`). There is no reverse proxy.
- The connector runs as the systemd service `hcp-connector` from `/opt/hcp-connector/bin/dms-hitachi-connector`, with its environment in `/opt/hcp-connector/env/connector.env`.
- Storage is the Hitachi HCP S3 endpoint `cditcmo.objstg-data.ksdc2.in`, bucket `CMOportal`, prefix `/connector/dms/test`.

## Issue 1: Uploads of about 1 MB or more failed

### Symptom

Uploading a larger file from the application failed with:

```
com.ospyn.dss.client.api.ClientException: Put failed  - Failed to connect to server
    at com.ospyn.dss.client.api.impl.DefaultClient.put(DefaultClient.java:165)
    at com.ospyn.olive.filestore.DSSHttpProcessor.put(DSSHttpProcessor.java:48)
    at com.ospyn.olive.filestore.DSSFileStore.executePost(DSSFileStore.java:336)
    ...
java.lang.RuntimeException: Problem invoking web script
    at com.ospyn.olive.filestore.DSSFileStore.put(DSSFileStore.java:311)
```

Small files and all downloads and previews worked. Uploading the same large file directly to the connector with `curl` worked.

### Diagnosis

A packet capture on the loopback interface (`tcpdump -i lo 'tcp port 9081'`) during a failed upload of an 869 KB, 7-page PDF showed:

```
18:03:47.745  Tomcat finishes sending the body; connector ACKs it
18:03:52.749  Tomcat sends FIN: gives up after exactly 5.0 s
18:03:56.502  Connector replies 200 {"cid":"375261139816153088"}; Tomcat answers RST
```

- **The DSS Java client (`com.ospyn.dss.client.api.impl.DefaultClient`) has a 5-second read timeout.** It reports that timeout as "Failed to connect to server".
- **The connector stored the file successfully but took 8.8 s to reply.** Before replying, it rendered *every* PDF page with `pdftoppm` and uploaded each PNG to HCP one at a time. Larger files usually have more pages, so the failure looked like a size limit.
- **Side effect:** every upload that "failed" this way is actually stored in HCP. The application never received the CID, so these objects are orphaned under `document/` and `meta/`.

### Fix

The upload handler (`uploadBinaryData` in `internal/api/handler.go`) now:

1. stores the document and its metadata,
2. renders **only page 1** at 72 DPI before replying, which keeps the earlier "NO IMAGE AVAILABLE on first load" fix working,
3. replies to the client,
4. renders pages 2 onwards in a background goroutine. The goroutine works on its own copy of the staged file (via `copyPreviewSource`), because the original temp file is removed when the handler returns.

Result: a 59 MB, 198-page PDF upload returned in **1.98 s**.

## Issue 2: Slow PDF preview loading

### Symptom

After the first fix, pages that hadn't been cached took 2–4 s each. For the 198-page PDF, the log showed:

```
PDF preview prewarm completed for 375267548062547968: pages=1-1 of 198 dpi=72
PDF preview prewarm failed for 375267548062547968: signal: killed
```

### Causes

1. **One `pdftoppm` run for the whole document, with a 60 s limit.** A 198-page PDF needed more than 60 s, so the run was killed and none of pages 2–198 were cached.
2. **No page was available until every page had been rendered.** Uploads only started after the whole render finished.
3. **Page-number padding bug.** `pdftoppm` zero-pads output names to the digit count of the page count (`preview-01.png` for a 12-page PDF). The code looked for `preview-1.png`, so pre-rendering *always* failed for PDFs with 10 or more pages.
4. **Each on-demand render copied the whole PDF.** `getimage` copied the full cached PDF into a temp directory before rendering one page. That was 57 MB of disk writes per page request.
5. **CPU contention.** On-demand renders competed with the background render, so a page someone was waiting for could take 6–7 s.

### Fixes

| Change | Where |
|---|---|
| Background pages render in batches: 2 pages first, then 10 at a time. Each batch has its own 60 s limit and is uploaded before the next starts. A failed batch is logged and skipped. | `prewarmPDFPreviewCache`, `prewarmPDFPreviewBatch` in `internal/api/handler.go` |
| Output file names are resolved with zero-padding, falling back to unpadded names. | `prewarmPDFPreviewBatch` |
| The background `pdftoppm` runs under `nice -n 10` (if `nice` is available), so on-demand renders get the CPU first. | `prewarmPDFPreviewBatch` |
| `DownloadToFile` hard-links the cached PDF instead of copying it, and falls back to a copy (e.g. if the cache and temp directories are on different filesystems). Callers must treat the file as read-only. | `DownloadToFile` in `internal/storage/client.go` |

Results after the batching and hard-link changes, for a 198-page PDF:
- Pages 4–56 read in order loaded in **26–110 ms** while the background render was still running.
- The full background render finished in about 2 min 10 s with no failed batches.
- Every page loaded in **35–140 ms** after that.

The later change (first batch of 2 pages, `nice`) targets the remaining slow cases. Pages 2–3 took about 4 s if opened right after upload, and on-demand renders took 6–7 s while the background render was running.

## Issue 3: Migrated PDFs preview only page 1

### Symptom

PDFs migrated into HCP by the earlier connector previewed only page 1. For example, a 22-page document showed 1 page in the viewer, but downloading it gave all 22 pages.

### Cause

The migrated metadata (`meta/{cid}.json`) has `tags.page_count = "1"`. The upload code saves a default of 1 whenever `pdfinfo` fails, for example when `poppler-utils` was not installed on the machine that ran the migration. As a result:
- `stats` returns `page_count: 1`, so the viewer shows one page.
- `getimage` rejects page 2 onwards with `400 "page N out of range"`.

Downloads don't use the page count, so they were unaffected.

Example (CID `1968554725778952192`, a 22-page PDF):

```
stats   -> {"mime_type":"application/pdf","page_count":"1",...}
getimage/1968554725778952192/2 -> HTTP 400
```

### Fix

Metadata now carries a `page_count_verified = "true"` tag when the count was read from the PDF itself. New uploads set it when `pdfinfo` succeeds. If `pdfinfo` fails during an upload, the connector logs `PDF page count detection failed for <filename>: <error>`.

`ensurePageCount` (in `internal/api/handler.go`) runs on `stats` and `getimage`. It trusts a `page_count` above 1. When the value is missing, invalid, or `1` without the connector's recheck marker, it:

1. downloads the PDF once (through the local download cache) and counts its pages with `pdfinfo`,
2. writes `tags.page_count`, `tags.page_count_verified`, and `tags.page_count_repaired` back to the metadata object. The separate recheck marker ensures old migrated metadata with a stale verified tag and a fallback count of 1 is recounted once. The object is edited as raw JSON, so any other fields written by the migration connector are kept.
3. returns the correct count in the same response,
4. pre-renders every page in the background, the same way new uploads are handled.

Concurrent `stats` and `getimage` requests for the same document share one repair. Each document is repaired once, the first time it is opened. After that the metadata is correct and nothing extra happens. Genuine single-page PDFs are checked once and then marked as verified.

For `stats` and `getimage`, a connector now handles the request locally when its metadata exists instead of accepting an early HTTP 200 from a peer. This ensures the page-count repair runs on the receiving connector; peer retrieval remains available when local metadata is absent.

If a repair fails for a reason other than the client giving up (for example, a damaged PDF or `pdfinfo` missing), the connector throttles retries for five minutes. This avoids downloading the same file on every view while allowing transient storage or network failures to recover without a restart.

**Prerequisite:** `pdfinfo` and `pdftoppm` (package `poppler-utils`) must be installed on **every** server running the connector. The connectors forward `stats` and `getimage` requests to their peers (`DSS_SERVER_*`), so whichever connector answers must have the new binary and the poppler tools.

A limit of `maxBackgroundPrewarms` (2) background pre-render jobs run at a time, for both new uploads and repaired documents. Further jobs wait for a free slot.

Log lines:

```
page count repaired for <cid>: pages=22
PDF preview prewarm completed for <cid>: pages=1-22 of 22 dpi=72
page count repair failed for <cid>: <error>
```

The first `stats` call for an unrepaired document includes the time taken to download it from HCP. For very large PDFs, that call could go past the DSS Java client's 5 s timeout. If it does, the repair is retried on the next request.

## Logging added

Every request now logs a start and an end line:

```
request start: POST /dss/api/put/20339 remote=127.0.0.1:42720 content-type="multipart/form-data; ..." content-length=59337764 transfer-encoding=[] expect=""
request end: POST /dss/api/put/20339 endpoint=put status=200 duration=1.982270299s
```

Pre-render progress:

```
PDF preview prewarm completed for <cid>: pages=1-1 of 198 dpi=72
PDF preview prewarm completed for <cid>: pages=2-198 of 198 dpi=72
PDF preview prewarm batch failed for <cid>: pages=<a>-<b>: <error>
```

A useful filter:

```bash
journalctl -u hcp-connector -f | grep -E 'prewarm|put.*duration|getimage.*duration'
```

## Building without Go on the server

The server has no Go toolchain. Cross-compile a static Linux binary on any machine that has Go 1.23+:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/dms-hitachi-connector ./cmd/server
```

Deploy:

```bash
sudo systemctl stop hcp-connector
sudo cp /opt/hcp-connector/bin/dms-hitachi-connector /opt/hcp-connector/bin/dms-hitachi-connector.bak
sudo install -m 755 /tmp/dms-hitachi-connector /opt/hcp-connector/bin/dms-hitachi-connector
sudo systemctl start hcp-connector
```

Roll back:

```bash
sudo systemctl stop hcp-connector
sudo mv /opt/hcp-connector/bin/dms-hitachi-connector.bak /opt/hcp-connector/bin/dms-hitachi-connector
sudo systemctl start hcp-connector
```

## Open items

1. **The DSS Java client's 5 s read timeout.** The upload reply still waits for the full file to reach HCP. A very large file, or a slow HCP response, can still go past 5 s. The owner of `com.ospyn.dss.client` should raise this timeout (60 s or more is suggested).
2. **Orphaned objects from failed uploads.** Uploads that failed before this fix are stored in HCP, but the application has no reference to them. They can be found by comparing `meta/*.json` under `/connector/dms/test` with the CIDs stored in the application database, then deleted.
3. **Documents uploaded before the fix** may be missing pre-rendered pages. PDFs with 10 or more pages had none, because of the padding bug. PDFs uploaded with the first fix but before batching may have only page 1. Each missing page is rendered and cached the first time it is viewed. Re-uploading a document pre-renders all its pages. Migrated PDFs without `page_count` are repaired and pre-rendered automatically when first opened (Issue 3).
4. **Pages opened before their batch is ready** are still rendered on demand (about 1–2 s for large PDFs).
5. **`minio-go v7.0.0`** is from 2020. Upgrading it is recommended but was not needed for these fixes.
