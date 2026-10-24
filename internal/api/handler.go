package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mygithub/dms-hitachi-connector/internal/config"
	"github.com/mygithub/dms-hitachi-connector/internal/metrics"
	"github.com/mygithub/dms-hitachi-connector/internal/storage"
	"github.com/mygithub/dms-hitachi-connector/internal/util"
)

const firstPagePreviewDPI = 72

type Handler struct {
	cfg     config.Config
	client  *storage.Client
	idGen   *util.IDGenerator
	metrics *metrics.Registry
}

func New(cfg config.Config, client *storage.Client, idGen *util.IDGenerator, m *metrics.Registry) *Handler {
	return &Handler{cfg: cfg, client: client, idGen: idGen, metrics: m}
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	rw := &statusWriter{ResponseWriter: w, status: 200}
	endpoint := "unknown"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/":
		endpoint = "root"
		writeJSON(rw, http.StatusOK, map[string]any{"status": "ok", "endpoints": []string{"/dss/api/ping", "/metrics", "/healthz", "/readyz"}})

	case r.Method == http.MethodGet && r.URL.Path == "/dss/api/ping":
		endpoint = "ping"
		if !h.cfg.SkipStorageHealthCheck {
			if err := h.client.Health(r.Context()); err != nil {
				h.metrics.IncPingFailure()
				h.metrics.IncHitachiConnectionFail()
				writeJSON(rw, http.StatusServiceUnavailable, map[string]any{"status": "error", "message": err.Error()})
				break
			}
		}
		if err := h.idGen.HealthError(); err != nil {
			h.metrics.IncPingFailure()
			writeJSON(rw, http.StatusServiceUnavailable, map[string]any{"status": "error", "message": err.Error()})
		} else {
			h.metrics.IncPingSuccess()
			writeJSON(rw, http.StatusOK, map[string]any{"status": "ok"})
		}
	case r.Method == http.MethodGet && r.URL.Path == "/metrics":
		endpoint = "metrics"
		rw.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(rw, h.metrics.Render())
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		endpoint = "healthz"
		writeJSON(rw, http.StatusOK, map[string]any{"status": "ok"})
	case r.Method == http.MethodGet && r.URL.Path == "/readyz":
		endpoint = "readyz"
		checks := map[string]string{}
		ready := true
		if h.cfg.SkipStorageHealthCheck {
			checks["storage"] = "skipped_by_configuration"
		} else if err := h.client.Health(r.Context()); err != nil {
			checks["storage"] = "error"
			ready = false
		} else {
			checks["storage"] = "ok"
		}
		if err := h.idGen.HealthError(); err != nil {
			checks["id_generator"] = "error"
			ready = false
		} else {
			checks["id_generator"] = "ok"
		}
		if !ready {
			writeJSON(rw, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "checks": checks})
			break
		}
		writeJSON(rw, http.StatusOK, map[string]any{"status": "ready", "checks": checks})
	case matchMethods(r.Method, http.MethodGet, http.MethodPost, http.MethodPut) && strings.HasPrefix(r.URL.Path, "/dss/api/put/"):
		endpoint = "put"
		h.uploadBinaryData(rw, r, strings.TrimPrefix(r.URL.Path, "/dss/api/put/"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/dss/api/stats/"):
		endpoint = "stats"
		h.stats(rw, r, strings.TrimPrefix(r.URL.Path, "/dss/api/stats/"))

	// case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/dss/api/getimage/"):
	// 	endpoint = "getimage"
	// 	rest := strings.TrimPrefix(r.URL.Path, "/dss/api/getimage/")
	// 	p := strings.Split(rest, "/")
	// 	if len(p) != 2 {
	// 		writeJSON(rw, http.StatusBadRequest, map[string]any{"error": true, "message": "invalid getimage path"})
	// 	} else if page, err := strconv.Atoi(p[1]); err != nil {
	// 		writeJSON(rw, http.StatusBadRequest, map[string]any{"error": true, "message": "invalid page number"})
	// 	} else {
	// 		h.getContentPreview(rw, r, p[0], page)
	// 	}

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/dss/api/getimage/"):
		endpoint = "getimage"

		rest := strings.TrimPrefix(r.URL.Path, "/dss/api/getimage/")
		p := strings.Split(rest, "/")

		fileID := p[0]

		pageStr := r.URL.Query().Get("page")
		if pageStr == "" && len(p) > 1 {
			pageStr = strings.Split(p[1], "?")[0]
		}
		if pageStr == "" {
			pageStr = "1" // default fallback
		}

		page, err := strconv.Atoi(pageStr)
		if err != nil || page < 1 {
			writeJSON(rw, http.StatusBadRequest, map[string]any{
				"error": true, "message": "invalid page number",
			})
			break
		}

		h.getContentPreview(rw, r, fileID, page)

	case r.Method == http.MethodDelete && r.URL.Path == "/dss/api/delete":
		endpoint = "delete_all"
		h.deleteAll(rw, r)

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/dss/api/delete/"):
		endpoint = "delete"
		h.deleteFile(rw, r, strings.TrimPrefix(r.URL.Path, "/dss/api/delete/"))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/dss/api/get/"):
		endpoint = "get"
		h.getBinary(rw, r, strings.TrimPrefix(r.URL.Path, "/dss/api/get/"))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/dss/api/process/"):
		endpoint = "process"

		fileID := strings.TrimPrefix(r.URL.Path, "/dss/api/process/")
		h.processFile(rw, r, fileID)

	default:
		http.NotFound(rw, r)
	}
	h.metrics.Observe(endpoint, r.Method, rw.status, time.Since(start).Seconds())
}
func matchMethods(got string, methods ...string) bool {
	for _, m := range methods {
		if got == m {
			return true
		}
	}
	return false
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) { w.status = code; w.ResponseWriter.WriteHeader(code) }
func (h *Handler) uploadBinaryData(w http.ResponseWriter, r *http.Request, tenantID string) {
	var (
		src         io.Reader
		filename    string
		contentType string
	)

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		if err := r.ParseMultipartForm(h.cfg.MaxUploadBytes); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "The provided data does not match the expected format",
			})
			return
		}

		f, fh, err := r.FormFile("bin")
		if err != nil {
			// fallback: if client used a different field name,
			// pick the first file available
			if r.MultipartForm != nil {
				found := false

				for _, files := range r.MultipartForm.File {
					if len(files) == 0 {
						continue
					}

					fh = files[0]
					f, err = fh.Open()

					if err == nil {
						found = true
						break
					}
				}

				if !found {
					writeJSON(w, http.StatusBadRequest, map[string]any{
						"error": "The provided data does not match the expected format",
					})
					return
				}
			} else {
				writeJSON(w, http.StatusBadRequest, map[string]any{
					"error": "The provided data does not match the expected format",
				})
				return
			}
		}

		defer f.Close()

		src = f
		filename = fh.Filename
		contentType = fh.Header.Get("Content-Type")

	} else {

		src = r.Body
		defer r.Body.Close()

		filename = r.Header.Get("X-Original-Filename")

		if filename == "" {
			filename = r.Header.Get("X-Filename")
		}

		contentType = r.Header.Get("Content-Type")
	}

	// ============================================================
	// STAGE UPLOAD FILE
	// ============================================================

	localPath, size, err := h.stageUploadFile(src)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "The provided data does not match the expected format",
		})
		return
	}

	defer os.Remove(localPath)

	// ============================================================
	// PRESERVE OLD DSS CID DURING MIGRATION
	//
	// Migration service sends:
	//
	// X-Source-CID: 12345
	//
	// If this header is present, use the old DSS CID as the
	// new Hitachi file ID.
	//
	// If this header is not present, keep the existing behavior
	// and generate a new file ID.
	// ============================================================

	sourceCID := strings.TrimSpace(
		r.Header.Get("X-Source-CID"),
	)

	var fileID string

	if sourceCID != "" {

		// Validate that CID is numeric.
		if _, err := strconv.ParseInt(
			sourceCID,
			10,
			64,
		); err != nil {

			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error":     true,
				"errorCode": 1,
				"message":   "invalid X-Source-CID",
			})

			return
		}

		// IMPORTANT:
		// Preserve old DSS CID.
		fileID = sourceCID

		fmt.Printf(
			"Migration upload: preserving old DSS CID as Hitachi fileID=%s\n",
			fileID,
		)

	} else {

		// ========================================================
		// NORMAL EXISTING UPLOAD
		//
		// No old CID supplied.
		// Generate a new ID exactly as before.
		// ========================================================

		generatedFileID, err := h.idGen.Generate()
		if err != nil {

			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"error":     true,
				"errorCode": 1,
				"message":   err.Error(),
			})

			return
		}

		fileID = generatedFileID
	}

	// ============================================================
	// CONTENT TYPE
	// ============================================================

	if contentType == "" {

		contentType = detectContentTypeFromFile(localPath)

	} else if contentType == "application/x-www-form-urlencoded" ||
		contentType == "text/plain" {

		if detected := detectContentTypeFromFile(localPath); detected != "" {
			contentType = detected
		}
	}

	// ============================================================
	// FILE NAME
	// ============================================================

	if filename == "" {

		filename = fileID

		if ext := extensionFromContentType(contentType); ext != "" {
			filename += ext
		}
	}

	// Detect PDF page count while the uploaded file is staged locally.
	pageCount := 1
	if isPDFContent(contentType, filename, localPath) {
		if n, err := detectPDFPageCount(localPath); err == nil && n > 0 {
			pageCount = n
		}
	}
	//
	// Example:
	//
	// Old CID = 12345
	//
	// document/12345
	// ============================================================

	storagePath := h.documentPath(fileID)

	// ============================================================
	// UPLOAD FILE
	// ============================================================

	_, err = h.client.UploadFile(
		r.Context(),
		storagePath,
		"file",
		filename,
		localPath,
	)

	if err != nil {

		h.metrics.IncHitachiConnectionFail()
		h.metrics.IncHitachiUploadFail(
			h.bucketName(),
		)
		h.metrics.IncFileUploadFail(
			h.bucketName(),
			fileID,
		)

		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":     true,
			"errorCode": 2,
			"message":   err.Error(),
			"data": map[string]any{
				"cid": fileID,
			},
		})

		return
	}

	h.metrics.IncHitachiUploadSuccess(
		h.bucketName(),
	)

	// ============================================================
	// CREATE METADATA
	// ============================================================

	meta := storage.FileMeta{
		FileID:      fileID,
		TenantID:    tenantID,
		StoragePath: storagePath,
		Size:        size,
		ContentType: contentType,
		UploadedAt:  time.Now().UTC(),

		Tags: map[string]string{
			"tenant_id":         tenantID,
			"original_filename": filename,
			"page_count":        strconv.Itoa(pageCount),
		},
	}

	// ============================================================
	// SAVE METADATA
	//
	// Example:
	//
	// meta/12345.json
	// ============================================================

	if err := h.client.UploadJSON(
		r.Context(),
		h.metaPath(fileID),
		meta,
	); err != nil {

		h.metrics.IncHitachiConnectionFail()
		h.metrics.IncHitachiUploadFail(
			h.bucketName(),
		)
		h.metrics.IncFileUploadFail(
			h.bucketName(),
			fileID,
		)

		payload := map[string]any{
			"error":     true,
			"errorCode": 3,
			"message":   err.Error(),
			"data": map[string]any{
				"cid": fileID,
			},
		}

		if failedPath, moveErr := h.moveBinaryToFailed(
			r.Context(),
			fileID,
			filename,
			localPath,
		); moveErr == nil {

			payload["data"].(map[string]any)["failed_path"] = failedPath

		} else {

			payload["data"].(map[string]any)["move_failed_error"] =
				moveErr.Error()
		}

		writeJSON(
			w,
			http.StatusBadGateway,
			payload,
		)

		return
	}

	// ============================================================
	// PRE-WARM ALL PDF PAGE CACHES
	//
	// Generate low-DPI previews before returning the upload response.
	// This prevents DSS/Java from requesting pages before their cache
	// entries exist, which caused "NO IMAGE AVAILABLE" on first load.
	//
	// We deliberately use 72 DPI here to keep upload-time rendering fast.
	// getimage continues to generate/cache the requested higher-DPI version
	// when Java asks for a scale that is not already cached.
	// ============================================================
	if pageCount > 0 && isPDFContent(contentType, filename, localPath) {
		previewDPI := 72

		if err := h.prewarmPDFPreviewCache(
			fileID,
			localPath,
			pageCount,
			previewDPI,
		); err != nil {
			// Preview generation must not make a successful document upload
			// fail. getimage has a normal on-demand rendering fallback.
			fmt.Printf(
				"PDF preview prewarm failed for %s: %v\n",
				fileID,
				err,
			)
		}
	}

	// ============================================================
	// SUCCESS METRICS
	// ============================================================

	h.metrics.IncHitachiUploadSuccess(
		h.bucketName(),
	)

	h.metrics.IncFileUploadSuccess(
		h.bucketName(),
	)

	// ============================================================
	// RESPONSE
	//
	// Returns the SAME old CID when migration sends X-Source-CID.
	// ============================================================

	writeJSON(w, http.StatusOK, map[string]any{
		"error":     false,
		"errorCode": 0,
		"data": map[string]any{
			"cid": fileID,
		},
	})
}
func copyPreviewSource(sourcePath, tempDir, fileID string) (string, error) {
	src, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer src.Close()

	dst, err := os.CreateTemp(tempDir, "dms-preview-source-"+fileID+"-*")
	if err != nil {
		return "", err
	}

	dstPath := dst.Name()
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(dstPath)
		return "", err
	}
	if err := dst.Close(); err != nil {
		_ = os.Remove(dstPath)
		return "", err
	}

	return dstPath, nil
}

func (h *Handler) stageUploadFile(src io.Reader) (string, int64, error) {
	tmpFile, err := os.CreateTemp(h.cfg.TempDir, "dms-upload-*")
	if err != nil {
		return "", 0, err
	}

	size, copyErr := io.Copy(tmpFile, src)
	closeErr := tmpFile.Close()
	if copyErr != nil {
		_ = os.Remove(tmpFile.Name())
		return "", 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpFile.Name())
		return "", 0, closeErr
	}

	return tmpFile.Name(), size, nil
}

func detectContentTypeFromFile(localPath string) string {
	file, err := os.Open(localPath)
	if err != nil {
		return ""
	}
	defer file.Close()

	header := make([]byte, 512)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		return ""
	}

	return http.DetectContentType(header[:n])
}

func pageCountFromMeta(meta storage.FileMeta) string {
	if v := strings.TrimSpace(meta.Tags["page_count"]); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return strconv.Itoa(n)
		}
	}
	return "1"
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request, fileID string) {
	//_, err := h.readMeta(r.Context(), fileID)
	meta, err := h.readMeta(r.Context(), fileID)

	if err != nil {
		h.metrics.IncGetStatsFail(h.bucketName(), fileID)

		// Return 404 only when the storage error specifically
		// indicates that the object does not exist.
		if storage.IsObjectNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{
				"error":   true,
				"message": "The specified key does not exist.",
			})
			return
		}

		// Other storage errors should not be reported as missing CIDs.
		h.metrics.IncHitachiConnectionFail()

		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":   true,
			"message": "Storage request failed.",
		})
		return
	}

	h.metrics.IncGetStatsSuccess(h.bucketName())

	writeJSON(w, http.StatusOK, map[string]any{
		"error":     false,
		"errorCode": 0,
		"data": map[string]any{
			"cid":            fileID,
			"tenant_id":      meta.TenantID,
			"path":           meta.StoragePath,
			"deleted":        "false",
			"content_status": "PROCESSED",
			"mime_type":      meta.ContentType,
			"page_count":     pageCountFromMeta(meta),
			"name":           fileID + ".pdf",
		},
	})
}
func (h *Handler) getBinary(w http.ResponseWriter, r *http.Request, fileID string) {
	meta, err := h.readMeta(r.Context(), fileID)
	if err != nil {
		h.metrics.IncFileDownloadFail(h.bucketName(), fileID)
		if storage.IsObjectNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": true, "message": "The specified key does not exist."})
			return
		}
		h.metrics.IncHitachiConnectionFail()
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": true, "message": "Storage request failed."})
		return
	}
	resp, err := h.client.Download(r.Context(), meta.StoragePath)
	if err != nil {
		h.metrics.IncHitachiConnectionFail()
		h.metrics.IncHitachiGetStreamFail(h.bucketName(), meta.StoragePath)
		h.metrics.IncFileDownloadFail(h.bucketName(), fileID)
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": true, "message": err.Error()})
		return
	}
	defer resp.Body.Close()
	h.metrics.IncHitachiGetStreamSuccess(h.bucketName())
	h.metrics.IncFileDownloadSuccess(h.bucketName())
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
	}
	filename := fileID + extensionFromContentType(meta.ContentType)
	if o := meta.Tags["original_filename"]; o != "" {
		filename = o
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", filename))
	if meta.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	}
	_, _ = io.Copy(w, resp.Body)
}

// deleteFile deletes one CID and its related metadata/preview cache objects.
func (h *Handler) deleteFile(w http.ResponseWriter, r *http.Request, fileID string) {
	fileID = strings.TrimSuffix(strings.TrimSpace(fileID), "/")
	if fileID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": true, "errorCode": 1, "message": "Invalid file ID",
		})
		return
	}

	meta, err := h.readMeta(r.Context(), fileID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": true, "errorCode": 2, "message": "File not found",
		})
		return
	}

	if err := h.client.Delete(r.Context(), meta.StoragePath); err != nil {
		h.metrics.IncHitachiConnectionFail()
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": true, "errorCode": 3, "message": err.Error(),
			"data": map[string]any{"cid": fileID},
		})
		return
	}

	// Delete metadata. The document is already gone if this fails.
	if err := h.client.Delete(r.Context(), h.metaPath(fileID)); err != nil {
		h.metrics.IncHitachiConnectionFail()
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": true, "errorCode": 4,
			"message": "Document deleted but metadata deletion failed: " + err.Error(),
			"data":    map[string]any{"cid": fileID},
		})
		return
	}

	pageCount := 1
	if v := strings.TrimSpace(meta.Tags["page_count"]); v != "" {
		if n, parseErr := strconv.Atoi(v); parseErr == nil && n > 0 {
			pageCount = n
		}
	}

	// Preview caches are derived data. Missing cache objects are harmless.
	for page := 1; page <= pageCount; page++ {
		for dpi := 72; dpi <= 150; dpi++ {
			cachePath := h.previewCachePath(fileID, page, dpi)
			if err := h.client.Delete(r.Context(), cachePath); err != nil {
				fmt.Printf("delete preview cache skipped for %s page=%d dpi=%d: %v\n",
					fileID, page, dpi, err)
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"error":     false,
		"errorCode": 0,
		"data":      map[string]any{"cid": fileID, "deleted": true},
	})
}

// deleteAll is intentionally guarded because the current storage.Client API
// exposed to Handler provides object-level Delete but does not expose an
// object-listing/bulk-delete operation. Deleting an S3 prefix by passing the
// prefix to Delete would NOT recursively delete its objects.
//
// Once storage.Client exposes a safe List/DeleteAll operation, this endpoint
// can call it without changing the rest of the handler.
func (h *Handler) deleteAll(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	count, err := h.client.DeleteAll(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":     true,
			"errorCode": http.StatusInternalServerError,
			"message":   err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"error":     false,
		"errorCode": 0,
		"data": map[string]any{
			"deleted":       true,
			"objects_count": count,
		},
	})
}

func (h *Handler) processFile(w http.ResponseWriter, r *http.Request, fileID string) {

	// =======================
	// VALIDATION
	// =======================
	if fileID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":     true,
			"errorCode": 1,
			"message":   "Invalid file ID",
		})
		return
	}

	// =======================
	// CHECK FILE EXISTS
	// =======================
	_, err := h.readMeta(r.Context(), fileID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":     true,
			"errorCode": 2,
			"message":   "File not found",
		})
		return
	}

	// =======================
	// PROCESS (NOT IMPLEMENTED)
	// =======================

	writeJSON(w, http.StatusOK, map[string]any{
		"error":     false,
		"errorCode": 0,
		"data":      map[string]any{},
	})
}

func (h *Handler) getContentPreview(w http.ResponseWriter, r *http.Request, fileID string, page int) {

	// =======================
	// SCALE HANDLING
	// =======================
	scale := 1.0
	if v := r.URL.Query().Get("scale"); v != "" {
		if p, err := strconv.ParseFloat(v, 64); err == nil {
			scale = p
		}
	}
	if scale > 2 {
		scale = 2
	}
	if scale <= 0 {
		scale = 1
	}

	// =======================
	// READ META
	// =======================
	meta, err := h.readMeta(r.Context(), fileID)
	if err != nil {
		h.metrics.IncHitachiConnectionFail()
		h.metrics.IncPDFImageFail(h.bucketName(), fileID)
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": true, "message": err.Error(),
		})
		return
	}

	// =======================
	// NORMALIZE TYPE
	// =======================
	contentType := strings.ToLower(strings.TrimSpace(meta.ContentType))

	fileName := strings.ToLower(strings.TrimSpace(meta.Tags["original_filename"]))
	if fileName == "" {
		fileName = strings.ToLower(fileID + ".pdf") // fallback
	}
	fileExt := strings.ToLower(filepath.Ext(fileName))

	pageCount := 1
	if v := strings.TrimSpace(meta.Tags["page_count"]); v != "" {
		if n, parseErr := strconv.Atoi(v); parseErr == nil && n > 0 {
			pageCount = n
		}
	}

	// detect types
	isImage := strings.HasPrefix(contentType, "image/") ||
		fileExt == ".png" ||
		fileExt == ".jpg" ||
		fileExt == ".jpeg" ||
		fileExt == ".gif"

	isPDF := strings.Contains(contentType, "pdf") || fileExt == ".pdf"

	// fix wrong MIME for PDF uploads
	if !isPDF && (contentType == "application/octet-stream" || strings.Contains(contentType, "x-www-form-urlencoded") || strings.Contains(contentType, "text/plain")) && fileExt == ".pdf" {
		isPDF = true
		contentType = "application/pdf"
	}

	// =======================
	// IMAGE FLOW
	// =======================
	if isImage {
		resp, err := h.client.Download(r.Context(), meta.StoragePath)
		if err != nil {
			h.metrics.IncHitachiConnectionFail()
			h.metrics.IncHitachiGetStreamFail(h.bucketName(), meta.StoragePath)
			h.metrics.IncFileDownloadFail(h.bucketName(), fileID)

			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error": true, "message": err.Error(),
			})
			return
		}
		defer resp.Body.Close()

		h.metrics.IncHitachiGetStreamSuccess(h.bucketName())
		h.metrics.IncFileDownloadSuccess(h.bucketName())

		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Disposition",
			fmt.Sprintf("inline; filename=%q",
				fileID+extensionFromContentType(contentType)))

		if meta.Size > 0 {
			w.Header().Set("Content-Length",
				strconv.FormatInt(meta.Size, 10))
		}

		_, _ = io.Copy(w, resp.Body)
		return
	}

	// =======================
	// INVALID TYPE
	// =======================
	if !isPDF {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   true,
			"message": "preview only supported for PDF or image content",
		})
		return
	}

	if page > pageCount {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   true,
			"message": fmt.Sprintf("page %d out of range; document has %d page(s)", page, pageCount),
		})
		return
	}

	// =======================
	// CACHE CHECK
	// =======================
	dpi := int(float64(h.cfg.PDFFallbackDPI) * scale)

	if dpi < 72 {
		dpi = 72
	}

	if dpi > 150 {
		dpi = 150
	}

	cachePath := h.previewCachePath(fileID, page, dpi)

	if resp, err := h.client.Download(r.Context(), cachePath); err == nil {
		defer resp.Close()
		h.metrics.IncHitachiGetStreamSuccess(h.bucketName())
		h.metrics.IncPDFImageSuccess(h.bucketName())

		if size := resp.ContentLength; size > 0 {
			w.Header().Set("Content-Length",
				strconv.FormatInt(size, 10))
		}

		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Disposition",
			fmt.Sprintf("inline; filename=%q", path.Base(cachePath)))

		_, _ = io.Copy(w, resp.Body)
		return
	}

	// ============================================================
	// LOW-DPI PREWARM CACHE FALLBACK
	//
	// Upload prewarming stores every PDF page at 72 DPI. If DSS asks
	// for another scale before that scaled cache has been generated,
	// serve the already available 72-DPI page instead of returning
	// an empty/no-image result.
	// ============================================================
	if dpi != 72 {
		fallbackCachePath := h.previewCachePath(fileID, page, 72)

		if resp, err := h.client.Download(r.Context(), fallbackCachePath); err == nil {
			defer resp.Close()
			h.metrics.IncHitachiGetStreamSuccess(h.bucketName())
			h.metrics.IncPDFImageSuccess(h.bucketName())

			if size := resp.ContentLength; size > 0 {
				w.Header().Set("Content-Length",
					strconv.FormatInt(size, 10))
			}

			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("Content-Disposition",
				fmt.Sprintf("inline; filename=%q", path.Base(fallbackCachePath)))

			_, _ = io.Copy(w, resp.Body)
			return
		}
	}

	// ============================================================
	// FIRST-LOAD FALLBACK
	//
	// Upload pre-warms Page 1 at 72 DPI because that render is fast
	// enough to complete before the upload response is returned.
	// DSS may request a different scale/DPI (for example 140 DPI).
	// If that exact cache is not present yet, serve the guaranteed
	// 72-DPI Page-1 cache instead of making Java wait for another
	// PDF render. This prevents the first-load "NO IMAGE AVAILABLE".
	// ============================================================
	if page == 1 && dpi != firstPagePreviewDPI {
		baseCachePath := h.previewCachePath(
			fileID,
			1,
			firstPagePreviewDPI,
		)

		if resp, err := h.client.Download(r.Context(), baseCachePath); err == nil {
			defer resp.Close()
			h.metrics.IncHitachiGetStreamSuccess(h.bucketName())
			h.metrics.IncPDFImageSuccess(h.bucketName())

			if size := resp.ContentLength; size > 0 {
				w.Header().Set("Content-Length",
					strconv.FormatInt(size, 10))
			}

			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("Content-Disposition",
				fmt.Sprintf("inline; filename=%q", path.Base(baseCachePath)))

			_, _ = io.Copy(w, resp.Body)
			return
		}
	}

	// =======================
	// GENERATE PREVIEW
	// =======================
	tmpDir, err := os.MkdirTemp(h.cfg.TempDir, "dms-preview-")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": true, "message": err.Error(),
		})
		return
	}
	defer os.RemoveAll(tmpDir)

	pdfPath := filepath.Join(tmpDir, fileID+".pdf")

	if err := h.client.DownloadToFile(r.Context(), meta.StoragePath, pdfPath); err != nil {
		h.metrics.IncHitachiConnectionFail()
		h.metrics.IncHitachiGetStreamFail(h.bucketName(), meta.StoragePath)
		h.metrics.IncPDFImageFail(h.bucketName(), fileID)

		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": true, "message": err.Error(),
		})
		return
	}

	h.metrics.IncHitachiGetStreamSuccess(h.bucketName())

	pngBytes, err := h.generatePDFPreviewFromFile(pdfPath, fileID, page, dpi)
	if err != nil {
		h.metrics.IncPDFImageFail(h.bucketName(), fileID)

		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": true, "message": err.Error(),
		})
		return
	}

	h.metrics.IncPDFImageSuccess(h.bucketName())

	// Cache synchronously. If cache upload fails, still return the image.
	if _, cacheErr := h.client.UploadBytes(
		context.Background(),
		cachePath,
		"file",
		path.Base(cachePath),
		pngBytes,
	); cacheErr != nil {
		fmt.Printf(
			"preview cache upload failed for %s page=%d dpi=%d: %v\n",
			fileID,
			page,
			dpi,
			cacheErr,
		)
	}

	// response
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.Itoa(len(pngBytes)))
	w.Header().Set("Content-Disposition",
		fmt.Sprintf("inline; filename=%q", path.Base(cachePath)))

	_, _ = w.Write(pngBytes)
}

func (h *Handler) prewarmPDFPreviewCache(
	fileID string,
	localPDFPath string,
	pageCount int,
	dpi int,
) error {
	if pageCount < 1 {
		return nil
	}
	if dpi < 72 {
		dpi = 72
	}
	if dpi > 150 {
		dpi = 150
	}

	tmpDir, err := os.MkdirTemp(h.cfg.TempDir, "dms-preview-prewarm-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "preview")

	// Render every page in one pdftoppm invocation. This avoids starting
	// one Poppler process per page and is much faster for multi-page PDFs.
	previewCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(
		previewCtx,
		"pdftoppm",
		"-png",
		"-r", strconv.Itoa(dpi),
		localPDFPath,
		prefix,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return fmt.Errorf("pdftoppm prewarm failed: %s", msg)
		}
		return err
	}

	// pdftoppm writes preview-1.png, preview-2.png, ...
	// Upload each page using the same cache naming convention used by getimage.
	for page := 1; page <= pageCount; page++ {
		pngPath := fmt.Sprintf("%s-%d.png", prefix, page)

		pngBytes, err := os.ReadFile(pngPath)
		if err != nil {
			return fmt.Errorf(
				"read prewarmed page %d: %w",
				page,
				err,
			)
		}

		cachePath := h.previewCachePath(fileID, page, dpi)

		if _, err := h.client.UploadBytes(
			context.Background(),
			cachePath,
			"file",
			path.Base(cachePath),
			pngBytes,
		); err != nil {
			return fmt.Errorf(
				"upload prewarmed page %d: %w",
				page,
				err,
			)
		}
	}

	fmt.Printf(
		"PDF preview prewarm completed for %s: pages=%d dpi=%d\n",
		fileID,
		pageCount,
		dpi,
	)

	return nil
}

func (h *Handler) generatePDFPreviewFromFile(localPDFPath, fileID string, page, dpi int) ([]byte, error) {
	if page < 1 {
		return nil, fmt.Errorf("invalid PDF page: %d", page)
	}
	if dpi < 72 {
		dpi = 72
	}
	if dpi > 150 {
		dpi = 150
	}

	tmpDir, err := os.MkdirTemp(h.cfg.TempDir, "dms-preview-upload-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	prefix := filepath.Join(tmpDir, "preview")

	previewCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(
		previewCtx,
		"pdftoppm",
		"-png",
		"-f", strconv.Itoa(page),
		"-singlefile",
		"-r", strconv.Itoa(dpi),
		localPDFPath,
		prefix,
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("pdftoppm failed: %s", msg)
		}
		return nil, err
	}

	pngPath := prefix + ".png"
	pngBytes, err := os.ReadFile(pngPath)
	if err != nil {
		return nil, err
	}

	return pngBytes, nil
}

func isPDFContent(contentType, filename, localPath string) bool {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	name := strings.ToLower(strings.TrimSpace(filename))

	if strings.Contains(ct, "pdf") || strings.HasSuffix(name, ".pdf") {
		return true
	}

	file, err := os.Open(localPath)
	if err != nil {
		return false
	}
	defer file.Close()

	header := make([]byte, 5)
	n, err := io.ReadFull(file, header)
	return err == nil && n == 5 && string(header) == "%PDF-"
}

func detectPDFPageCount(localPath string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "pdfinfo", localPath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return 0, fmt.Errorf("pdfinfo failed: %s", msg)
		}
		return 0, err
	}

	for _, line := range strings.Split(stdout.String(), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 || !strings.EqualFold(strings.TrimSpace(parts[0]), "Pages") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || n < 1 {
			return 0, fmt.Errorf("invalid PDF page count")
		}
		return n, nil
	}

	return 0, fmt.Errorf("PDF page count not found")
}

func (h *Handler) readMeta(ctx context.Context, fileID string) (storage.FileMeta, error) {
	var meta storage.FileMeta
	if err := h.client.ReadJSON(ctx, h.metaPath(fileID), &meta); err != nil {
		return meta, err
	}
	return meta, nil
}

func (h *Handler) moveBinaryToFailed(ctx context.Context, fileID, fileName, localPath string) (string, error) {
	failedPath := h.failedBinaryPath(fileID, fileName)
	if _, err := h.client.UploadFile(ctx, failedPath, "file", path.Base(failedPath), localPath); err != nil {
		return "", fmt.Errorf("upload failed copy: %w", err)
	}
	if err := h.client.Delete(ctx, h.binaryPath(fileID)); err != nil {
		return "", fmt.Errorf("delete original after failed copy: %w", err)
	}
	return failedPath, nil
}

func (h *Handler) documentPath(fileID string) string {
	return path.Join(h.cfg.InternalRoot, "document", fileID)
}

func (h *Handler) failedBinaryPath(fileID, fileName string) string {
	name := fileID
	if trimmed := path.Base(strings.TrimSpace(fileName)); trimmed != "" && trimmed != "." && trimmed != "/" {
		name = fileID + "__" + trimmed
	}
	return path.Join(h.cfg.InternalRoot, "failed", name)
}

func (h *Handler) binaryPath(fileID string) string {
	return h.documentPath(fileID)
}

func (h *Handler) metaPath(fileID string) string {
	return path.Join(h.cfg.InternalRoot, "meta", fileID+".json")
}
func (h *Handler) previewCachePath(fileID string, page, dpi int) string {
	return path.Join(h.cfg.InternalRoot, "thumbnail", fmt.Sprintf("%s_page_%d_dpi_%d.png", fileID, page, dpi))
}
func (h *Handler) bucketName() string {
	return h.cfg.HitachiBucket
}
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
func extensionFromContentType(contentType string) string {
	if contentType == "" {
		return ""
	}
	exts, err := mime.ExtensionsByType(contentType)
	if err != nil || len(exts) == 0 {
		return ""
	}
	return exts[0]
}
