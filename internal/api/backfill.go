package api

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/mygithub/dms-hitachi-connector/internal/storage"
)

// BackfillStatus describes what BackfillPageCount did with one document.
type BackfillStatus string

const (
	BackfillRepaired     BackfillStatus = "repaired"       // page count re-read and saved
	BackfillWouldRepair  BackfillStatus = "would_repair"   // dry run: needs repair
	BackfillAlreadyOK    BackfillStatus = "already_ok"     // metadata already trusted
	BackfillNotPDF       BackfillStatus = "not_pdf"        // not a PDF, nothing to do
	BackfillFailed       BackfillStatus = "failed"         // see BackfillResult.Err
	BackfillMetaNotFound BackfillStatus = "meta_not_found" // metadata object missing
)

type BackfillResult struct {
	FileID   string
	Status   BackfillStatus
	OldCount string // page_count before the run, as stored
	NewCount int    // page count read from the PDF (repaired only)
	Err      error
}

// ListMetadataFileIDs calls fn with the file ID of every metadata object.
func (h *Handler) ListMetadataFileIDs(ctx context.Context, fn func(fileID string) error) error {
	metaDir := path.Join(h.cfg.InternalRoot, "meta")

	return h.client.List(ctx, metaDir, func(name string) error {
		base := path.Base(name)
		if !strings.HasSuffix(base, ".json") || path.Dir(name) != metaDir {
			return nil
		}
		fileID := strings.TrimSuffix(base, ".json")
		if fileID == "" {
			return nil
		}
		return fn(fileID)
	})
}

// BackfillPageCount fixes the stored page_count of one document using the
// same rules as the on-view repair in ensurePageCount. Documents that are
// already trusted are skipped, so the backfill can be re-run safely.
// With prewarm, every page is also rendered into the preview cache.
func (h *Handler) BackfillPageCount(ctx context.Context, fileID string, dryRun, prewarm bool) BackfillResult {
	res := BackfillResult{FileID: fileID}

	meta, err := h.readMeta(ctx, fileID)
	if err != nil {
		res.Err = err
		res.Status = BackfillFailed
		if storage.IsObjectNotFound(err) {
			res.Status = BackfillMetaNotFound
		}
		return res
	}
	res.OldCount = meta.Tags["page_count"]

	if pageCountMetadataTrusted(meta) {
		res.Status = BackfillAlreadyOK
		return res
	}
	if !metaLooksLikePDF(fileID, meta) {
		res.Status = BackfillNotPDF
		return res
	}
	if _, ok := h.readStoredPageCount(ctx, fileID); ok {
		res.Status = BackfillAlreadyOK
		return res
	}
	if dryRun {
		res.Status = BackfillWouldRepair
		return res
	}

	pageCount, pdfPath, tmpDir, err := h.recountPageCount(ctx, fileID, meta)
	if err != nil {
		res.Status = BackfillFailed
		res.Err = err
		return res
	}
	defer os.RemoveAll(tmpDir)

	res.Status = BackfillRepaired
	res.NewCount = pageCount

	if prewarm {
		if err := h.prewarmPDFPreviewCache(fileID, pdfPath, 1, pageCount, pageCount, firstPagePreviewDPI, true); err != nil {
			// The count is saved; missing previews are rendered on demand.
			res.Err = err
		}
	}

	return res
}
