// Command repair-page-counts is a one-time backfill for documents migrated
// from the old DSS, whose metadata was stored with page_count = 1.
//
// For every metadata object it reads the real page count from the PDF and
// saves it, using the same rules as the connector's on-view repair. Documents
// that are already correct are skipped, so it is safe to stop (Ctrl+C) and
// run again; a re-run only processes what is still unrepaired.
//
// It reads the same environment variables as the connector (HITACHI_*,
// TEMP_DIR). pdfinfo (and pdftoppm for -prewarm) must be installed.
//
//	repair-page-counts -dry-run              # count what needs repair
//	repair-page-counts -workers 4            # repair everything
//	repair-page-counts -cid 123,456          # repair specific documents
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mygithub/dms-hitachi-connector/internal/api"
	"github.com/mygithub/dms-hitachi-connector/internal/config"
	"github.com/mygithub/dms-hitachi-connector/internal/metrics"
	"github.com/mygithub/dms-hitachi-connector/internal/storage"
)

func main() {
	var (
		workers      = flag.Int("workers", 4, "documents processed in parallel")
		dryRun       = flag.Bool("dry-run", false, "only report which documents need repair; download nothing, change nothing")
		prewarm      = flag.Bool("prewarm", false, "also render every page into the preview cache (slow; pages are otherwise rendered on first view)")
		cids         = flag.String("cid", "", "comma-separated document IDs to process instead of scanning all metadata")
		limit        = flag.Int("limit", 0, "stop after this many documents have been examined (0 = no limit)")
		failuresFile = flag.String("failures-file", "page-count-failures.txt", "file that receives the IDs of documents that failed")
		docTimeout   = flag.Duration("doc-timeout", 10*time.Minute, "time limit for one document")
	)
	flag.Parse()
	log.SetFlags(log.LstdFlags)

	if *workers < 1 {
		log.Fatal("-workers must be at least 1")
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// The download cache is disabled: every document is read once.
	client, err := storage.NewClient(
		cfg.HitachiEndpoint,
		cfg.HitachiAccessKeyID,
		cfg.HitachiSecretAccessKey,
		cfg.HitachiRegion,
		cfg.HitachiBucket,
		cfg.InternalRoot,
		cfg.HitachiUsePathStyle,
		cfg.HTTPTimeout,
		cfg.HitachiInsecureSkipVerify,
		"",
		0,
	)
	if err != nil {
		log.Fatalf("create s3 client: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := client.Health(ctx); err != nil {
		log.Fatalf("storage not reachable: %v", err)
	}

	h := api.New(cfg, client, nil, metrics.New())

	ids := make(chan string, *workers*2)
	results := make(chan api.BackfillResult, *workers*2)

	// Producer: explicit IDs or a scan of all metadata.
	var listErr error
	go func() {
		defer close(ids)
		examined := 0
		send := func(id string) error {
			if *limit > 0 && examined >= *limit {
				return errStop
			}
			examined++
			select {
			case ids <- id:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if *cids != "" {
			for _, id := range strings.Split(*cids, ",") {
				if id = strings.TrimSpace(id); id != "" {
					if send(id) != nil {
						return
					}
				}
			}
			return
		}

		if err := h.ListMetadataFileIDs(ctx, send); err != nil && err != errStop && ctx.Err() == nil {
			listErr = err
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				if ctx.Err() != nil {
					return
				}
				docCtx, cancel := context.WithTimeout(ctx, *docTimeout)
				results <- h.BackfillPageCount(docCtx, id, *dryRun, *prewarm)
				cancel()
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	counts := map[api.BackfillStatus]int{}
	var failed []string
	started := time.Now()
	total := 0

	for res := range results {
		total++
		counts[res.Status]++

		switch res.Status {
		case api.BackfillRepaired:
			msg := fmt.Sprintf("REPAIRED  cid=%s pages %s -> %d", res.FileID, res.OldCount, res.NewCount)
			if res.Err != nil {
				msg += fmt.Sprintf(" (preview prewarm incomplete: %v)", res.Err)
			}
			log.Print(msg)
		case api.BackfillWouldRepair:
			log.Printf("NEEDS FIX cid=%s page_count=%q", res.FileID, res.OldCount)
		case api.BackfillFailed, api.BackfillMetaNotFound:
			failed = append(failed, res.FileID)
			log.Printf("FAILED    cid=%s: %v", res.FileID, res.Err)
		}

		if total%100 == 0 {
			log.Printf("progress: %d examined in %s %s", total, time.Since(started).Round(time.Second), summary(counts))
		}
	}

	log.Printf("done: %d examined in %s %s", total, time.Since(started).Round(time.Second), summary(counts))

	if len(failed) > 0 && !*dryRun {
		if err := writeLines(*failuresFile, failed); err != nil {
			log.Printf("could not write %s: %v", *failuresFile, err)
		} else {
			log.Printf("%d failed document IDs written to %s (retry with -cid $(paste -sd, %s))",
				len(failed), *failuresFile, *failuresFile)
		}
	}

	switch {
	case listErr != nil:
		log.Fatalf("listing metadata failed: %v (re-run to continue; repaired documents are skipped)", listErr)
	case ctx.Err() != nil:
		log.Print("interrupted: re-run to continue; repaired documents are skipped")
		os.Exit(130)
	case len(failed) > 0:
		os.Exit(1)
	}
}

var errStop = fmt.Errorf("limit reached")

func summary(c map[api.BackfillStatus]int) string {
	return fmt.Sprintf("[repaired=%d would_repair=%d already_ok=%d not_pdf=%d failed=%d meta_not_found=%d]",
		c[api.BackfillRepaired], c[api.BackfillWouldRepair], c[api.BackfillAlreadyOK],
		c[api.BackfillNotPDF], c[api.BackfillFailed], c[api.BackfillMetaNotFound])
}

func writeLines(name string, lines []string) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
