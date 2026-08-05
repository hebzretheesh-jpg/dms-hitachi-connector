package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mygithub/dms-hitachi-connector/internal/api"
	"github.com/mygithub/dms-hitachi-connector/internal/config"
	"github.com/mygithub/dms-hitachi-connector/internal/metrics"
	"github.com/mygithub/dms-hitachi-connector/internal/server"
	"github.com/mygithub/dms-hitachi-connector/internal/storage"
	"github.com/mygithub/dms-hitachi-connector/internal/util"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	m := metrics.New()

	client, err := storage.NewClient(
		cfg.HitachiEndpoint,
		cfg.HitachiAccessKeyID,
		cfg.HitachiSecretAccessKey,
		cfg.HitachiRegion,
		cfg.HitachiBucket,
		cfg.InternalRoot,
		cfg.HitachiUsePathStyle,
		cfg.HTTPTimeout,
		cfg.DownloadCacheDir,
		cfg.DownloadCacheMaxFiles,
	)
	if err != nil {
		log.Fatalf("create s3 client: %v", err)
	}

	idGen, err := util.NewIDGenerator(cfg.WorkerID)
	if err != nil {
		log.Fatalf("create id generator: %v", err)
	}

	handler := api.New(cfg, client, idGen, m)
	srv := server.New(cfg.ListenAddr, handler, cfg.ReadTimeout, cfg.WriteTimeout, cfg.IdleTimeout)

	go func() {
		log.Printf("starting dms hitachi connector on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = srv.Shutdown(ctx)
}
