package server

import (
	"context"
	"net/http"
	"time"
)

type Server struct {
	http *http.Server
}

func New(addr string, handler http.Handler, rt, wt, it time.Duration) *Server {
	return &Server{
		http: &http.Server{
			Addr:    addr,
			Handler: handler,
			// Headers must arrive quickly; the body may take much longer
			// (large uploads on slow links), so ReadTimeout is set separately.
			ReadHeaderTimeout: 30 * time.Second,
			ReadTimeout:       rt,
			WriteTimeout:      wt,
			IdleTimeout:       it,
		},
	}
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
