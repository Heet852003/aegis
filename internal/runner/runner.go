// Package runner wires storage, the scheduling engine, the workflow
// coordinator, and the HTTP/WebSocket API into one running server. It's the
// shared implementation behind both the standalone aegisd binary and
// `aegis server start`, so the two never drift apart.
package runner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/Heet852003/aegis/internal/api"
	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/store"
	"github.com/Heet852003/aegis/internal/workflow"
	"github.com/Heet852003/aegis/web"
)

type Options struct {
	Driver      string // "sqlite" | "postgres"
	SQLitePath  string
	PostgresDSN string
	Addr        string
	NoDashboard bool
}

// Run blocks until ctx is cancelled, then shuts down the HTTP server
// gracefully.
func Run(ctx context.Context, opts Options) error {
	s, elector, err := buildStore(ctx, store.Driver(opts.Driver), opts.SQLitePath, opts.PostgresDSN)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	defer s.Close()

	if err := s.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	bus := engine.NewBus()
	eng := engine.NewEngine(s, bus, elector, engine.Config{})
	wf := workflow.NewCoordinator(eng)

	go eng.Start(ctx)
	wf.Start(ctx)

	var distFS fs.FS = web.DistFS()
	if opts.NoDashboard {
		distFS = nil
	}
	srv := api.NewServer(eng, wf, distFS)

	httpServer := &http.Server{Addr: opts.Addr, Handler: srv.Handler()}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("aegisd listening", "addr", opts.Addr, "driver", opts.Driver)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

func buildStore(ctx context.Context, driver store.Driver, sqlitePath, postgresDSN string) (store.Store, engine.LeaderElector, error) {
	switch driver {
	case store.DriverSQLite:
		s, err := store.NewSQLite(sqlitePath)
		if err != nil {
			return nil, nil, err
		}
		return s, engine.SingleNodeElector{}, nil
	case store.DriverPostgres:
		if postgresDSN == "" {
			return nil, nil, fmt.Errorf("postgres DSN is required when driver=postgres")
		}
		s, err := store.NewPostgres(ctx, postgresDSN)
		if err != nil {
			return nil, nil, err
		}
		return s, engine.PostgresElector{Store: s, LockKey: 84271}, nil
	default:
		return nil, nil, fmt.Errorf("unknown driver %q (want sqlite or postgres)", driver)
	}
}
