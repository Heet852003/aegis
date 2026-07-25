// Command aegisd runs the Aegis server: the scheduling engine, the workflow
// coordinator, the REST/WebSocket API, and (unless disabled) the embedded
// dashboard, all in one process.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Heet852003/aegis/internal/runner"
)

func main() {
	var (
		driver      = flag.String("driver", envOr("AEGIS_DRIVER", "sqlite"), "storage driver: sqlite | postgres")
		sqlitePath  = flag.String("sqlite-path", envOr("AEGIS_SQLITE_PATH", "aegis.db"), "path to the SQLite database file (sqlite driver only)")
		postgresDSN = flag.String("postgres-dsn", envOr("AEGIS_POSTGRES_DSN", ""), "PostgreSQL connection string (postgres driver only)")
		addr        = flag.String("addr", envOr("AEGIS_ADDR", ":8080"), "HTTP listen address")
		noDashboard = flag.Bool("no-dashboard", false, "disable serving the embedded dashboard")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := runner.Run(ctx, runner.Options{
		Driver:      *driver,
		SQLitePath:  *sqlitePath,
		PostgresDSN: *postgresDSN,
		Addr:        *addr,
		NoDashboard: *noDashboard,
	})
	if err != nil {
		slog.Error("aegisd exited with error", "error", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
