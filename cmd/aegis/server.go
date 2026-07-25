package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/Heet852003/aegis/internal/runner"
)

func newServerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "server", Short: "Run an Aegis server from this single binary"}
	cmd.AddCommand(newServerStartCmd())
	return cmd
}

func newServerStartCmd() *cobra.Command {
	var (
		driver      string
		sqlitePath  string
		postgresDSN string
		addr        string
		noDashboard bool
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the Aegis server (API + dashboard) in the foreground",
		Example: `  aegis server start
  aegis server start --driver postgres --postgres-dsn "postgres://user:pass@localhost/aegis"`,
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return runner.Run(ctx, runner.Options{
				Driver:      driver,
				SQLitePath:  sqlitePath,
				PostgresDSN: postgresDSN,
				Addr:        addr,
				NoDashboard: noDashboard,
			})
		},
	}
	cmd.Flags().StringVar(&driver, "driver", "sqlite", "storage driver: sqlite | postgres")
	cmd.Flags().StringVar(&sqlitePath, "sqlite-path", "aegis.db", "path to the SQLite database file")
	cmd.Flags().StringVar(&postgresDSN, "postgres-dsn", "", "PostgreSQL connection string")
	cmd.Flags().StringVar(&addr, "addr", ":8080", "HTTP listen address")
	cmd.Flags().BoolVar(&noDashboard, "no-dashboard", false, "disable serving the embedded dashboard")
	return cmd
}
