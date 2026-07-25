// Command aegis is the Aegis CLI: submit and inspect jobs and workflows,
// manage cron schedules, watch connected workers, and (via `aegis server`)
// run an embedded single-binary server for local trials.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var serverAddr string

func main() {
	root := &cobra.Command{
		Use:   "aegis",
		Short: "Aegis — a distributed job queue and workflow orchestration engine",
		Long: `Aegis is a self-hosted job queue and DAG workflow orchestrator.

This CLI talks to a running aegisd server over its REST API to submit and
inspect jobs and workflows, manage cron schedules, and watch connected
workers. Use "aegis server" to run a complete server (API + dashboard) from
a single binary for local development.`,
	}
	root.PersistentFlags().StringVar(&serverAddr, "server", envOr("AEGIS_SERVER", "http://localhost:8080"), "Aegis server base URL")

	root.AddCommand(newJobCmd())
	root.AddCommand(newWorkflowCmd())
	root.AddCommand(newCronCmd())
	root.AddCommand(newWorkerCmd())
	root.AddCommand(newStatsCmd())
	root.AddCommand(newServerCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}
