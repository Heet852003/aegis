package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

func newCronCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "cron", Short: "Manage recurring (cron) job schedules"}
	cmd.AddCommand(newCronCreateCmd())
	cmd.AddCommand(newCronListCmd())
	return cmd
}

func newCronCreateCmd() *cobra.Command {
	var (
		name        string
		expr        string
		jobType     string
		payload     string
		queue       string
		maxAttempts int
	)
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create or update a cron schedule",
		Example: `  aegis cron create --name nightly-report --expr "0 2 * * *" --type generate_report --payload '{}'`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var raw json.RawMessage
			if payload != "" {
				if !json.Valid([]byte(payload)) {
					return fmt.Errorf("--payload is not valid JSON")
				}
				raw = json.RawMessage(payload)
			}
			body := map[string]any{
				"name":         name,
				"expression":   expr,
				"job_type":     jobType,
				"payload":      raw,
				"queue":        queue,
				"max_attempts": maxAttempts,
			}
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("POST", "/api/v1/cron", nil, body, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "schedule name (required)")
	cmd.Flags().StringVar(&expr, "expr", "", "cron expression, e.g. \"*/5 * * * *\" (required)")
	cmd.Flags().StringVar(&jobType, "type", "", "job type to enqueue on each fire (required)")
	cmd.Flags().StringVar(&payload, "payload", "{}", "JSON payload for each fired job")
	cmd.Flags().StringVar(&queue, "queue", "default", "queue name")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 3, "max attempts before dead-lettering")
	cmd.MarkFlagRequired("name")
	cmd.MarkFlagRequired("expr")
	cmd.MarkFlagRequired("type")
	return cmd
}

func newCronListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List cron schedules",
		RunE: func(cmd *cobra.Command, args []string) error {
			var out []map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/cron", nil, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
}
