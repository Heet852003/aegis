package main

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "job", Short: "Submit and inspect jobs"}
	cmd.AddCommand(newJobSubmitCmd())
	cmd.AddCommand(newJobGetCmd())
	cmd.AddCommand(newJobListCmd())
	cmd.AddCommand(newJobCancelCmd())
	cmd.AddCommand(newJobRequeueCmd())
	return cmd
}

func newJobSubmitCmd() *cobra.Command {
	var (
		jobType     string
		payload     string
		queue       string
		priority    int
		maxAttempts int
		delaySec    int
	)
	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a new job",
		Example: `  aegis job submit --type send_email --payload '{"to":"a@b.com"}'
  aegis job submit --type resize_image --payload '{"url":"..."}' --priority 5 --max-attempts 5`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var raw json.RawMessage
			if payload != "" {
				if !json.Valid([]byte(payload)) {
					return fmt.Errorf("--payload is not valid JSON")
				}
				raw = json.RawMessage(payload)
			}
			body := map[string]any{
				"type":          jobType,
				"payload":       raw,
				"queue":         queue,
				"priority":      priority,
				"max_attempts":  maxAttempts,
				"delay_seconds": delaySec,
			}
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("POST", "/api/v1/jobs", nil, body, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
	cmd.Flags().StringVar(&jobType, "type", "", "job type/handler name (required)")
	cmd.Flags().StringVar(&payload, "payload", "{}", "JSON payload passed to the worker handler")
	cmd.Flags().StringVar(&queue, "queue", "default", "queue name")
	cmd.Flags().IntVar(&priority, "priority", 0, "higher runs first")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 3, "max attempts before dead-lettering")
	cmd.Flags().IntVar(&delaySec, "delay", 0, "delay in seconds before the job becomes runnable")
	cmd.MarkFlagRequired("type")
	return cmd
}

func newJobGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <job-id>",
		Short: "Show a job's current state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/jobs/"+args[0], nil, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
}

func newJobListCmd() *cobra.Command {
	var status, queue, jobType string
	var limit int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			q := url.Values{}
			if status != "" {
				q.Set("status", status)
			}
			if queue != "" {
				q.Set("queue", queue)
			}
			if jobType != "" {
				q.Set("type", jobType)
			}
			if limit > 0 {
				q.Set("limit", fmt.Sprint(limit))
			}
			var out []map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/jobs", q, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
	cmd.Flags().StringVar(&status, "status", "", "filter by status (pending, running, succeeded, failed, dead_letter, cancelled)")
	cmd.Flags().StringVar(&queue, "queue", "", "filter by queue")
	cmd.Flags().StringVar(&jobType, "type", "", "filter by job type")
	cmd.Flags().IntVar(&limit, "limit", 50, "max results")
	return cmd
}

func newJobCancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <job-id>",
		Short: "Cancel a pending job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient(serverAddr)
			if err := c.do("POST", "/api/v1/jobs/"+args[0]+"/cancel", nil, nil, nil); err != nil {
				return err
			}
			fmt.Println("cancelled", args[0])
			return nil
		},
	}
}

func newJobRequeueCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "requeue <job-id>",
		Short: "Requeue a dead-lettered job (resets attempts to 0)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient(serverAddr)
			if err := c.do("POST", "/api/v1/jobs/"+args[0]+"/requeue", nil, nil, nil); err != nil {
				return err
			}
			fmt.Println("requeued", args[0])
			return nil
		},
	}
}
