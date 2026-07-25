package main

import "github.com/spf13/cobra"

func newStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show queue health: pending/running counts, throughput, latency",
		RunE: func(cmd *cobra.Command, args []string) error {
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/stats", nil, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
}
