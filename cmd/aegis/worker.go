package main

import "github.com/spf13/cobra"

func newWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "worker", Short: "Inspect connected workers"}
	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List currently connected workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			var out []map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/workers", nil, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	})
	return cmd
}
