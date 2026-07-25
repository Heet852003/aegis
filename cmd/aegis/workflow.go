package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Heet852003/aegis/internal/models"
)

// yamlWorkflowSpec mirrors models.WorkflowSpec but keeps step payloads as
// plain `any` rather than json.RawMessage. yaml.v3 marshals []byte fields
// (which json.RawMessage is, underneath) as base64 strings, not as nested
// structure, so decoding YAML directly into WorkflowSpec would turn a payload
// map into a base64 blob. Decoding into `any` first and re-marshaling to
// JSON afterward gives the payload the structure workers actually expect.
type yamlWorkflowSpec struct {
	Name  string             `yaml:"name"`
	Steps []yamlWorkflowStep `yaml:"steps"`
}

type yamlWorkflowStep struct {
	Name        string   `yaml:"name"`
	Type        string   `yaml:"type"`
	Payload     any      `yaml:"payload"`
	DependsOn   []string `yaml:"depends_on"`
	MaxAttempts int      `yaml:"max_attempts"`
}

func (y yamlWorkflowSpec) toSpec() (models.WorkflowSpec, error) {
	spec := models.WorkflowSpec{Name: y.Name}
	for _, s := range y.Steps {
		payload, err := json.Marshal(s.Payload)
		if err != nil {
			return spec, fmt.Errorf("step %q: encoding payload: %w", s.Name, err)
		}
		spec.Steps = append(spec.Steps, models.WorkflowStepSpec{
			Name:        s.Name,
			Type:        s.Type,
			Payload:     payload,
			DependsOn:   s.DependsOn,
			MaxAttempts: s.MaxAttempts,
		})
	}
	return spec, nil
}

func newWorkflowCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "workflow", Short: "Submit and inspect DAG workflows"}
	cmd.AddCommand(newWorkflowSubmitCmd())
	cmd.AddCommand(newWorkflowGetCmd())
	cmd.AddCommand(newWorkflowListCmd())
	return cmd
}

func newWorkflowSubmitCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "submit <file.yaml>",
		Short:   "Submit a workflow defined in a YAML or JSON file",
		Example: `  aegis workflow submit ./examples/etl-pipeline/workflow.yaml`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var raw yamlWorkflowSpec
			if err := yaml.Unmarshal(data, &raw); err != nil {
				return fmt.Errorf("parsing %s: %w", args[0], err)
			}
			spec, err := raw.toSpec()
			if err != nil {
				return err
			}
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("POST", "/api/v1/workflows", nil, spec, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
}

func newWorkflowGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <workflow-id>",
		Short: "Show a workflow's steps and status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var out map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/workflows/"+args[0], nil, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
}

func newWorkflowListCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List workflows",
		RunE: func(cmd *cobra.Command, args []string) error {
			q := url.Values{}
			if limit > 0 {
				q.Set("limit", fmt.Sprint(limit))
			}
			var out []map[string]any
			c := newClient(serverAddr)
			if err := c.do("GET", "/api/v1/workflows", q, nil, &out); err != nil {
				return err
			}
			printJSON(out)
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, "max results")
	return cmd
}
