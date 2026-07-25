// Command worker-go is a demo worker implementing every job type used by
// examples/etl-pipeline/workflow.yaml. Run it alongside aegisd, then submit
// the workflow and watch it complete end-to-end in the dashboard:
//
//	go run ./examples/etl-pipeline/worker-go
//	aegis workflow submit examples/etl-pipeline/workflow.yaml
package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	aegis "github.com/Heet852003/aegis/sdk/go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	w := aegis.NewWorker("ws://localhost:8080/ws/worker", "etl-worker-go")
	w.Concurrency = 4

	w.Handle("extract_data", simulate("extracted rows"))
	w.Handle("transform_data", simulate("transformed rows"))
	w.Handle("validate_data", simulate("validated schema"))
	w.Handle("load_data", simulate("loaded into warehouse"))
	w.Handle("send_notification", simulate("notification sent"))

	log.Println("etl-worker-go connecting to", w.ServerAddr)
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// simulate returns a handler that pretends to do `label` work for a short,
// randomized duration and returns a small JSON result — standing in for
// whatever a real handler (an S3 read, a SQL transform, a Slack post...)
// would do.
func simulate(label string) aegis.Handler {
	return func(ctx context.Context, job *aegis.Job) (json.RawMessage, error) {
		log.Printf("[%s] %s: %s", job.Type, label, string(job.Payload))
		select {
		case <-time.After(time.Duration(300+rand.Intn(700)) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		result, _ := json.Marshal(map[string]any{"ok": true, "detail": label})
		return result, nil
	}
}
