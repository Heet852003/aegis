# Example: ETL pipeline workflow

A 5-step DAG (`extract` → `transform` & `validate` in parallel → `load` →
`notify`) that demonstrates dependency fan-out/fan-in, plus a demo worker in
each supported language implementing every job type the workflow uses.

```
extract ─┬─▶ transform ─┐
         └─▶ validate ──┴─▶ load ─▶ notify
```

## Run it

1. Start the server (from the repo root):

   ```bash
   go run ./cmd/aegisd
   ```

2. Start a worker — either language works standalone:

   ```bash
   go run ./examples/etl-pipeline/worker-go
   # or
   pip install -e sdk/python && python examples/etl-pipeline/worker-python/worker.py
   ```

3. Submit the workflow:

   ```bash
   go run ./cmd/aegis workflow submit examples/etl-pipeline/workflow.yaml
   ```

4. Open the dashboard at http://localhost:8080 and watch `transform` and
   `validate` run concurrently, then `load`, then `notify`, each step
   updating live as the worker reports back.
