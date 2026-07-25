# aegis-worker-sdk

Python worker SDK for [Aegis](https://github.com/Heet852003/aegis) — connect to
an Aegis server over its WebSocket dispatch protocol, register handlers by
job type, and let the SDK deal with registration, concurrency, heartbeats,
retries-via-failure-reporting, and reconnect-with-backoff.

## Install

```bash
pip install -e sdk/python
```

## Usage

```python
import asyncio
from aegis_sdk import Worker, Job

worker = Worker("ws://localhost:8080/ws/worker", name="python-workers", concurrency=4)

@worker.handle("resize_image")
async def resize_image(job: Job):
    print("resizing", job.payload)
    return {"ok": True}

@worker.handle("send_email")
def send_email(job: Job):
    # sync handlers work too — the SDK awaits them via a thread if needed
    print("sending email to", job.payload.get("to"))
    return {"sent": True}

asyncio.run(worker.run())
```

Raising an exception inside a handler fails the job; Aegis retries it with
exponential backoff up to the job's `max_attempts` before moving it to the
dead-letter queue. Handlers don't need their own retry logic.
