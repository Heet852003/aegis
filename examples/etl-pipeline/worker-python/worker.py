"""Demo worker implementing every job type used by ../workflow.yaml.

Run it alongside aegisd, then submit the workflow and watch it complete
end-to-end in the dashboard:

    pip install -e sdk/python
    python examples/etl-pipeline/worker-python/worker.py
    aegis workflow submit examples/etl-pipeline/workflow.yaml
"""

import asyncio
import logging
import random

from aegis_sdk import Job, Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

worker = Worker("ws://localhost:8080/ws/worker", name="etl-worker-python", concurrency=4)


async def simulate(label: str, job: Job) -> dict:
    logging.info("[%s] %s: %s", job.type, label, job.payload)
    await asyncio.sleep(random.uniform(0.3, 1.0))
    return {"ok": True, "detail": label}


@worker.handle("extract_data")
async def extract_data(job: Job):
    return await simulate("extracted rows", job)


@worker.handle("transform_data")
async def transform_data(job: Job):
    return await simulate("transformed rows", job)


@worker.handle("validate_data")
async def validate_data(job: Job):
    return await simulate("validated schema", job)


@worker.handle("load_data")
async def load_data(job: Job):
    return await simulate("loaded into warehouse", job)


@worker.handle("send_notification")
async def send_notification(job: Job):
    return await simulate("notification sent", job)


if __name__ == "__main__":
    logging.info("etl-worker-python connecting to %s", worker.server_addr)
    asyncio.run(worker.run())
