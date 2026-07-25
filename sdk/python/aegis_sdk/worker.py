"""Python worker SDK for Aegis.

Connects to an Aegis server's WebSocket dispatch endpoint, registers itself
with the job types it can handle, and executes jobs pushed to it — handling
concurrency limiting, periodic heartbeats for long-running jobs, structured
completion/failure reporting, and automatic reconnect with backoff.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Union

import websockets

logger = logging.getLogger("aegis_sdk")

Handler = Callable[["Job"], Union[Optional[dict], Awaitable[Optional[dict]]]]


@dataclass
class Job:
    """A unit of work dispatched to this worker."""

    id: str
    type: str
    payload: Any
    attempts: int = 0
    max_attempts: int = 3
    priority: int = 0
    queue: str = "default"
    workflow_id: Optional[str] = None
    workflow_step: Optional[str] = None
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_wire(cls, data: dict) -> "Job":
        return cls(
            id=data["id"],
            type=data["type"],
            payload=data.get("payload"),
            attempts=data.get("attempts", 0),
            max_attempts=data.get("max_attempts", 3),
            priority=data.get("priority", 0),
            queue=data.get("queue", "default"),
            workflow_id=data.get("workflow_id") or None,
            workflow_step=data.get("workflow_step") or None,
            raw=data,
        )


class Worker:
    """Connects to an Aegis server and executes jobs using registered handlers."""

    def __init__(
        self,
        server_addr: str,
        name: str,
        queues: Optional[list[str]] = None,
        concurrency: int = 4,
        heartbeat_every: float = 15.0,
    ):
        self.server_addr = server_addr
        self.name = name
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.heartbeat_every = heartbeat_every
        self._handlers: dict[str, Handler] = {}

    def handle(self, job_type: str):
        """Decorator registering a handler for `job_type`.

        The handler may be sync or async, and may return a JSON-serializable
        dict (or None) as the job result. Raising fails the job; Aegis
        retries with backoff up to max_attempts before dead-lettering it.
        """

        def decorator(fn: Handler) -> Handler:
            self._handlers[job_type] = fn
            return fn

        return decorator

    async def run(self) -> None:
        """Connect and process jobs forever, reconnecting with backoff on
        any connection failure. Runs until cancelled."""
        backoff = 1.0
        while True:
            try:
                await self._run_once()
                backoff = 1.0
            except (websockets.exceptions.ConnectionClosed, OSError) as exc:
                logger.warning("aegis worker disconnected (%s), retrying in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _run_once(self) -> None:
        async with websockets.connect(self.server_addr, max_size=8 * 1024 * 1024) as ws:
            send_lock = asyncio.Lock()

            async def send(msg: dict) -> None:
                async with send_lock:
                    await ws.send(json.dumps(msg))

            await send(
                {
                    "type": "register",
                    "name": self.name,
                    "queues": self.queues,
                    "job_types": list(self._handlers.keys()),
                    "concurrency": self.concurrency,
                }
            )

            active: dict[str, bool] = {}
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(send, active))
            try:
                await send({"type": "request", "count": self.concurrency})
                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type")

                    if msg_type == "job":
                        job = Job.from_wire(msg["job"])
                        active[job.id] = True
                        asyncio.create_task(self._handle_job(send, job, active))
                    elif msg_type == "registered":
                        logger.info("aegis worker registered: worker_id=%s name=%s", msg.get("worker_id"), self.name)
                    elif msg_type == "error":
                        logger.error("aegis server error: %s", msg.get("message"))
            finally:
                heartbeat_task.cancel()

    async def _heartbeat_loop(self, send, active: dict[str, bool]) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_every)
                for job_id in list(active.keys()):
                    await send({"type": "heartbeat", "job_id": job_id})
        except asyncio.CancelledError:
            pass

    async def _handle_job(self, send, job: Job, active: dict[str, bool]) -> None:
        handler = self._handlers.get(job.type)
        try:
            if handler is None:
                raise RuntimeError(f"no handler registered for job type {job.type!r}")
            result = handler(job)
            if inspect.isawaitable(result):
                result = await result
            await send({"type": "complete", "job_id": job.id, "result": result or {}})
        except Exception as exc:  # noqa: BLE001 - reporting the failure to the server is the point
            logger.exception("job %s (%s) failed", job.id, job.type)
            await send({"type": "fail", "job_id": job.id, "error": str(exc)})
        finally:
            active.pop(job.id, None)
            await send({"type": "request", "count": 1})
