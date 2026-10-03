"""Shared scheduler state for application composition and operator routes.

Only main configures the scheduler and populates the registry. Cron and manual
triggers use the same callable, reservation dictionary and lock. Importing this
module does not initialize configuration, credentials, clients or workers.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone, tzinfo
from typing import Protocol, TypedDict


JobCallable = Callable[[], Awaitable[object]]


class RunningJob(TypedDict):
    run_id: str
    started_at: datetime


class ScheduledJob(Protocol):
    next_run_time: datetime | None
    trigger: object


class Scheduler(Protocol):
    timezone: tzinfo

    def add_job(self, func: JobCallable, **kwargs: object) -> object: ...
    def get_job(self, job_id: str) -> ScheduledJob | None: ...
    def remove_job(self, job_id: str) -> None: ...
    def reschedule_job(self, job_id: str, **kwargs: object) -> object: ...


_scheduler: Scheduler | None = None
JOB_REGISTRY: dict[str, JobCallable] = {}
running_jobs: dict[str, RunningJob] = {}
running_jobs_lock = asyncio.Lock()

JOB_CRON_MAP: dict[str, str] = {
    "CRON_RUN_SHAREPOINT_INDEX": "sharepoint_index",
    "CRON_RUN_SHAREPOINT_PURGE": "sharepoint_purge",
    "CRON_RUN_IMAGES_PURGE": "multimodality_images_purge",
    "CRON_RUN_BLOB_INDEX": "blob_index",
    "CRON_RUN_BLOB_PURGE": "blob_purge",
    "CRON_RUN_NL2SQL_INDEX": "nl2sql_index",
    "CRON_RUN_NL2SQL_PURGE": "nl2sql_purge",
}


def set_scheduler(scheduler: Scheduler) -> None:
    """Publish the application-composed scheduler; never create another one."""
    global _scheduler
    _scheduler = scheduler


def get_scheduler() -> Scheduler:
    if _scheduler is None:
        raise RuntimeError("The application scheduler has not been configured")
    return _scheduler


def track_running(job_id: str, func: JobCallable) -> JobCallable:
    """Preserve a manual reservation, or record a cron run, until completion."""
    async def wrapped() -> object:
        async with running_jobs_lock:
            if job_id not in running_jobs:
                running_jobs[job_id] = {
                    "run_id": job_id,
                    "started_at": datetime.now(tz=timezone.utc),
                }
        try:
            return await func()
        finally:
            async with running_jobs_lock:
                running_jobs.pop(job_id, None)

    wrapped.__name__ = getattr(func, "__name__", job_id)
    return wrapped
