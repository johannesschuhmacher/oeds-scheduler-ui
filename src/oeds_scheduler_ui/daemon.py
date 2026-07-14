"""Daemon loop wrapper for the modular scheduler application."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime
from threading import Event

from oeds_scheduler_ui.application import SchedulerApplication
from oeds_scheduler_ui.runtime import CrawlerRunResult


class SchedulerDaemon:
    """Repeatedly tick a scheduler application until stopped."""

    def __init__(
        self,
        application: SchedulerApplication,
        *,
        poll_seconds: float = 60.0,
        now_func: Callable[[], datetime] = datetime.now,
        sleep_func: Callable[[float], None] = time.sleep,
        stop_event: Event | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.application = application
        self.poll_seconds = poll_seconds
        self._now_func = now_func
        self._sleep_func = sleep_func
        self._stop_event = stop_event or Event()

    @property
    def stopped(self) -> bool:
        return self._stop_event.is_set()

    def stop(self) -> None:
        self._stop_event.set()

    def run_once(self, now: datetime | None = None) -> tuple[CrawlerRunResult, ...]:
        return self.application.tick(now or self._now_func())

    def seconds_until_next_tick(self, now: datetime | None = None) -> float:
        ref_time = now or self._now_func()
        next_run_time = self.application.service.next_run_time
        if next_run_time is None:
            return self.poll_seconds
        seconds_until_job = max(0.0, (next_run_time - ref_time).total_seconds())
        return min(self.poll_seconds, seconds_until_job)

    def run_forever(self) -> None:
        while not self.stopped:
            now = self._now_func()
            self.run_once(now)
            wait_seconds = self.seconds_until_next_tick(self._now_func())
            if wait_seconds <= 0:
                continue
            if self._stop_event.wait(wait_seconds):
                break
