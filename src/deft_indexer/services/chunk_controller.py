"""Adaptive chunk size controller for RPC log fetching."""

from dataclasses import dataclass

import structlog

log = structlog.get_logger(__name__)


@dataclass
class ChunkController:
    """
    Adaptive chunk size controller.

    Adjusts chunk size based on:
    - RPC response time
    - Number of logs returned
    - Error conditions (rate limits, timeouts, overflow)
    """

    size: int
    min_size: int
    max_size: int

    # Tuning parameters
    target_duration: float = 3.0  # Target RPC call duration (seconds)
    max_logs_per_chunk: int = 5000  # Target log count
    growth_factor: float = 1.25
    shrink_factor: float = 0.5

    def on_success(self, duration_s: float, log_count: int) -> int:
        """
        Adjust chunk size after successful fetch.

        Grow if fast and low log count, shrink if slow or high log count.
        """
        old_size = self.size

        # Fast response with low density -> increase chunk
        if duration_s < (self.target_duration * 0.5) and log_count < (
            self.max_logs_per_chunk * 0.3
        ):
            self.size = min(int(self.size * self.growth_factor), self.max_size)

        # Slow response or high density -> decrease chunk
        elif duration_s > self.target_duration or log_count > self.max_logs_per_chunk:
            self.size = max(int(self.size * self.shrink_factor), self.min_size)

        if self.size != old_size:
            log.info(
                "chunk_size_adjusted",
                old_size=old_size,
                new_size=self.size,
                duration=duration_s,
                log_count=log_count,
            )

        return self.size

    def on_error(self, error_kind: str, at_minimum: bool = False) -> tuple[int, float]:
        """
        Adjust chunk size after error.

        Aggressive shrink on overflow/timeout, moderate on other errors.
        Returns (new_size, backoff_seconds) to prevent hot loops at min_chunk_size.
        """
        old_size = self.size
        backoff_seconds = 0.0

        if error_kind in {"-32005", "too_many_results", "timeout"}:
            # Aggressive shrink for capacity errors
            self.size = max(int(self.size * self.shrink_factor), self.min_size)
            # If already at minimum, add exponential backoff
            if at_minimum or self.size == self.min_size:
                backoff_seconds = min(5.0, 0.5 * (2 ** (old_size == self.min_size)))
        elif error_kind == "rate_limit":
            # Moderate shrink for rate limits
            self.size = max(int(self.size * 0.75), self.min_size)
        else:
            # Conservative shrink for unknown errors
            self.size = max(int(self.size * 0.9), self.min_size)

        log.warning(
            "chunk_size_reduced",
            old_size=old_size,
            new_size=self.size,
            error_kind=error_kind,
            backoff_seconds=backoff_seconds,
        )

        return self.size, backoff_seconds

    def reset_to_default(self) -> None:
        """Reset to initial chunk size (e.g., after long idle)."""
        from ..config import get_settings

        settings = get_settings()
        self.size = settings.initial_chunk_size
