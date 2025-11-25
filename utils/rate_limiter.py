from collections import deque
import threading
import time


class RateLimiter:
    """
    Thread-safe rate limiter. Polars is multithreaded by default,
    so rate limitation needs to take this into account when making calls
    against APIs.
    """

    def __init__(self, max_calls: int, period: float = 1.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self._lock = threading.Lock()
        self._calls = deque()

    def wait(self) -> None:
        """
        Block until another call can be made.
        """

        while True:
            with self._lock:
                now = time.monotonic()

                # If first call is before the window, we can drop it
                while self._calls and self._calls[0] <= now - self.period:
                    self._calls.popleft()

                # We can make a call if there are fewer
                # than max calls in the queue
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return

                oldest = self._calls[0]
                sleep_for = self.period - (now - oldest)

            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                time.sleep(0.001)
