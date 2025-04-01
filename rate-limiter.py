import time
import threading

class RateLimiter:
    def __init__(self, max_requests_per_minute):
        self.lock = threading.Lock()
        self.min_interval = 60.0 / max_requests_per_minute 
        self.last_time_called = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time_called
            wait_time = self.min_interval - elapsed

            if wait_time > 0:
                time.sleep(wait_time)

            self.last_time_called = time.time()
