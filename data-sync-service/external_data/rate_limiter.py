import time
import threading
from datetime import datetime


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = threading.Lock()

    def acquire(self) -> bool:
        with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window_seconds]
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

    def wait_and_acquire(self, api_name: str = "unknown") -> None:
        wait_count = 0
        while True:
            if self.acquire():
                if wait_count > 0:
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print(f"[{current_time}] [RateLimiter] [{api_name}] 等待完成 - 总等待次数: {wait_count}次, 窗口请求数: {len(self.requests)}/{self.max_requests}")
                return
            with self.lock:
                current_requests = len(self.requests)
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if self.requests:
                    oldest_request = min(self.requests)
                    wait_time = self.window_seconds - (time.time() - oldest_request)
                    if wait_time > 0:
                        wait_time += 0.1
                        print(f"[{current_time}] [RateLimiter] [{api_name}] 触发限流 - 等待次数: {wait_count+1}次, 当前请求数: {current_requests}/{self.max_requests}, 等待时间: {wait_time:.2f}秒")
                        time.sleep(wait_time)
                        wait_count += 1
                    else:
                        print(f"[{current_time}] [RateLimiter] [{api_name}] 触发限流 - 等待次数: {wait_count+1}次, 当前请求数: {current_requests}/{self.max_requests}, 等待时间: 0.01秒")
                        time.sleep(0.01)
                        wait_count += 1
                else:
                    print(f"[{current_time}] [RateLimiter] [{api_name}] 触发限流 - 等待次数: {wait_count+1}次, 当前请求数: {current_requests}/{self.max_requests}, 等待时间: 0.01秒")
                    time.sleep(0.01)
                    wait_count += 1
