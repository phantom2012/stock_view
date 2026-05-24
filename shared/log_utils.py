import logging
import time
import inspect
from threading import Lock
from typing import Dict, Tuple


class LogUtils:

    def __init__(self, logger: logging.Logger, threshold: int = 3, window_seconds: float = 3.0, throttle_interval: float = 2.0):
        self._logger = logger
        self._threshold = threshold
        self._window = window_seconds
        self._throttle_interval = throttle_interval
        self._call_records: Dict[str, dict] = {}
        self._lock = Lock()

    def _get_caller_info(self, stack_level: int = 2) -> str:
        frame = inspect.currentframe()
        try:
            for _ in range(stack_level):
                if frame is None:
                    break
                frame = frame.f_back

            if frame is None:
                return "unknown:0"

            filename = frame.f_code.co_filename
            lineno = frame.f_lineno
            return f"{filename}:{lineno}"
        finally:
            del frame

    def _cleanup_old_calls(self, record: dict, current_time: float) -> None:
        if 'timestamps' in record:
            cutoff = current_time - self._window
            record['timestamps'] = [t for t in record['timestamps'] if t > cutoff]

    def _should_log(self, stack_level: int = 2) -> Tuple[bool, str, int]:
        caller_key = self._get_caller_info(stack_level)
        current_time = time.time()

        with self._lock:
            if caller_key not in self._call_records:
                self._call_records[caller_key] = {
                    'timestamps': [],
                    'throttled': False,
                    'last_print_time': 0,
                    'throttle_count': 0,
                    'last_activity_time': 0
                }

            record = self._call_records[caller_key]

            if current_time - record['last_activity_time'] > self._window and record['last_activity_time'] > 0:
                record['throttled'] = False
                record['throttle_count'] = 0
                record['last_print_time'] = 0
                record['timestamps'] = []

            record['timestamps'].append(current_time)
            record['last_activity_time'] = current_time

            call_count = len(record['timestamps'])

            if call_count > self._threshold and not record['throttled']:
                record['throttled'] = True
                record['throttle_count'] = call_count
                record['last_print_time'] = current_time
                return True, caller_key, call_count

            if record['throttled']:
                record['throttle_count'] = call_count
                time_since_last_print = current_time - record['last_print_time']

                if time_since_last_print >= self._throttle_interval:
                    record['last_print_time'] = current_time
                    return True, caller_key, call_count

                return False, caller_key, call_count

            return True, caller_key, call_count

    def _get_throttle_suffix(self, call_count: int) -> str:
        return f" [调用次数: {call_count}]"

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warn(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def limit_warn(self, msg: str, *args, stack_level: int = 2, **kwargs) -> None:
        should_log, caller_key, call_count = self._should_log(stack_level)

        if should_log:
            if call_count > self._threshold:
                msg += self._get_throttle_suffix(call_count)
            self._logger.warning(msg, *args, **kwargs)

    def limit_error(self, msg: str, *args, stack_level: int = 2, **kwargs) -> None:
        should_log, caller_key, call_count = self._should_log(stack_level)

        if should_log:
            if call_count > self._threshold:
                msg += self._get_throttle_suffix(call_count)
            self._logger.error(msg, *args, **kwargs)

    def log_progress(self, message: str, current: int, total: int) -> None:
        if total < 10:
            self._logger.info(message)
        else:
            step = total // 10
            if current % step == 0 or current == total:
                self._logger.info(message)


def create_log_util(name: str) -> LogUtils:
    logger = logging.getLogger(name)
    return LogUtils(logger)