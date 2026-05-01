"""
数据同步定时器框架
用于创建定时任务，支持每个任务独立配置定时间隔和启用状态

功能：
1. 支持注册多个定时任务，每个任务独立配置
2. 每个任务可配置定时间隔（秒/分钟/小时/每日固定时间）
3. 每个任务可独立启用/禁用
4. 支持手动触发执行
5. 线程安全，后台守护线程运行
6. 记录每次执行结果
"""
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class IntervalType(Enum):
    """定时间隔类型"""
    SECONDS = "seconds"         # 按秒间隔
    MINUTES = "minutes"         # 按分钟间隔
    HOURS = "hours"             # 按小时间隔
    DAILY = "daily"             # 每日固定时间


class TaskStatus(Enum):
    """任务执行状态"""
    IDLE = "idle"               # 空闲
    RUNNING = "running"         # 运行中
    SUCCESS = "success"         # 成功
    FAILED = "failed"           # 失败
    SKIPPED = "skipped"         # 跳过


@dataclass
class TaskConfig:
    """
    定时任务配置

    Attributes:
        name: 任务名称（唯一标识）
        interval_type: 定时间隔类型
        interval_value: 间隔值
            - SECONDS: 秒数
            - MINUTES: 分钟数
            - HOURS: 小时数
            - DAILY: 每日执行的小时数（0-23）
        interval_minute: DAILY模式下执行的分钟数（0-59），默认0
        execute_func: 任务执行函数，返回 bool 表示执行成功/失败
        enabled: 是否启用
        description: 任务描述（可选）
    """
    name: str
    interval_type: IntervalType = IntervalType.DAILY
    interval_value: int = 16     # 默认每日16点执行
    interval_minute: int = 0     # 默认0分
    execute_func: Callable[[], bool] = None
    enabled: bool = True
    description: str = ""


@dataclass
class TaskResult:
    """单次任务执行结果"""
    task_name: str
    status: TaskStatus
    start_time: datetime = None
    end_time: datetime = None
    error_message: str = ""
    detail: str = ""


class DataSyncTimer:
    """
    数据同步定时器框架
    单例模式，管理所有定时任务

    用法:
        timer = DataSyncTimer()

        # 注册任务（每5分钟执行一次）
        timer.register_task(TaskConfig(
            name="我的定时任务",
            interval_type=IntervalType.MINUTES,
            interval_value=5,
            execute_func=my_task_func,
            enabled=True,
        ))

        # 注册任务（每日16:30执行）
        timer.register_task(TaskConfig(
            name="每日收盘任务",
            interval_type=IntervalType.DAILY,
            interval_value=16,
            interval_minute=30,
            execute_func=my_daily_func,
            enabled=True,
        ))

        # 启动定时器
        timer.start()

        # 手动触发
        timer.run_task("我的定时任务")
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._tasks: Dict[str, TaskConfig] = {}
        self._results: Dict[str, TaskResult] = {}
        self._timer_thread: Optional[threading.Thread] = None
        self._running = False
        self._stop_event = threading.Event()
        self._task_lock = threading.Lock()

        self._initialized = True
        logger.info("DataSyncTimer initialized")

    # ==================== 任务注册与管理 ====================

    def register_task(self, config: TaskConfig) -> bool:
        """
        注册一个定时任务

        Args:
            config: 任务配置

        Returns:
            bool: 注册成功返回True
        """
        if not config.name:
            logger.error("任务名称不能为空")
            return False

        if not config.execute_func:
            logger.error(f"任务 '{config.name}': execute_func 不能为空")
            return False

        with self._task_lock:
            if config.name in self._tasks:
                logger.warning(f"任务 '{config.name}' 已存在，将被覆盖")
            self._tasks[config.name] = config
            logger.info(f"任务 '{config.name}' 注册成功 (间隔={config.interval_type.value}:{config.interval_value}, 启用={config.enabled})")
            return True

    def unregister_task(self, name: str) -> bool:
        """
        注销一个定时任务

        Args:
            name: 任务名称

        Returns:
            bool: 注销成功返回True
        """
        with self._task_lock:
            if name in self._tasks:
                del self._tasks[name]
                logger.info(f"任务 '{name}' 已注销")
                return True
            logger.warning(f"任务 '{name}' 不存在")
            return False

    def get_task(self, name: str) -> Optional[TaskConfig]:
        """获取任务配置"""
        return self._tasks.get(name)

    def get_all_tasks(self) -> Dict[str, TaskConfig]:
        """获取所有任务配置"""
        with self._task_lock:
            return dict(self._tasks)

    def enable_task(self, name: str, enabled: bool = True) -> bool:
        """
        启用/禁用任务

        Args:
            name: 任务名称
            enabled: True启用，False禁用

        Returns:
            bool: 操作成功返回True
        """
        with self._task_lock:
            if name in self._tasks:
                self._tasks[name].enabled = enabled
                logger.info(f"任务 '{name}' 已{'启用' if enabled else '禁用'}")
                return True
            return False

    def update_task_interval(self, name: str, interval_type: IntervalType, interval_value: int, interval_minute: int = 0) -> bool:
        """
        更新任务的定时间隔

        Args:
            name: 任务名称
            interval_type: 间隔类型
            interval_value: 间隔值
            interval_minute: DAILY模式下的分钟数

        Returns:
            bool: 更新成功返回True
        """
        with self._task_lock:
            if name in self._tasks:
                self._tasks[name].interval_type = interval_type
                self._tasks[name].interval_value = interval_value
                self._tasks[name].interval_minute = interval_minute
                logger.info(f"任务 '{name}' 间隔已更新: {interval_type.value}:{interval_value}:{interval_minute}")
                return True
            return False

    def get_task_result(self, name: str) -> Optional[TaskResult]:
        """获取任务最近一次执行结果"""
        return self._results.get(name)

    def get_all_results(self) -> Dict[str, TaskResult]:
        """获取所有任务最近一次执行结果"""
        return dict(self._results)

    # ==================== 任务执行 ====================

    def run_task(self, task_name: str) -> Optional[TaskResult]:
        """
        执行指定任务

        Args:
            task_name: 任务名称

        Returns:
            TaskResult: 执行结果
        """
        with self._task_lock:
            config = self._tasks.get(task_name)
            if not config:
                logger.error(f"任务 '{task_name}' 不存在")
                return None

        if not config.enabled:
            logger.info(f"任务 '{task_name}' 已禁用，跳过执行")
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.SKIPPED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message="任务已禁用"
            )

        result = TaskResult(
            task_name=task_name,
            status=TaskStatus.RUNNING,
            start_time=datetime.now()
        )

        try:
            logger.info(f"开始执行任务 '{task_name}'...")
            success = config.execute_func()

            if success:
                result.status = TaskStatus.SUCCESS
                result.detail = "执行成功"
                logger.info(f"任务 '{task_name}' 执行成功")
            else:
                result.status = TaskStatus.FAILED
                result.error_message = "执行函数返回失败"
                logger.warning(f"任务 '{task_name}' 执行失败")

        except Exception as e:
            logger.error(f"任务 '{task_name}' 执行异常: {e}")
            import traceback
            traceback.print_exc()
            result.status = TaskStatus.FAILED
            result.error_message = str(e)

        result.end_time = datetime.now()

        with self._task_lock:
            self._results[task_name] = result

        return result

    def run_all_tasks(self) -> Dict[str, TaskResult]:
        """
        执行所有已启用的任务

        Returns:
            Dict[str, TaskResult]: 任务名称 -> 执行结果
        """
        results = {}
        with self._task_lock:
            task_names = list(self._tasks.keys())

        for name in task_names:
            result = self.run_task(name)
            if result:
                results[name] = result

        return results

    # ==================== 定时器管理 ====================

    def _calc_next_run_seconds(self, config: TaskConfig) -> float:
        """
        计算距离下次执行还有多少秒

        Args:
            config: 任务配置

        Returns:
            float: 距离下次执行的秒数
        """
        now = datetime.now()

        if config.interval_type == IntervalType.SECONDS:
            return config.interval_value

        elif config.interval_type == IntervalType.MINUTES:
            return config.interval_value * 60

        elif config.interval_type == IntervalType.HOURS:
            return config.interval_value * 3600

        elif config.interval_type == IntervalType.DAILY:
            # 计算到下次指定时间的秒数
            target = now.replace(
                hour=config.interval_value,
                minute=config.interval_minute,
                second=0,
                microsecond=0
            )
            if now >= target:
                # 如果今天已过，则明天执行
                target += timedelta(days=1)
            return (target - now).total_seconds()

        return 60  # 默认60秒

    def _timer_loop(self):
        """
        定时器主循环
        每秒检查一次，判断哪些任务需要执行
        """
        logger.info("DataSyncTimer 定时器主循环已启动")

        # 记录每个任务的上次执行时间
        last_run_times: Dict[str, datetime] = {}

        while not self._stop_event.is_set():
            try:
                now = datetime.now()

                with self._task_lock:
                    task_snapshot = dict(self._tasks)

                for name, config in task_snapshot.items():
                    if not config.enabled:
                        continue

                    # 检查是否到了执行时间
                    last_run = last_run_times.get(name)
                    next_run_seconds = self._calc_next_run_seconds(config)

                    if last_run is None:
                        # 首次运行：立即执行
                        should_run = True
                    elif config.interval_type == IntervalType.DAILY:
                        # 每日模式：检查是否到了指定时间且今天还没执行
                        target_time = now.replace(
                            hour=config.interval_value,
                            minute=config.interval_minute,
                            second=0,
                            microsecond=0
                        )
                        should_run = (
                            now >= target_time and
                            last_run.date() < now.date()
                        )
                    else:
                        # 间隔模式：检查是否超过了间隔时间
                        elapsed = (now - last_run).total_seconds()
                        should_run = elapsed >= next_run_seconds

                    if should_run:
                        logger.info(f"定时触发任务 '{name}'")
                        # 在独立线程中执行，不阻塞定时器循环
                        thread = threading.Thread(
                            target=self._run_task_safe,
                            args=(name,),
                            daemon=True
                        )
                        thread.start()
                        last_run_times[name] = now

            except Exception as e:
                logger.error(f"定时器循环异常: {e}")

            # 每秒检查一次
            self._stop_event.wait(1)

        logger.info("DataSyncTimer 定时器主循环已停止")

    def _run_task_safe(self, task_name: str):
        """安全执行任务（捕获所有异常）"""
        try:
            self.run_task(task_name)
        except Exception as e:
            logger.error(f"安全执行任务 '{task_name}' 异常: {e}")

    def start(self):
        """
        启动定时器
        在后台守护线程中运行
        """
        if self._running:
            logger.warning("DataSyncTimer 已在运行中")
            return

        self._running = True
        self._stop_event.clear()
        self._timer_thread = threading.Thread(
            target=self._timer_loop,
            daemon=True,
            name="DataSyncTimer"
        )
        self._timer_thread.start()
        logger.info("DataSyncTimer 已启动")

    def stop(self):
        """
        停止定时器
        """
        if not self._running:
            logger.warning("DataSyncTimer 未在运行")
            return

        self._stop_event.set()
        self._running = False
        logger.info("DataSyncTimer 已停止")

    @property
    def is_running(self) -> bool:
        """定时器是否在运行"""
        return self._running

    # ==================== 状态查询 ====================

    def get_summary(self) -> Dict[str, Any]:
        """
        获取定时器状态摘要

        Returns:
            Dict 包含定时器状态和所有任务信息
        """
        summary = {
            'is_running': self._running,
            'tasks': {},
        }

        with self._task_lock:
            for name, config in self._tasks.items():
                result = self._results.get(name)
                task_info = {
                    'name': name,
                    'description': config.description,
                    'enabled': config.enabled,
                    'interval_type': config.interval_type.value,
                    'interval_value': config.interval_value,
                    'interval_minute': config.interval_minute,
                }
                if result:
                    task_info['last_result'] = {
                        'status': result.status.value,
                        'start_time': result.start_time.isoformat() if result.start_time else None,
                        'end_time': result.end_time.isoformat() if result.end_time else None,
                        'error_message': result.error_message,
                        'detail': result.detail,
                    }
                summary['tasks'][name] = task_info

        return summary


# ==================== 全局单例 ====================

_global_timer = None


def get_data_sync_timer() -> DataSyncTimer:
    """
    获取数据同步定时器全局单例

    Returns:
        DataSyncTimer 实例
    """
    global _global_timer
    if _global_timer is None:
        _global_timer = DataSyncTimer()
    return _global_timer


def register_default_tasks():
    """
    注册所有默认的业务定时任务
    在 main.py 中调用 timer.start() 之前调用此方法
    具体业务任务实现在 backend/tasks/ 目录下

    用法:
        from common.data_sync_timer import get_data_sync_timer, register_default_tasks

        timer = get_data_sync_timer()
        register_default_tasks()  # 注册所有业务任务
        timer.start()             # 启动定时器
    """
    timer = get_data_sync_timer()

    # ---- 在这里注册所有业务定时任务 ----

    # 资金流向数据同步（每5分钟执行一次）
    from tasks.money_flow_sync_task import sync_money_flow_data
    timer.register_task(TaskConfig(
        name="资金流向数据同步",
        interval_type=IntervalType.MINUTES,
        interval_value=5,
        execute_func=sync_money_flow_data,
        enabled=True,
        description="扫描filter_results股票，补充stock_money_flow缺失的最近30个交易日数据",
    ))

    # 股票流通股本信息同步（每5分钟执行一次）
    from tasks.stock_info_sync_task import sync_stock_info_free_share
    timer.register_task(TaskConfig(
        name="股票流通股本信息同步",
        interval_type=IntervalType.MINUTES,
        interval_value=3,
        execute_func=sync_stock_info_free_share,
        enabled=True,
        description="扫描filter_results股票，检查stock_info表中free_share是否为空，为空则调用daily_basic接口更新",
    ))


    logger.info("已注册 %d 个默认定时任务", len(timer.get_all_tasks()))


# ==================== 使用示例（仅作参考，不影响正常功能） ====================


if __name__ == "__main__":
    """
    使用示例 - 运行方式: python -m common.data_sync_timer

    示例1: 每5分钟执行一次的任务
    示例2: 每日16:30执行的任务
    """

    # 示例1: 每5分钟执行一次的任务
    def my_periodic_task() -> bool:
        """模拟一个周期性执行的任务"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 周期性任务执行中...")
        # TODO: 在这里编写你的业务逻辑
        return True

    # 示例2: 每日收盘后执行的任务
    def my_daily_task() -> bool:
        """模拟一个每日执行的任务"""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 每日任务执行中...")
        # TODO: 在这里编写你的业务逻辑
        return True

    # 创建定时器实例
    timer = get_data_sync_timer()

    # 注册任务1: 每5分钟执行一次
    timer.register_task(TaskConfig(
        name="我的定时任务",
        interval_type=IntervalType.MINUTES,
        interval_value=5,
        execute_func=my_periodic_task,
        enabled=True,
        description="每5分钟执行一次的示例任务",
    ))

    # 注册任务2: 每日16:30执行
    timer.register_task(TaskConfig(
        name="每日收盘任务",
        interval_type=IntervalType.DAILY,
        interval_value=16,
        interval_minute=30,
        execute_func=my_daily_task,
        enabled=True,
        description="每日16:30执行的示例任务",
    ))

    # 手动触发一次测试
    print("=" * 50)
    print("手动触发测试任务...")
    result = timer.run_task("我的定时任务")
    print(f"执行结果: {result.status.value}")

    # 启动定时器（后台运行）
    print("\n启动定时器（按 Ctrl+C 停止）...")
    timer.start()

    try:
        # 主线程保持运行
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\n收到停止信号...")
    finally:
        timer.stop()
        print("定时器已停止")
