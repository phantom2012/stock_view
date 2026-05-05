"""
数据同步调度器
管理所有定时任务和通知表扫描
支持启动延迟执行功能
"""
import logging
import json
from datetime import datetime, timedelta
from typing import Optional
import requests

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

from shared.db import get_session, DataSyncNotify
from config import (
    MONEY_FLOW_CONFIG, STOCK_INFO_CONFIG, DAILY_DATA_CONFIG,
    AUCTION_DATA_CONFIG, MINUTE_DATA_CONFIG, CLEAR_DATA_CONFIG, NOTIFY_SCANNER_CONFIG,
    BACKEND_CONFIG,
)
from syncers.money_flow_syncer import MoneyFlowSyncer
from syncers.stock_info_syncer import StockInfoSyncer
from syncers.daily_data_syncer import DailyDataSyncer
from syncers.auction_data_syncer import AuctionDataSyncer
from syncers.minute_data_syncer import MinuteDataSyncer
from syncers.clear_data_syncer import ClearDataSyncer

logger = logging.getLogger(__name__)


class DataSyncScheduler:
    """
    数据同步调度器
    管理定时任务和通知表扫描
    """

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.syncers = {
            'money_flow': MoneyFlowSyncer(),
            'stock_info': StockInfoSyncer(),
            'daily_data': DailyDataSyncer(),
            'auction_data': AuctionDataSyncer(),
            'minute_data': MinuteDataSyncer(),
            'clear_data': ClearDataSyncer(),
        }

    def start(self):
        """启动调度器"""
        self._register_timed_tasks()
        self._register_notify_scanner()
        self._init_notify_records()
        self.scheduler.start()
        logger.info("调度器已启动")

    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown(wait=False)
        logger.info("调度器已停止")

    def _register_timed_tasks(self):
        """注册定时任务"""
        # 资金流向同步（每15分钟）
        self._register_job_with_delay(
            'money_flow',
            IntervalTrigger(minutes=MONEY_FLOW_CONFIG['interval_minutes']),
            MONEY_FLOW_CONFIG.get('start_delay_minutes', 0),
            'timed_money_flow_sync',
            '定时资金流向同步',
            30,
            f"每{MONEY_FLOW_CONFIG['interval_minutes']}分钟"
        )

        # 股票信息同步（每日16:00）
        self._register_job_with_delay(
            'stock_info',
            CronTrigger(hour=STOCK_INFO_CONFIG['cron_hour'], minute=STOCK_INFO_CONFIG['cron_minute']),
            STOCK_INFO_CONFIG.get('start_delay_minutes', 0),
            'timed_stock_info_sync',
            '定时股票信息同步',
            300,
            f"每日{STOCK_INFO_CONFIG['cron_hour']}:{STOCK_INFO_CONFIG['cron_minute']:02d}"
        )

        # 日线数据同步（每5分钟）
        self._register_job_with_delay(
            'daily_data',
            IntervalTrigger(minutes=DAILY_DATA_CONFIG['interval_minutes']),
            DAILY_DATA_CONFIG.get('start_delay_minutes', 0),
            'timed_daily_data_sync',
            '定时日线数据同步',
            300,
            f"每{DAILY_DATA_CONFIG['interval_minutes']}分钟"
        )

        # 竞价数据同步（每日9:35）
        self._register_job_with_delay(
            'auction_data',
            CronTrigger(hour=AUCTION_DATA_CONFIG['cron_hour'], minute=AUCTION_DATA_CONFIG['cron_minute']),
            AUCTION_DATA_CONFIG.get('start_delay_minutes', 0),
            'timed_auction_data_sync',
            '定时竞价数据同步',
            300,
            f"每日{AUCTION_DATA_CONFIG['cron_hour']}:{AUCTION_DATA_CONFIG['cron_minute']}"
        )

        # 分钟数据同步（每日16:00）
        self._register_job_with_delay(
            'minute_data',
            CronTrigger(hour=MINUTE_DATA_CONFIG['cron_hour'], minute=MINUTE_DATA_CONFIG['cron_minute']),
            MINUTE_DATA_CONFIG.get('start_delay_minutes', 0),
            'timed_minute_data_sync',
            '定时分钟数据同步',
            300,
            f"每日{MINUTE_DATA_CONFIG['cron_hour']}:{MINUTE_DATA_CONFIG['cron_minute']:02d}"
        )

        # 数据清理扫描（每分钟）
        self._register_job_with_delay(
            'clear_data',
            IntervalTrigger(seconds=CLEAR_DATA_CONFIG['interval_seconds']),
            CLEAR_DATA_CONFIG.get('start_delay_minutes', 0),
            'timed_clear_data_sync',
            '定时数据清理',
            10,
            f"每{CLEAR_DATA_CONFIG['interval_seconds']}秒"
        )

    def _register_job_with_delay(self, sync_type: str, trigger, delay_minutes: int,
                                job_id: str, job_name: str, misfire_grace_time: int, desc: str):
        """
        注册带启动延迟的定时任务

        Args:
            sync_type: 同步类型
            trigger: APScheduler trigger
            delay_minutes: 启动延迟分钟数，0或None表示不延迟
            job_id: 任务ID
            job_name: 任务名称
            misfire_grace_time: 容错时间
            desc: 描述信息
        """
        if delay_minutes and delay_minutes > 0:
            # 添加启动延迟执行的一次性任务
            delay_time = datetime.now() + timedelta(minutes=delay_minutes)
            self.scheduler.add_job(
                self._run_syncer,
                DateTrigger(run_date=delay_time),
                args=[sync_type],
                id=f'{job_id}_delay_start',
                name=f'{job_name}(启动延迟)',
                misfire_grace_time=misfire_grace_time,
            )
            logger.info(f"注册启动延迟任务: {job_name} (延迟{delay_minutes}分钟执行)")

        # 添加定时任务
        self.scheduler.add_job(
            self._run_syncer,
            trigger,
            args=[sync_type],
            id=job_id,
            name=job_name,
            misfire_grace_time=misfire_grace_time,
        )
        logger.info(f"注册定时任务: {job_name} ({desc})")

    def _register_notify_scanner(self):
        """注册通知表扫描器"""
        self.scheduler.add_job(
            self._scan_notify_table,
            IntervalTrigger(seconds=NOTIFY_SCANNER_CONFIG['interval_seconds']),
            id='notify_scanner',
            name='通知表扫描',
            misfire_grace_time=5,
        )
        logger.info(f"注册通知表扫描器 (每{NOTIFY_SCANNER_CONFIG['interval_seconds']}秒)")

    def _init_notify_records(self):
        """初始化通知表记录（如果不存在）"""
        # 定义各同步类型的优先级（数值越小优先级越高，从1开始）
        sync_type_priorities = [
            ('stock_info', 1),      # 股票信息 - 最高优先级
            ('daily_data', 2),      # 日线数据
            ('minute_data', 3),     # 分钟数据
            ('auction_data', 5),    # 竞价数据
            ('money_flow', 6),      # 资金流向
            ('clear_data', 10),     # 数据清理 - 最低优先级
        ]
        with get_session() as db:
            for sync_type, priority in sync_type_priorities:
                existing = db.query(DataSyncNotify).filter(
                    DataSyncNotify.sync_type == sync_type
                ).first()
                if not existing:
                    record = DataSyncNotify(
                        priority=priority,
                        sync_type=sync_type,
                        trigger_flag=0,
                        status=0,
                        result_msg='初始化',
                        success_count=0,
                        fail_count=0,
                        update_time=datetime.now(),
                    )
                    db.add(record)
                    logger.info(f"初始化通知记录: {sync_type} (优先级: {priority})")

    def _run_syncer(self, sync_type: str):
        """
        执行同步器（定时任务调用）

        Args:
            sync_type: 同步类型
        """
        syncer = self.syncers.get(sync_type)
        if not syncer:
            logger.error(f"未知的同步类型: {sync_type}")
            return

        logger.info(f"[定时任务] 开始执行 {sync_type} 同步...")
        try:
            success, success_count, fail_count, result_msg = syncer.sync()
            logger.info(
                f"[定时任务] {sync_type} 同步完成: "
                f"{'成功' if success else '失败'}, "
                f"成功{success_count}条, 失败{fail_count}条, {result_msg}"
            )
        except Exception as e:
            logger.error(f"[定时任务] {sync_type} 同步异常: {e}")
            import traceback
            traceback.print_exc()

    def _scan_notify_table(self):
        """扫描通知表，按优先级有序处理 backend 触发的同步请求"""
        try:
            with get_session() as db:
                # 按优先级升序排序（数值越小优先级越高）
                pending = db.query(DataSyncNotify).filter(
                    DataSyncNotify.trigger_flag == 1,
                    DataSyncNotify.status == 0
                ).order_by(DataSyncNotify.priority).all()

                if not pending:
                    return

                logger.info(f"[通知扫描] 检测到 {len(pending)} 个待处理任务，开始按优先级顺序执行")

                for notify in pending:
                    sync_type = notify.sync_type
                    priority = notify.priority

                    # 获取股票列表（如果指定）
                    stock_codes = None
                    if notify.stock_codes:
                        try:
                            import json
                            stock_codes = json.loads(notify.stock_codes)
                        except json.JSONDecodeError:
                            logger.warning(f"[通知扫描] {sync_type} 的 stock_codes 格式无效")

                    logger.info(f"[通知扫描] 执行任务: {sync_type} (优先级: {priority}, 股票数: {len(stock_codes) if stock_codes else '全部'})")

                    # 更新状态为处理中
                    self._update_notify_status(sync_type, 1, '处理中')

                    # 执行同步（传递股票列表）
                    syncer = self.syncers.get(sync_type)
                    if not syncer:
                        self._update_notify_status(
                            sync_type, -1, 0, 0, f"未知的同步类型: {sync_type}"
                        )
                        continue

                    try:
                        # 传递 stock_codes 参数给同步器
                        success, success_count, fail_count, result_msg = syncer.sync(stock_codes=stock_codes)
                        status = 2 if success else -1
                        self._update_notify_status(
                            sync_type, status, success_count, fail_count,
                            result_msg or ('成功' if success else '失败')
                        )
                        logger.info(
                            f"[通知扫描] {sync_type} 同步完成: "
                            f"{'成功' if success else '失败'}, "
                            f"成功{success_count}条, 失败{fail_count}条"
                        )

                        # 通知 backend 同步完成
                        self._notify_backend_sync_complete(sync_type, success, result_msg)

                    except Exception as e:
                        self._update_notify_status(
                            sync_type, -1, 0, 0, f"同步异常: {str(e)}"
                        )
                        logger.error(f"[通知扫描] {sync_type} 同步异常: {e}")
                        import traceback
                        traceback.print_exc()

        except Exception as e:
            logger.error(f"[通知扫描] 扫描异常: {e}")

    def _notify_backend_sync_complete(self, sync_type: str, success: bool, message: str = ""):
        """
        通知 backend 同步完成

        Args:
            sync_type: 同步类型
            success: 是否成功
            message: 结果消息
        """
        try:
            base_url = BACKEND_CONFIG['base_url']
            endpoint = BACKEND_CONFIG['sync_complete_endpoint']
            url = f"{base_url}{endpoint}"
            timeout = BACKEND_CONFIG['timeout_seconds']

            logger.info(f"[通知 backend] 向 {url} 发送 {sync_type} 同步完成通知")

            response = requests.post(
                url,
                params={
                    'sync_type': sync_type,
                    'success': success,
                    'message': message
                },
                timeout=timeout
            )

            if response.status_code == 200:
                logger.info(f"[通知 backend] {sync_type} 同步完成通知发送成功")
            else:
                logger.warning(f"[通知 backend] {sync_type} 同步完成通知发送失败: {response.status_code}")

        except requests.exceptions.RequestException as e:
            logger.error(f"[通知 backend] {sync_type} 同步完成通知发送异常: {e}")

    def _update_notify_status(
        self,
        sync_type: str,
        status: int,
        success_count: int = 0,
        fail_count: int = 0,
        result_msg: str = ""
    ):
        """
        更新通知表状态

        Args:
            sync_type: 同步类型
            status: 状态 (0=待处理, 1=处理中, 2=已完成, -1=失败)
            success_count: 成功条数
            fail_count: 失败条数
            result_msg: 结果信息
        """
        with get_session() as db:
            notify = db.query(DataSyncNotify).filter(
                DataSyncNotify.sync_type == sync_type
            ).first()
            if notify:
                if status in (2, -1):
                    # 完成或失败时，清除触发标志
                    notify.trigger_flag = 0
                notify.status = status
                notify.success_count = success_count
                notify.fail_count = fail_count
                notify.result_msg = result_msg
                notify.update_time = datetime.now()
                db.commit()
