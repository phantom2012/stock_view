import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Body
from fastapi import BackgroundTasks
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel
import asyncio
from datetime import datetime

from services.auction_data_service import get_auction_data_service
from services.money_flow_service import get_money_flow_service
from services.data_sync_notify_service import get_data_sync_notify_service
from shared.db import get_session, FilterConfig, DataSyncNotify

router = APIRouter(prefix="/api/data", tags=["数据加载"])

logger = logging.getLogger(__name__)
auction_data_service = get_auction_data_service()
money_flow_service = get_money_flow_service()
notify_service = get_data_sync_notify_service()


# SSE 订阅者队列（存储消息队列）
sse_queues = set()


@router.post("/load-auction-data")
def load_auction_data(stocks: List[Dict[str, Any]] = Body(...), days: int = 30):
    return auction_data_service.load_auction_data(stocks, days)


@router.post("/load-money-flow")
def load_money_flow(stocks: List[Dict[str, Any]] = Body(...), days: int = 30):
    return money_flow_service.load_money_flow_data(stocks, days)


@router.post("/load-daily-data")
def load_daily_data(block_codes: List[str] = Body(...)):
    """
    加载板块日线数据

    流程：
    1. 将板块列表更新到 filter_config(type=2) 的 select_blocks 字段
    2. 发送 daily_data 同步通知（清空 stock_codes，让同步器从板块配置读取股票列表）
    3. data-sync-service 扫描到通知后执行日线同步
    4. 同步完成后通过 SSE 通知前端

    Args:
        block_codes: 板块代码列表

    Returns:
        Dict: 执行结果
    """
    logger.info(f"收到加载板块日线请求，板块列表: {block_codes}")

    try:
        # 1. 更新 filter_config(type=2) 的 select_blocks 字段
        with get_session() as db:
            config = db.query(FilterConfig).filter(FilterConfig.type == 2).first()
            if config:
                config.select_blocks = ','.join(block_codes)
                config.update_time = datetime.now()
            else:
                config = FilterConfig(
                    type=2,
                    select_blocks=','.join(block_codes),
                    update_time=datetime.now()
                )
                db.add(config)
            db.commit()
            logger.info(f"已更新 filter_config(type=2) 的 select_blocks: {config.select_blocks}")

        # 2. 发送 daily_data 同步通知（notify_daily_data_sync 会清空 stock_codes）
        success = notify_service.notify_daily_data_sync()

        if success:
            return {
                "status": "success",
                "msg": f"板块日线数据同步已触发，共 {len(block_codes)} 个板块，请等待后台处理完成",
                "block_count": len(block_codes)
            }
        else:
            return {
                "status": "error",
                "msg": "触发日线数据同步失败"
            }

    except Exception as e:
        logger.error(f"触发板块日线数据同步失败: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "msg": str(e)}


@router.post("/save-filter-stocks")
def save_filter_stocks(stocks: List[Dict[str, Any]] = Body(...)):
    return auction_data_service.save_filter_stocks(stocks)



@router.get("/sync-status/{sync_type}")
def get_sync_status(sync_type: str):
    """获取指定同步类型的状态"""
    return notify_service.get_sync_status(sync_type)


class SyncCompleteRequest(BaseModel):
    sync_type: str
    success: bool = True
    message: str = ""


@router.post("/sync-complete")
async def sync_complete(request: SyncCompleteRequest):
    """
    data-sync-service 调用此接口通知同步完成

    Args:
        request: 同步完成请求体
            - sync_type: 同步类型 (money_flow, stock_info, daily_data, auction_data)
            - success: 是否成功
            - message: 结果消息
    """
    logger.info(f"收到 {request.sync_type} 同步完成通知: success={request.success}, message={request.message}")

    # 构建消息
    data = {
        "type": "sync_complete",
        "sync_type": request.sync_type,
        "success": request.success,
        "message": request.message,
        "timestamp": asyncio.get_event_loop().time()
    }

    # 通知所有 SSE 订阅者
    await notify_sse_subscribers(data)

    return {"status": "success", "msg": "通知已发送"}


@router.get("/sse")
async def sse_endpoint():
    """
    SSE 端点，向前端推送实时通知

    使用方式：
    const eventSource = new EventSource('/api/data/sse');
    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        console.log('收到通知:', data);
    };
    """
    # 创建一个新的消息队列
    queue = asyncio.Queue()
    sse_queues.add(queue)
    logger.info(f"SSE 客户端已连接: 总订阅数: {len(sse_queues)}")

    async def event_generator():
        try:
            while True:
                # 等待消息
                data = await queue.get()

                # 发送消息
                yield {
                    "event": "message",
                    "data": data
                }
        except asyncio.CancelledError:
            sse_queues.remove(queue)
            logger.info(f"SSE 客户端已断开: 总订阅数: {len(sse_queues)}")
            raise
        except Exception as e:
            logger.error(f"SSE 连接异常: {e}")
            sse_queues.discard(queue)

    return EventSourceResponse(event_generator())


async def notify_sse_subscribers(data: dict):
    """
    通知所有 SSE 订阅者

    Args:
        data: 要发送的数据（将被序列化为 JSON）
    """
    import json

    logger.info(f"准备通知 {len(sse_queues)} 个 SSE 订阅者: {data}")

    # 将数据序列化为 JSON 字符串
    message = json.dumps(data)

    # 向所有订阅者发送消息
    for queue in sse_queues:
        try:
            await queue.put(message)
        except Exception as e:
            logger.error(f"发送消息给 SSE 订阅者失败: {e}")
