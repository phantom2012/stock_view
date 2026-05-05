import logging
from datetime import datetime
from typing import Dict, Any
from fastapi import APIRouter

from stock_cache import get_stock_cache
from common.stock_code_convert import to_goldminer_symbol

router = APIRouter(prefix="/api/stock", tags=["股票信息"])

logger = logging.getLogger(__name__)
stock_cache = get_stock_cache()


def _build_default_stock_info(code: str) -> Dict[str, Any]:
    return {
        'code': code,
        'stock_name': '未知股票',
        'today_gain': '-',
        'open_price': '-',
        'open_volume': '-',
        'price_diff': '-',
        'interval_max_rise': '-',
        'max_day_rise': '-',
        'next_day_rise': '-',
        'trade_date': datetime.now().strftime('%Y-%m-%d'),
        'exp_score': '-',
        'rising_wave_score': '-',
        'weipan_exceed': False,
        'zaopan_exceed': False,
        'rising_wave': False
    }


@router.get("/get-stock-info")
def get_stock_info(code: str):
    result = _build_default_stock_info(code)

    try:
        symbol = to_goldminer_symbol(code)
        stock_name = stock_cache.get_stock_name(symbol)

        # 如果数据库中没有名称，直接使用"未知"，不再调用外部接口
        if stock_name == '未知':
            logger.warning(f"股票 {code} 在数据库中未找到名称")

        data = stock_cache.get_history_data(symbol, days=2)

        if data is not None and len(data) >= 2:
            today_close = data.iloc[-1]['close']
            prev_close = data.iloc[-2]['close']
            if prev_close and prev_close > 0:
                result['today_gain'] = round((today_close - prev_close) / prev_close * 100, 2)

        result['stock_name'] = stock_name
        return result
    except Exception as e:
        logger.error(f"Error in get-stock-info: {e}")
        return result


@router.get("/get-stock-history")
def get_stock_history(code: str, days: int = 10):
    try:
        symbol = to_goldminer_symbol(code)
        data = stock_cache.get_history_data(symbol, days)

        if data is not None and len(data) > 0:
            result = []
            prev_avg_price = None

            daily_data = []
            for _, row in data.iterrows():
                close = row['close']
                volume = row['volume']
                amount = row['amount']
                pre_close = row['pre_close']

                avg_price = amount / volume if volume and volume > 0 else 0

                open_amount = 0
                try:
                    trade_date_str = str(row['eob'])[:10]
                    trade_date = datetime.strptime(trade_date_str, '%Y-%m-%d')

                    auction_data = stock_cache.get_auction_data(symbol, trade_date)
                    open_amount = auction_data['open_amount']
                    try:
                        if open_amount is None:
                            open_amount = 0
                        elif isinstance(open_amount, bytes):
                            open_amount = 0
                        else:
                            open_amount = float(open_amount)
                    except (ValueError, TypeError):
                        open_amount = 0
                    except Exception as e:
                        print(f"Error getting open auction amount: {e}")
                except Exception as e:
                    print(f"Error getting auction data: {e}")

                daily_data.append({
                    'date': str(row['eob'])[:10],
                    'open': float(row['open']),
                    'amount': float(amount),
                    'open_amount': float(open_amount),
                    'close': float(close),
                    'avg_price': avg_price,
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'pre_close': float(pre_close)
                })

            for i, day in enumerate(daily_data):
                open_score = None
                if i > 0 and prev_avg_price and prev_avg_price > 0:
                    open_score = ((day['open'] - prev_avg_price) / prev_avg_price * 1000)
                    open_score = round(open_score, 1)

                bias = ((day['close'] - day['avg_price']) / day['avg_price'] * 100) if day['avg_price'] and day['avg_price'] > 0 else 0

                change_pct = ((day['close'] - day['pre_close']) / day['pre_close'] * 100) if day['pre_close'] and day['pre_close'] > 0 else 0

                open_change_pct = 0
                if day['pre_close'] and day['pre_close'] > 0:
                    open_change_pct = ((day['open'] - day['pre_close']) / day['pre_close'] * 100)
                else:
                    open_change_pct = 0

                result.append({
                    'date': day['date'],
                    'open': round(day['open'], 2),
                    'open_score': open_score,
                    'open_change_pct': round(open_change_pct, 2),
                    'open_amount': round(day['open_amount'], 2),
                    'close': round(day['close'], 2),
                    'avg_price': round(day['avg_price'], 2),
                    'high': round(day['high'], 2),
                    'low': round(day['low'], 2),
                    'change_pct': round(change_pct, 2),
                    'bias': round(bias, 2)
                })

                prev_avg_price = day['avg_price']

            result.reverse()
            return result
        else:
            logger.warning(f"No history data found for {code}")
            return []

    except Exception as e:
        logger.error(f"Error getting stock history: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
