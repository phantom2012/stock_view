import struct
import os
from datetime import datetime, timedelta

GET_STOCK_CODE = "600487"
GET_DATE = "2026-04-17"

TDX_PATH = r"F:\soft\new_tdx64"


def read_tdx_day_data(stock_code: str, target_date: str) -> dict:
    if stock_code.startswith('6'):
        market = 'sh'
        prefix = 'sh'
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        market = 'sz'
        prefix = 'sz'
    else:
        print(f"无法识别股票代码: {stock_code}")
        return {}

    file_path = os.path.join(TDX_PATH, 'vipdoc', market, 'lday', f'{prefix}{stock_code}.day')

    if not os.path.exists(file_path):
        print(f"日线文件不存在: {file_path}")
        return {}

    with open(file_path, 'rb') as f:
        data = f.read()

    record_size = 32
    total_records = len(data) // record_size

    target_record = None
    for i in range(total_records):
        rec = data[i*record_size:(i+1)*record_size]
        date_val = struct.unpack('<I', rec[0:4])[0]
        year = date_val // 10000
        month = (date_val % 10000) // 100
        day = date_val % 100
        date_str = f"{year:04d}-{month:02d}-{day:02d}"

        if date_str == target_date:
            open_px = struct.unpack('<I', rec[4:8])[0] / 100.0
            high_px = struct.unpack('<I', rec[8:12])[0] / 100.0
            low_px = struct.unpack('<I', rec[12:16])[0] / 100.0
            close_px = struct.unpack('<I', rec[16:20])[0] / 100.0
            vol = struct.unpack('<I', rec[20:24])[0]
            amount = struct.unpack('<I', rec[24:28])[0]

            target_record = {
                'date': date_str,
                'open': open_px,
                'high': high_px,
                'low': low_px,
                'close': close_px,
                'volume': vol,
                'amount': amount
            }
            break

    if target_record:
        print(f"找到目标日期 {target_date} 的日线数据")
    else:
        print(f"未找到目标日期 {target_date} 的日线数据")

    return target_record


def read_tdx_minute_data_by_date(stock_code: str, target_date: str) -> tuple:
    if stock_code.startswith('6'):
        market = 'sh'
        prefix = 'sh'
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        market = 'sz'
        prefix = 'sz'
    else:
        print(f"无法识别股票代码: {stock_code}")
        return [], {}

    file_path = os.path.join(TDX_PATH, 'vipdoc', market, 'minline', f'{prefix}{stock_code}.lc1')

    if not os.path.exists(file_path):
        print(f"分时文件不存在: {file_path}")
        return [], {}

    date_groups = {}

    with open(file_path, 'rb') as f:
        data = f.read()

    total_records = len(data) // 32
    print(f'分时文件记录数: {total_records}')

    for i in range(total_records):
        rec = data[i*32:(i+1)*32]
        time_val = struct.unpack("<I", rec[0:4])[0]
        high16 = (time_val >> 16) & 0xFFFF
        low16 = time_val & 0xFFFF

        minutes_from_midnight = high16
        hh = minutes_from_midnight // 60
        mm = minutes_from_midnight % 60

        open_price = struct.unpack("<f", rec[4:8])[0]
        high_price = struct.unpack("<f", rec[8:12])[0]
        low_price = struct.unpack("<f", rec[12:16])[0]
        close_price = struct.unpack("<f", rec[16:20])[0]
        vol = struct.unpack("<I", rec[20:24])[0]
        amount_lo = struct.unpack("<I", rec[24:28])[0]
        amount_hi = struct.unpack("<I", rec[28:32])[0]
        amount = (amount_hi << 32) | amount_lo

        record = {
            'time': f"{hh:02d}:{mm:02d}",
            'hour': hh,
            'minute': mm,
            'price': close_price,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': vol,
            'amount': amount,
            'low16': low16
        }

        if low16 not in date_groups:
            date_groups[low16] = []
        date_groups[low16].append(record)

    low16_list = sorted(date_groups.keys())
    base_low16 = low16_list[0]
    base_date = datetime(2026, 4, 17)

    found_low16 = None
    for low16 in low16_list:
        days_diff = low16 - base_low16
        inferred_date = base_date + timedelta(days=days_diff)
        if inferred_date.strftime('%Y-%m-%d') == target_date:
            found_low16 = low16
            break

    if found_low16 is None:
        for low16 in low16_list:
            days_diff = low16 - base_low16
            inferred_date = base_date + timedelta(days=days_diff)
            if target_date[:7] in inferred_date.strftime('%Y-%m-%d'):
                if found_low16 is None:
                    found_low16 = low16

    if found_low16 is None:
        found_low16 = low16_list[0]

    target_records = date_groups.get(found_low16, [])
    print(f'目标日期 {target_date} 记录数: {len(target_records)}')
    if target_records:
        print(f'时间范围: {target_records[0]["time"]} - {target_records[-1]["time"]}')
        print(f'注: .lc1分时数据从 09:31 开始，不包含 9:15-9:25 竞价时段')

    return target_records, {}


def get_auction_data_from_minute_data(minute_data: list, day_data: dict) -> dict:
    result = {
        'auction_price': 0,
        'auction_amount': 0,
        'open_price': 0,
        'open_amount': 0,
        'has_auction_data': False,
        'note': ''
    }

    if not minute_data:
        result['note'] = '没有分时数据'
        return result

    auction_data = [d for d in minute_data if d['hour'] == 9 and 15 <= d['minute'] <= 25]

    if not auction_data:
        result['note'] = '.lc1分时数据不包含9:15-9:25竞价时段 (数据从09:31开始)'
        if day_data:
            result['open_price'] = day_data['open']
            result['note'] += f"，使用日线开盘价 {day_data['open']} 作为参考"
        if minute_data:
            result['open_amount'] = minute_data[0]['amount']
        return result

    result['has_auction_data'] = True

    auction_at_925 = [d for d in auction_data if d['minute'] == 25]
    if auction_at_925:
        result['auction_price'] = auction_at_925[-1]['price']
        result['auction_amount'] = auction_at_925[-1]['amount']
    else:
        result['auction_price'] = auction_data[-1]['price']
        result['auction_amount'] = auction_data[-1]['amount']

    open_at_930 = [d for d in minute_data if d['hour'] == 9 and d['minute'] == 30]
    if open_at_930:
        result['open_price'] = open_at_930[0]['price']
        result['open_amount'] = open_at_930[0]['amount']

    return result


if __name__ == "__main__":
    print(f"=" * 60)
    print(f"读取股票 {GET_STOCK_CODE} 在 {GET_DATE} 的数据")
    print(f"=" * 60)

    print('\n--- 日线数据 ---')
    day_data = read_tdx_day_data(GET_STOCK_CODE, GET_DATE)
    if day_data:
        print(f"日期: {day_data['date']}")
        print(f"开盘价: {day_data['open']}")
        print(f"最高价: {day_data['high']}")
        print(f"最低价: {day_data['low']}")
        print(f"收盘价: {day_data['close']}")
        print(f"成交量: {day_data['volume']}")
        print(f"成交额: {day_data['amount']}")

    print('\n--- 分时数据 ---')
    minute_data, _ = read_tdx_minute_data_by_date(GET_STOCK_CODE, GET_DATE)

    result = get_auction_data_from_minute_data(minute_data, day_data)

    print(f"\n--- 早盘竞价数据 ---")
    print(f"竞价价 (9:25最后一笔): {result['auction_price']}")
    print(f"竞价成交额 (9:25最后一笔): {result['auction_amount']}")
    print(f"开盘价 (9:30第一笔): {result['open_price']}")
    print(f"开盘成交额 (9:30第一笔): {result['open_amount']}")
    print(f"备注: {result['note']}")

    if minute_data:
        print(f"\n当日分时数据样本 (前10条):")
        for d in minute_data[:10]:
            print(f"  {d['time']} - 开:{d['open']:.2f} 高:{d['high']:.2f} 低:{d['low']:.2f} 收:{d['close']:.2f} 额:{d['amount']}")