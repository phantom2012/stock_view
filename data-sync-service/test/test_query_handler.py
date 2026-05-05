import sys
sys.path.append('f:\\gupiao\\stock_view\\data-sync-service')

# 共用配置 - 用户只需修改这里
STOCK_CODE = "000062"  # 股票代码
TRADE_DATE = "2026-04-30"  # 交易日期

def _code_to_symbol(code: str) -> str:
    """将股票代码转换为掘金symbol格式"""
    if code.startswith('6'):
        return f"SHSE.{code}"
    elif code.startswith('0') or code.startswith('3'):
        return f"SZSE.{code}"
    else:
        return f"SHSE.{code}"

from external_data.query_handler import get_query_handler

def test_get_minute_data():
    handler = get_query_handler()

    symbol = _code_to_symbol(STOCK_CODE)
    start_time = "14:55:00"
    end_time = "15:00:00"

    print(f"测试 get_minute_data: {symbol}, {TRADE_DATE} {start_time}-{end_time}")
    print("=" * 80)

    try:
        df = handler.get_minute_data(symbol, TRADE_DATE, start_time, end_time)
        if df is not None and not df.empty:
            print(f"成功获取 {len(df)} 条分钟数据")
            print(df)
        else:
            print("未获取到数据 - 注意：掘金分钟数据通常从09:30开始，09:00-09:30可能无数据")
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

    print("=" * 80)
    print()

def test_get_tick_data_auction():
    handler = get_query_handler()

    symbol = _code_to_symbol(STOCK_CODE)
    start_time = "09:24:59"
    end_time = "09:30:00"

    print(f"测试 get_tick_data (早盘竞价): {symbol}, {TRADE_DATE} {start_time}-{end_time}")
    print("=" * 80)

    try:
        df = handler.get_tick_data(symbol, TRADE_DATE, start_time, end_time)
        if df is not None and not df.empty:
            print(f"成功获取 {len(df)} 条Tick数据")

            non_zero_prices = df[df['price'] > 0]
            if not non_zero_prices.empty:
                print("\n非零价格数据:")
                print(non_zero_prices)
            else:
                print(df.head(20))
                if len(df) > 20:
                    print(f"... 还有 {len(df) - 20} 条数据")

        else:
            print("未获取到数据")
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

    print("=" * 80)
    print()

if __name__ == "__main__":
    print("=" * 80)
    print("测试 ExternalDataQueryHandler 接口")
    print("=" * 80)
    print(f"配置: code={STOCK_CODE}, date={TRADE_DATE}, symbol={_code_to_symbol(STOCK_CODE)}")
    print("=" * 80)
    print()

    test_get_minute_data()
    test_get_tick_data_auction()
