from gm.api import *
from datetime import datetime
import sys
import os

# 获取数据类型
# 1-获取当日开盘，收盘以及涨幅信息
# 2-获取股价最新快照信息
# 3-获取盘口信息，最新20条
# 4-订阅获取股票实时快照
# 5-获取股票指定日期的开盘成交额
# 6-获取股票指定日期的分钟分时数据
# 7-获取股票基础信息（总股本、流通股本、市盈率等）
GET_DATA_TYPE = 7
GET_STOCK_CODE = "600487"
GET_DATE = "2026-04-30"

# API Token
API_KEY = "2e664976b46df6a0903672349c30226ac68e7bf3"

# 全局变量用于存储订阅的股票
SUBSCRIBE_SYMBOL = None

# 从环境变量读取订阅的股票（用于解决run()重新import模块导致的全局变量丢失问题）
import os
_env_symbol = os.environ.get('GM_SUBSCRIBE_SYMBOL')
if _env_symbol:
    SUBSCRIBE_SYMBOL = _env_symbol


def init(context):
    """
    策略初始化函数 - 掘金API策略入口
    """
    if SUBSCRIBE_SYMBOL:
        # 订阅tick数据
        subscribe(symbols=SUBSCRIBE_SYMBOL, frequency='tick')
        print(f"✅ 已订阅股票: {SUBSCRIBE_SYMBOL}")
    else:
        print(f"⚠️ 警告: 未设置订阅股票!")


def on_tick(context, tick):
    """
    tick数据回调函数 - 实时接收tick数据
    """
    try:
        print(f"\n{'='*80}")
        print(f"【实时快照】时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        print(f"股票代码:   {tick.symbol}")
        print(f"最新价格:   {tick.price}")
        print(f"成交量:     {tick.volume}")
        print(f"累计成交量: {tick.cum_volume}")
        print(f"累计成交额: {tick.cum_amount}")

        # 打印买卖盘口 - quotes是字典列表结构
        if hasattr(tick, 'quotes') and tick.quotes:
            print(f"\n--- 买卖盘口 ---")
            # 卖盘 (从卖5到卖1)
            print("  卖盘:")
            for i in range(5, 0, -1):
                if i <= len(tick.quotes):
                    quote = tick.quotes[i-1]
                    ask_p = quote.get('ask_p', 'N/A') if isinstance(quote, dict) else getattr(quote, 'ask_p', 'N/A')
                    ask_v = quote.get('ask_v', 'N/A') if isinstance(quote, dict) else getattr(quote, 'ask_v', 'N/A')
                    print(f"    卖{i}: {str(ask_p):<10} 量: {str(ask_v):<10}")

            # 买盘 (从买1到买5)
            print("  买盘:")
            for i in range(1, 6):
                if i <= len(tick.quotes):
                    quote = tick.quotes[i-1]
                    bid_p = quote.get('bid_p', 'N/A') if isinstance(quote, dict) else getattr(quote, 'bid_p', 'N/A')
                    bid_v = quote.get('bid_v', 'N/A') if isinstance(quote, dict) else getattr(quote, 'bid_v', 'N/A')
                    print(f"    买{i}: {str(bid_p):<10} 量: {str(bid_v):<10}")

        print(f"{'='*80}")
    except Exception as e:
        print(f"处理tick数据失败: {e}")
        import traceback
        traceback.print_exc()


def get_daily_info(symbol):
    """
    类型1: 获取当日开盘，收盘以及涨幅信息
    使用 history 接口获取当天的日线数据
    """
    print(f"=== 获取股票 {symbol} 的当日行情信息 ===")
    set_token(API_KEY)

    today = datetime.now().strftime('%Y-%m-%d')
    start_time = f"{today} 00:00:00"
    end_time = f"{today} 23:59:59"

    try:
        # 先获取所有可用字段
        data_raw = history(
            symbol=symbol,
            frequency='1d',
            start_time=start_time,
            end_time=end_time,
            fields='symbol,bob,eob,open,close,high,low,volume,amount,pre_close,vwap,stop_status,suspend_status',
            df=True
        )

        if data_raw is not None and len(data_raw) > 0:
            row = data_raw.iloc[0]

            print(f"\n{'='*80}")
            print(f"【完整字段数据】")
            print(f"{'='*80}")
            for key, value in row.items():
                print(f"  {key:<20}: {value}")

            # 提取常用字段进行格式化显示
            open_price = row['open']
            close_price = row['close']
            pre_close = row['pre_close']
            high = row['high']
            low = row['low']
            volume = row['volume']
            amount = row['amount']

            # 计算涨跌幅
            if pre_close and close_price:
                change = close_price - pre_close
                change_percent = (change / pre_close) * 100
            else:
                change = 0
                change_percent = 0

            # 成交额换算为亿
            amount_in_yi = amount / 100000000 if amount else 0

            print(f"\n{'='*80}")
            print(f"【格式化显示】")
            print(f"{'='*80}")
            print(f"股票代码:   {row['symbol']}")
            print(f"交易日期:   {row['eob']}")
            print(f"昨收价:     {pre_close:.2f}")
            print(f"开盘价:     {open_price:.2f}")
            print(f"收盘价:     {close_price:.2f}")
            print(f"最高价:     {high:.2f}")
            print(f"最低价:     {low:.2f}")
            print(f"涨跌幅:     {change:+.2f} ({change_percent:+.2f}%)")
            print(f"成交量:     {volume:,.0f} 手")
            print(f"成交额:     {amount_in_yi:.2f} 亿")
            if 'vwap' in row:
                print(f"均价:       {row['vwap']:.2f}")
            print(f"{'='*80}")
        else:
            print("\n❌ 未获取到当日行情数据（可能非交易日或数据未更新）")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


def get_current_snapshot(symbol):
    """
    类型2: 获取股价最新快照信息
    使用 current 接口获取实时行情
    """
    print(f"=== 获取股票 {symbol} 的最新快照信息 ===")
    set_token(API_KEY)

    try:
        # 获取所有可用字段
        data = current(
            symbols=symbol,
            fields='symbol,open,price,high,low,pre_close,volume,amount,last_close,eob,ask_price1,ask_price2,ask_price3,ask_price4,ask_price5,bid_price1,bid_price2,bid_price3,bid_price4,bid_price5,ask_volume1,ask_volume2,ask_volume3,ask_volume4,ask_volume5,bid_volume1,bid_volume2,bid_volume3,bid_volume4,bid_volume5'
        )

        if data:
            item = data[0]

            print(f"\n{'='*80}")
            print(f"【完整字段数据】")
            print(f"{'='*80}")
            for key, value in item.items():
                print(f"  {key:<20}: {value}")

            # 提取常用字段进行格式化显示
            pre_close = item.get('pre_close')
            price = item.get('price')
            if pre_close and price:
                change = price - pre_close
                change_percent = (change / pre_close) * 100
            else:
                change = 0
                change_percent = 0

            print(f"\n{'='*80}")
            print(f"【格式化显示】")
            print(f"{'='*80}")
            print(f"股票代码:   {item.get('symbol', 'N/A')}")
            print(f"快照时间:   {item.get('eob', 'N/A')}")
            print(f"昨收价:     {item.get('pre_close', 'N/A')}")
            print(f"开盘价:     {item.get('open', 'N/A')}")
            print(f"当前价:     {item.get('price', 'N/A')}")
            print(f"最高价:     {item.get('high', 'N/A')}")
            print(f"最低价:     {item.get('low', 'N/A')}")
            print(f"涨跌幅:     {change:+.2f} ({change_percent:+.2f}%)")
            print(f"成交量:     {item.get('volume', 'N/A')}")
            print(f"成交额:     {item.get('amount', 'N/A')}")

            # 五档行情
            print(f"\n--- 五档行情 ---")
            for i in range(5, 0, -1):
                ask_p = item.get(f'ask_price{i}', 'N/A')
                ask_v = item.get(f'ask_volume{i}', 'N/A')
                print(f"  卖{i}: {str(ask_p):<10} 量: {str(ask_v):<10}")

            print(f"  {'-'*30}")

            for i in range(1, 6):
                bid_p = item.get(f'bid_price{i}', 'N/A')
                bid_v = item.get(f'bid_volume{i}', 'N/A')
                print(f"  买{i}: {str(bid_p):<10} 量: {str(bid_v):<10}")

            print(f"{'='*80}")
        else:
            print("\n❌ 未获取到实时行情数据")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


def get_order_book(symbol):
    """
    类型3: 获取盘口信息，最新20条
    使用 history_n 接口获取最近20条tick数据
    """
    print(f"=== 获取股票 {symbol} 的最新20条盘口信息 ===")
    set_token(API_KEY)

    try:
        data = history_n(
            symbol=symbol,
            frequency='tick',
            count=20,
            fields='symbol,price,volume,cum_volume,cum_amount,quotes,created_at'
        )

        if data:
            print(f"\n✅ 成功获取 {len(data)} 条tick数据")
            print(f"\n{'序号':<6} {'时间':<22} {'价格':<10} {'累计成交量':<15} {'累计成交额':<15} {'买一价':<10} {'买一量':<10} {'卖一价':<10} {'卖一量':<10}")
            print("-" * 120)

            for i, item in enumerate(data):
                price = item.get('price', 'N/A')
                cum_volume = item.get('cum_volume', 'N/A')
                cum_amount = item.get('cum_amount', 'N/A')
                created_at = item.get('created_at', 'N/A')
                quotes = item.get('quotes', [])

                # 获取买卖一档
                bid_p1 = quotes[0]['bid_p'] if quotes and len(quotes) > 0 else 'N/A'
                bid_v1 = quotes[0]['bid_v'] if quotes and len(quotes) > 0 else 'N/A'
                ask_p1 = quotes[0]['ask_p'] if quotes and len(quotes) > 0 else 'N/A'
                ask_v1 = quotes[0]['ask_v'] if quotes and len(quotes) > 0 else 'N/A'

                time_str = str(created_at)[-19:] if created_at else 'N/A'

                print(f"{i+1:<6} {time_str:<22} {str(price):<10} {str(cum_volume):<15} {str(cum_amount):<15} {str(bid_p1):<10} {str(bid_v1):<10} {str(ask_p1):<10} {str(ask_v1):<10}")

            print(f"\n{'='*120}")
        else:
            print("\n❌ 未获取到tick数据")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


def get_realtime_snapshot(symbol):
    """
    类型4: 订阅获取股票实时快照
    使用掘金API策略模式订阅实时tick数据

    由于掘金API的策略运行机制，此函数只设置环境变量
    实际的策略运行在 __main__ 块中通过 run() 实现
    """
    print(f"=== 订阅股票 {symbol} 的实时快照信息 ===")
    print("按 Ctrl+C 退出订阅\n")

    # 设置环境变量，用于在 run() 重新 import 模块后仍能获取到订阅的股票
    os.environ['GM_SUBSCRIBE_SYMBOL'] = symbol


def get_open_call_auction_amount(symbols, trade_date):
    """
    类型5: 获取股票指定日期的开盘成交额
    使用掘金API的 history 函数获取tick数据，通过计算9:30第一个tick与9:30之前最后一个tick的差值来获取开盘竞价成交额
    """
    print(f"=== 获取股票 {symbols} 在 {trade_date} 的开盘竞价成交额 ===")
    set_token(API_KEY)

    try:
        # 获取9:25-9:31的tick数据
        df = history(
            symbol=symbols,
            frequency='tick',
            start_time=f"{trade_date} 09:25:00",
            end_time=f"{trade_date} 09:31:00",
            fields='symbol,price,volume,cum_amount,cum_volume,created_at',
            df=True
        )

        if df is not None and not df.empty:
            import pandas as pd
            df['created_at'] = pd.to_datetime(df['created_at'])

            # 找出9:30之前最后一个tick
            before_930 = df[df['created_at'] < f'{trade_date} 09:30:00']
            # 找出9:30的第一个tick
            at_930 = df[(df['created_at'] >= f'{trade_date} 09:30:00') & (df['created_at'] < f'{trade_date} 09:30:01')]

            if not before_930.empty and not at_930.empty:
                last_vol = before_930.iloc[-1]['cum_volume']
                last_amt = before_930.iloc[-1]['cum_amount']
                first_vol = at_930.iloc[0]['cum_volume']
                first_amt = at_930.iloc[0]['cum_amount']
                open_volume = first_vol - last_vol
                open_amount = first_amt - last_amt

                print(f"\n✅ 成功获取开盘竞价数据")
                print(f"\n{'='*80}")
                print(f"股票代码: {symbols}")
                print(f"交易日期: {trade_date}")
                print(f"{'='*80}")
                print(f"竞价结束(9:29:09) 累计成交量: {last_vol:>15,}  累计成交额: {last_amt:>18,.2f}")
                print(f"开盘时刻(9:30:00) 累计成交量: {first_vol:>15,}  累计成交额: {first_amt:>18,.2f}")
                print(f"{'='*80}")
                print(f"开盘竞价成交量: {open_volume:>15,}")
                print(f"开盘竞价成交额: {open_amount:>18,.2f}")
                print(f"{'='*80}")
            else:
                print("\n❌ 未获取到有效的竞价数据")
        else:
            print("\n❌ 未获取到tick数据，可能当日为非交易日或数据未更新")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


def get_minute_data(symbol, trade_date):
    """
    类型6: 获取股票指定日期的分钟级别的分时数据
    使用掘金API的history接口直接获取数据，并打印早上10点之前的数据
    """
    print(f"=== 获取股票 {symbol} 在 {trade_date} 的分钟分时数据（10点前） ===")
    set_token(API_KEY)

    try:
        # 获取9:30-10:00的分钟数据
        start_time = f"{trade_date} 09:30:00"
        end_time = f"{trade_date} 10:00:00"

        minute_data = history(
            symbol=symbol,
            frequency='60s',
            start_time=start_time,
            end_time=end_time,
            fields='symbol,eob,open,close,high,low,volume,amount',
            df=True
        )

        if minute_data is not None and not minute_data.empty:
            print(f"\n✅ 成功获取分钟数据，共 {len(minute_data)} 条记录")
            print(f"\n{'='*120}")
            print(f"{'时间':<22} {'开盘价':<12} {'收盘价':<12} {'最高价':<12} {'最低价':<12} {'成交量':<15} {'成交额':<20}")
            print("-" * 120)

            total_volume = 0
            total_amount = 0

            for idx, row in minute_data.iterrows():
                time_val = row.get('eob', 'N/A')
                open_price = row.get('open', 'N/A')
                close_price = row.get('close', 'N/A')
                high_price = row.get('high', 'N/A')
                low_price = row.get('low', 'N/A')
                volume = row.get('volume', 'N/A')
                amount = row.get('amount', 'N/A')

                total_volume += volume if volume else 0
                total_amount += amount if amount else 0

                print(f"{str(time_val):<22} {open_price:<12.2f} {close_price:<12.2f} {high_price:<12.2f} {low_price:<12.2f} {volume:<15.0f} {amount:<20.2f}")

            print(f"{'='*120}")
            print(f"\n【10点前数据汇总】")
            print(f"  总成交量: {total_volume:,.0f}")
            print(f"  总成交额: {total_amount:,.2f}")
            print(f"{'='*120}")
        else:
            print("\n❌ 未获取到分钟数据，可能当日为非交易日或数据未更新")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


def get_stock_basic_info(symbol, trade_date=None):
    """
    类型7: 获取股票基础信息（基本信息、估值指标、市值等）
    使用掘金API的多个接口组合获取股票基础信息
    """
    print(f"=== 获取股票 {symbol} 的基础信息 ===")
    set_token(API_KEY)

    if trade_date is None:
        trade_date = datetime.now().strftime('%Y-%m-%d')

    try:
        # 使用 get_instruments 获取股票基本信息
        basic_info = get_instruments(symbols=symbol)

        # 使用 stk_get_daily_valuation_pt 获取估值信息
        valuation_info = stk_get_daily_valuation_pt(
            symbols=symbol,
            fields='pe_ttm,pe_lyr,pe_mrq,ps_ttm'
        )

        # 使用 current 获取当前价格
        current_info = current(symbols=symbol, fields='symbol,price')

        # 使用指数成分股接口获取市值信息（使用上证指数，覆盖范围最广）
        market_value = None
        try:
            # 使用上证指数获取市值信息
            constituents = stk_get_index_constituents(index='SHSE.000001')
            if constituents is not None and not constituents.empty:
                stock_data = constituents[constituents['symbol'] == symbol]
                if not stock_data.empty:
                    market_value = {
                        'total_mv': stock_data.iloc[0].get('market_value_total'),
                        'circ_mv': stock_data.iloc[0].get('market_value_circ')
                    }
        except Exception as e:
            print(f"获取市值信息失败: {e}")

        print(f"\n{'='*80}")
        print(f"【基本信息】")
        print(f"{'='*80}")

        stock_name = 'N/A'
        if basic_info and len(basic_info) > 0:
            item = basic_info[0]
            stock_name = item.get('sec_name', 'N/A')
            print(f"股票代码:       {item.get('symbol', 'N/A')}")
            print(f"股票名称:       {stock_name}")
            print(f"股票简称:       {item.get('sec_abbr', 'N/A')}")
            print(f"交易所:         {item.get('exchange', 'N/A')}")
            listed_date = item.get('listed_date')
            if listed_date:
                print(f"上市日期:       {listed_date.strftime('%Y-%m-%d')}")
            else:
                print(f"上市日期:       N/A")
            print(f"昨收价格:       {item.get('pre_close', 'N/A'):.2f}")
            print(f"涨停价:         {item.get('upper_limit', 'N/A'):.2f}")
            print(f"跌停价:         {item.get('lower_limit', 'N/A'):.2f}")
            print(f"复权因子:       {item.get('adj_factor', 'N/A'):.6f}")
        else:
            print("未获取到基本信息")

        print(f"\n{'='*80}")
        print(f"【当前行情】")
        print(f"{'='*80}")

        if current_info and len(current_info) > 0:
            cur_item = current_info[0]
            print(f"当前价格:       {cur_item.get('price', 'N/A'):.2f}")
        else:
            print("未获取到当前行情")

        print(f"\n{'='*80}")
        print(f"【估值指标】")
        print(f"{'='*80}")

        if valuation_info and len(valuation_info) > 0:
            val_item = valuation_info[0]
            print(f"交易日期:       {val_item.get('trade_date', 'N/A')}")
            print(f"市盈率(TTM):    {val_item.get('pe_ttm', 'N/A'):.4f}")
            print(f"市盈率(LYR):    {val_item.get('pe_lyr', 'N/A'):.4f}")
            print(f"市盈率(MRQ):    {val_item.get('pe_mrq', 'N/A'):.4f}")
            print(f"市销率(TTM):    {val_item.get('ps_ttm', 'N/A'):.4f}")
        else:
            print("未获取到估值信息")

        print(f"\n{'='*80}")
        print(f"【市值信息】")
        print(f"{'='*80}")

        if market_value:
            print(f"总市值(亿元):   {market_value.get('total_mv', 'N/A'):.4f}")
            print(f"流通市值(亿元): {market_value.get('circ_mv', 'N/A'):.4f}")
        else:
            print("未获取到市值信息（股票可能不在主要指数成分股中）")

        print(f"{'='*80}")

    except Exception as e:
        print(f"\n❌ 查询失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 构建完整的股票代码（假设是深交所）
    if GET_STOCK_CODE.startswith('6'):
        SYMBOL = f"SHSE.{GET_STOCK_CODE}"
    else:
        SYMBOL = f"SZSE.{GET_STOCK_CODE}"

    print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"目标股票: {GET_STOCK_CODE} ({SYMBOL})")
    print(f"数据类型: {GET_DATA_TYPE}\n")

    if GET_DATA_TYPE == 1:
        get_daily_info(SYMBOL)
    elif GET_DATA_TYPE == 2:
        get_current_snapshot(SYMBOL)
    elif GET_DATA_TYPE == 3:
        get_order_book(SYMBOL)
    elif GET_DATA_TYPE == 4:
        # Type 4: 使用策略模式运行
        get_realtime_snapshot(SYMBOL)

        # 启动掘金策略，run() 会自动调用模块级别的 init 和 on_tick 函数
        # 关键修复：清理 sys.path，只保留当前目录，避免掘金API产生错误的模块名
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 完全替换 sys.path，只保留当前目录
        sys.path = [current_dir]

        # 关键修复：将 __file__ 转换为正斜杠路径，避免 Windows 下 os.path.commonprefix 匹配失败
        # 掘金API的 run() 函数会用 sys.path 中的路径（已转换为正斜杠）与 filename 进行 commonprefix 比较
        filename_for_run = __file__.replace('\\', '/')

        run(
            strategy_id='realtime_snapshot_strategy',
            filename=filename_for_run,
            mode=MODE_LIVE,
            token=API_KEY
        )
    elif GET_DATA_TYPE == 5:
        # Type 5: 获取股票指定日期的开盘成交额
        get_open_call_auction_amount(SYMBOL, GET_DATE)
    elif GET_DATA_TYPE == 6:
        # Type 6: 获取股票指定日期的分钟分时数据
        get_minute_data(SYMBOL, GET_DATE)
    elif GET_DATA_TYPE == 7:
        # Type 7: 获取股票基础信息（总股本、流通股本、市盈率等）
        get_stock_basic_info(SYMBOL, GET_DATE)
    else:
        print(f"❌ 不支持的数据类型: {GET_DATA_TYPE}")
        print("支持的数据类型:")
        print("  1 - 获取当日开盘，收盘以及涨幅信息")
        print("  2 - 获取股价最新快照信息")
        print("  3 - 获取盘口信息，最新20条")
        print("  4 - 订阅获取股票实时快照")
        print("  5 - 获取股票指定日期的开盘成交额")
        print("  6 - 获取股票指定日期的分钟分时数据")
        print("  7 - 获取股票基础信息（总股本、流通股本、市盈率等）")
