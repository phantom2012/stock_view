import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'data-sync-service'))

import tushare as ts
import pandas as pd
from datetime import datetime, timedelta

from external_data.tushare_query import TushareQuery

GET_STOCK_CODE = "001309"
GET_DATE = "2026-05-19"

# 运行模式配置
# RUN_MODE = 1: 查询当日竞价数据（早盘+尾盘）
# RUN_MODE = 2: 使用Tushare stk_mins接口查询9:30后开盘信息
# RUN_MODE = 3: 使用Tushare stk_auction_o接口获取早盘竞价信息
# RUN_MODE = 4: 使用Tushare moneyflow_ths接口获取资金流向数据
# RUN_MODE = 5: 使用掘金接口查询9:30后开盘快照
# RUN_MODE = 6: 使用Tushare moneyflow_dc接口获取资金信息
# RUN_MODE = 7: 使用Tushare daily_basic接口获取每日指标（基本面信息）
# RUN_MODE = 8: 使用Tushare stock_basic接口查询股票基本信息
# RUN_MODE = 9: 使用Tushare fina_indicator接口查询财务数据
# RUN_MODE = 10: 使用Tushare index_dailybasic接口获取德明利主营行业的PE、PB均值
RUN_MODE = 6

# Tushare API Token
TUSHARE_API_TOKEN = "17bf2b4e7bffa84e9b02a52f026df310c03badcb29c63533e935353c"
# Tushare 代理地址
TUSHARE_PROXY_URL = "http://121.40.135.59:8010/"

def get_auction_data():
    """
    使用Tushare API获取指定股票指定日期的竞价数据
    """
    try:
        # 初始化Tushare Pro API
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        # 设置代理地址
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        # 构建Tushare格式的股票代码 (600487.SH 或 000001.SZ)
        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        print(f"正在获取 {ts_code} 在 {GET_DATE} 的竞价数据...")

        # 调用Tushare的stk_auction接口获取竞价数据
        df = pro.stk_auction(
            ts_code=ts_code,
            trade_date=trade_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {GET_DATE} 的竞价数据")
            return

        print(f"\n{'='*80}")
        print(f"竞价数据概览:")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        # 显示所有竞价数据记录（早盘和尾盘）
        print(f"\n完整竞价数据:")
        print(df.to_string(index=False))

        # 分别显示早盘和尾盘竞价数据
        if 'auction_type' in df.columns:
            morning_auction = df[df['auction_type'] == 'morning']  # 早盘竞价
            evening_auction = df[df['auction_type'] == 'evening']  # 尾盘竞价（如果有）

            if not morning_auction.empty:
                print(f"\n{'='*80}")
                print(f"早盘竞价数据 (09:15-09:25):")
                print(f"{'='*80}")
                for _, row in morning_auction.iterrows():
                    print(f"  时间: {row.get('trade_time', 'N/A')}")
                    print(f"  价格: {row.get('price', 'N/A')}")
                    print(f"  成交量: {row.get('volume', 'N/A')}")
                    print(f"  成交额: {row.get('amount', 'N/A')}")
                    print(f"  买一价: {row.get('bid_price_1', 'N/A')}  买一量: {row.get('bid_vol_1', 'N/A')}")
                    print(f"  卖一价: {row.get('ask_price_1', 'N/A')}  卖一量: {row.get('ask_vol_1', 'N/A')}")
                    print()

            if not evening_auction.empty:
                print(f"\n{'='*80}")
                print(f"尾盘竞价数据 (14:57-15:00):")
                print(f"{'='*80}")
                for _, row in evening_auction.iterrows():
                    print(f"  时间: {row.get('trade_time', 'N/A')}")
                    print(f"  价格: {row.get('price', 'N/A')}")
                    print(f"  成交量: {row.get('volume', 'N/A')}")
                    print(f"  成交额: {row.get('amount', 'N/A')}")
                    print(f"  买一价: {row.get('bid_price_1', 'N/A')}  买一量: {row.get('bid_vol_1', 'N/A')}")
                    print(f"  卖一价: {row.get('ask_price_1', 'N/A')}  卖一量: {row.get('ask_vol_1', 'N/A')}")
                    print()
        else:
            # 如果没有auction_type字段，直接显示所有数据
            print(f"\n{'='*80}")
            print(f"竞价详情:")
            print(f"{'='*80}")
            for idx, row in df.iterrows():
                print(f"\n记录 {idx + 1}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]}")

        # 统计信息
        print(f"\n{'='*80}")
        print(f"竞价数据统计:")
        print(f"{'='*80}")
        if 'price' in df.columns:
            print(f"  竞价价格范围: {df['price'].min()} - {df['price'].max()}")
        if 'volume' in df.columns:
            print(f"  总成交量: {df['volume'].sum()}")
        if 'amount' in df.columns:
            print(f"  总成交额: {df['amount'].sum():.2f}元")

    except Exception as e:
        print(f"获取竞价数据失败: {e}")
        import traceback
        traceback.print_exc()

def get_opening_snapshot_tushare():
    """
    使用Tushare API获取指定股票指定日期9:30后第一张快照的开盘信息
    通过stk_mins接口获取9:30-9:31的K线数据（模式2）
    """
    try:
        # 初始化Tushare Pro API
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        # 设置代理地址
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        # 构建Tushare格式的股票代码
        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        print(f"正在获取 {ts_code} 在 {GET_DATE} 9:30后的开盘快照...")

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        # 尝试使用stk_mins接口获取9:30的分钟数据
        try:
            print(f"使用stk_mins接口（模式2）获取开盘信息...")
            df = pro.stk_mins(
                ts_code=ts_code,
                freq='1min',
                start_date=f"{trade_date} 09:30:00",
                end_date=f"{trade_date} 09:35:00"
            )

            if df is not None and not df.empty:
                # 获取第一条分钟数据（9:30-9:31）
                first_bar = df.iloc[0]

                print(f"\n{'='*80}")
                print(f"9:30后第一张分钟K线快照 (模式2 - stk_mins接口):")
                print(f"{'='*80}")
                print(f"  时间: {first_bar.get('time', 'N/A')}")
                print(f"  开盘价: {first_bar.get('open', 'N/A')}")
                print(f"  收盘价: {first_bar.get('close', 'N/A')}")
                print(f"  最高价: {first_bar.get('high', 'N/A')}")
                print(f"  最低价: {first_bar.get('low', 'N/A')}")
                print(f"  成交量: {first_bar.get('vol', 'N/A')}")
                print(f"  成交额: {first_bar.get('amount', 'N/A')}")

                # 显示前5条分钟数据
                if len(df) > 1:
                    print(f"\n{'='*80}")
                    print(f"前5条分钟K线数据:")
                    print(f"{'='*80}")
                    print(f"\n{'序号':<6} {'时间':<20} {'开盘':<10} {'收盘':<10} {'最高':<10} {'最低':<10} {'成交量':<12} {'成交额':<15}")
                    print("-" * 100)

                    for i, (_, row) in enumerate(df.head(5).iterrows()):
                        time_str = str(row.get('time', 'N/A'))
                        open_price = row.get('open', 0)
                        close_price = row.get('close', 0)
                        high_price = row.get('high', 0)
                        low_price = row.get('low', 0)
                        volume = row.get('vol', 0)
                        amount = row.get('amount', 0)

                        print(f"{i+1:<6} {time_str:<20} {open_price:<10.2f} {close_price:<10.2f} {high_price:<10.2f} {low_price:<10.2f} {volume:<12,.0f} {amount:<15.2f}")

                # 统计信息
                print(f"\n{'='*80}")
                print(f"开盘数据统计:")
                print(f"{'='*80}")
                print(f"  开盘价: {first_bar.get('open', 'N/A')}")
                print(f"  当前价: {first_bar.get('close', 'N/A')}")
                print(f"  涨跌幅: {((first_bar.get('close', 0) - first_bar.get('open', 0)) / first_bar.get('open', 1) * 100):.2f}%" if first_bar.get('open', 0) != 0 else "  涨跌幅: N/A")
                print(f"  成交量: {first_bar.get('vol', 'N/A')}")
                print(f"  成交额: {first_bar.get('amount', 'N/A'):.2f}元")
                return
        except Exception as stk_mins_error:
            print(f"stk_mins接口失败: {stk_mins_error}")
            print("尝试使用daily接口获取开盘信息...")

        # 备选方案: 使用daily接口获取日K线数据
        try:
            df = pro.daily(
                ts_code=ts_code,
                trade_date=trade_date
            )

            if df is None or df.empty:
                print(f"未获取到 {ts_code} 在 {GET_DATE} 的日K线数据")
                return

            # 获取日K线数据
            daily_data = df.iloc[0]

            print(f"\n{'='*80}")
            print(f"9:30后开盘信息 (模式2 - daily接口):")
            print(f"{'='*80}")
            print(f"  日期: {daily_data.get('trade_date', 'N/A')}")
            print(f"  开盘价: {daily_data.get('open', 'N/A')}")
            print(f"  收盘价: {daily_data.get('close', 'N/A')}")
            print(f"  最高价: {daily_data.get('high', 'N/A')}")
            print(f"  最低价: {daily_data.get('low', 'N/A')}")
            print(f"  成交量: {daily_data.get('vol', 'N/A')}")
            print(f"  成交额: {daily_data.get('amount', 'N/A')}")

            # 统计信息
            print(f"\n{'='*80}")
            print(f"开盘数据统计:")
            print(f"{'='*80}")
            print(f"  开盘价: {daily_data.get('open', 'N/A')}")
            print(f"  收盘价: {daily_data.get('close', 'N/A')}")
            print(f"  涨跌幅: {((daily_data.get('close', 0) - daily_data.get('open', 0)) / daily_data.get('open', 1) * 100):.2f}%" if daily_data.get('open', 0) != 0 else "  涨跌幅: N/A")
            print(f"  成交量: {daily_data.get('vol', 'N/A')}")
            print(f"  成交额: {daily_data.get('amount', 'N/A'):.2f}元")
        except Exception as daily_error:
            print(f"daily接口也失败: {daily_error}")
            print("无法获取开盘信息")

    except Exception as e:
        print(f"获取开盘快照失败: {e}")
        import traceback
        traceback.print_exc()


def get_morning_auction_tushare():
    """
    使用Tushare stk_auction_o接口获取早盘竞价信息（模式3）
    stk_auction_o接口专门获取早盘集合竞价数据（9:15-9:25）
    """
    try:
        # 初始化Tushare Pro API
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        # 设置代理地址
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        # 构建Tushare格式的股票代码 (600487.SH 或 000001.SZ)
        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        print(f"正在获取 {ts_code} 在 {GET_DATE} 的早盘竞价数据...")
        print(f"使用stk_auction_o接口（模式3）获取早盘竞价信息...")

        # 调用Tushare的stk_auction_o接口获取早盘竞价数据
        df = pro.stk_auction_o(
            ts_code=ts_code,
            trade_date=trade_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {GET_DATE} 的早盘竞价数据")
            return

        print(f"\n{'='*80}")
        print(f"早盘竞价数据概览 (stk_auction_o接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        # 显示完整的早盘竞价数据
        print(f"\n{'='*80}")
        print(f"完整早盘竞价数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        # 详细显示早盘竞价信息
        print(f"\n{'='*80}")
        print(f"早盘竞价详细信息:")
        print(f"{'='*80}")

        # 如果有多条记录，按时间排序显示
        if 'time' in df.columns:
            df_sorted = df.sort_values('time')
        else:
            df_sorted = df

        for _, row in df_sorted.iterrows():
            print(f"\n  时间: {row.get('time', 'N/A')}")
            print(f"  开盘价: {row.get('open', 'N/A')}")
            print(f"  收盘价: {row.get('close', 'N/A')}")
            print(f"  最高价: {row.get('high', 'N/A')}")
            print(f"  最低价: {row.get('low', 'N/A')}")
            print(f"  成交量: {row.get('vol', 'N/A')}")
            print(f"  成交额: {row.get('amount', 'N/A')}")

            # 买盘数据
            for i in range(1, 6):
                bid_price = row.get(f'bid{i}_price', 'N/A')
                bid_vol = row.get(f'bid{i}_vol', 'N/A')
                if bid_price != 'N/A' and bid_price != 0:
                    print(f"  买{i}价: {bid_price}  买{i}量: {bid_vol}")

            # 卖盘数据
            for i in range(1, 6):
                ask_price = row.get(f'ask{i}_price', 'N/A')
                ask_vol = row.get(f'ask{i}_vol', 'N/A')
                if ask_price != 'N/A' and ask_price != 0:
                    print(f"  卖{i}价: {ask_price}  卖{i}量: {ask_vol}")

        # 统计信息
        print(f"\n{'='*80}")
        print(f"早盘竞价统计:")
        print(f"{'='*80}")
        if 'open' in df.columns:
            print(f"  开盘价范围: {df['open'].min()} - {df['open'].max()}")
        if 'close' in df.columns:
            print(f"  收盘价范围: {df['close'].min()} - {df['close'].max()}")
        if 'vol' in df.columns:
            print(f"  总成交量: {df['vol'].sum():,.0f}")
        if 'amount' in df.columns:
            print(f"  总成交额: {df['amount'].sum():,.2f}元")

    except Exception as e:
        print(f"获取早盘竞价数据失败: {e}")
        import traceback
        traceback.print_exc()


def get_money_flow_tushare():
    """
    使用Tushare moneyflow_ths接口获取指定股票指定日期范围的资金流向数据（模式4）
    moneyflow_ths接口返回同花顺资金流向数据，包含net_amount净流入金额
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        start_dt = datetime.strptime(GET_DATE, '%Y-%m-%d') - timedelta(days=10)
        start_date = start_dt.strftime('%Y%m%d')
        end_date = GET_DATE.replace('-', '')

        print(f"正在获取 {ts_code} 在 {start_date} 到 {end_date} 的资金流向数据...")
        print(f"使用moneyflow_ths接口（模式4）获取资金流向信息...")

        df = pro.moneyflow_ths(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {start_date} 到 {end_date} 的资金流向数据")
            return

        print(f"\n{'='*80}")
        print(f"资金流向数据概览 (moneyflow_ths接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        print(f"\n{'='*80}")
        print(f"完整资金流向数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        print(f"\n{'='*80}")
        print(f"每日净流入金额 (net_amount):")
        print(f"{'='*80}")
        print(f"{'日期':<12} {'净流入金额(万元)':<20}")
        print("-" * 35)

        total_net_amount = 0
        for _, row in df.iterrows():
            trade_date = row.get('trade_date', 'N/A')
            net_amount = row.get('net_amount', 0)
            total_net_amount += net_amount
            print(f"{trade_date:<12} {net_amount:,.2f}")

        print("-" * 35)
        print(f"{'合计':<12} {total_net_amount:,.2f}")

    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        import traceback
        traceback.print_exc()


def get_money_flow_dc_tushare():
    """
    使用TushareQuery封装接口获取指定股票指定日期范围的资金信息（模式6）
    """
    try:
        if GET_STOCK_CODE.startswith('6'):
            symbol = f"SHSE.{GET_STOCK_CODE}"
        else:
            symbol = f"SZSE.{GET_STOCK_CODE}"

        end_date = GET_DATE
        end_dt = datetime.strptime(GET_DATE, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=10)
        start_date = start_dt.strftime('%Y-%m-%d')

        print(f"正在获取 {symbol} 在 {start_date} 到 {end_date} 的资金信息...")
        print(f"使用TushareQuery.get_money_flow_data接口（模式6）获取资金信息...")

        query = TushareQuery()
        df = query.get_money_flow_data(symbol, start_date, end_date)

        if df is None or df.empty:
            print(f"未获取到 {symbol} 在 {start_date} 到 {end_date} 的资金信息")
            return

        print(f"\n{'='*80}")
        print(f"资金信息数据概览:")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        print(f"\n{'='*80}")
        print(f"完整资金信息数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        print(f"\n{'='*80}")
        print(f"每日净流入金额 (net_amount):")
        print(f"{'='*80}")
        print(f"{'日期':<12} {'净流入金额(万元)':<20}")
        print("-" * 35)

        total_net_amount = 0
        for _, row in df.iterrows():
            trade_date = row.get('trade_date', 'N/A')
            net_amount = row.get('net_amount', 0)
            total_net_amount += net_amount
            print(f"{trade_date:<12} {net_amount:,.2f}")

        print("-" * 35)
        print(f"{'合计':<12} {total_net_amount:,.2f}")

    except Exception as e:
        print(f"获取资金信息失败: {e}")
        import traceback
        traceback.print_exc()


def get_daily_basic_tushare():
    """
    使用Tushare daily_basic接口获取指定股票指定日期的每日指标（基本面信息）（模式7）
    daily_basic接口返回股票的基本面指标，包括：
    - 流通股本、总股本
    - 市值（总市值、流通市值）
    - 估值指标（市盈率、市净率、市销率等）
    - 财务指标（毛利率、净利率、ROE等）
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        print(f"正在获取 {ts_code} 在 {GET_DATE} 的每日基本面指标...")
        print(f"使用daily_basic接口（模式7）获取基本面信息...")

        # 调用Tushare的daily_basic接口获取每日指标
        df = pro.daily_basic(
            ts_code=ts_code,
            trade_date=trade_date,
            fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {GET_DATE} 的每日基本面指标")
            return

        print(f"\n{'='*80}")
        print(f"每日基本面指标数据概览 (daily_basic接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        print(f"\n{'='*80}")
        print(f"完整每日基本面指标数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        # 详细显示基本面信息
        print(f"\n{'='*80}")
        print(f"基本面指标详细信息:")
        print(f"{'='*80}")

        data = df.iloc[0]

        print(f"\n【基本信息】")
        print(f"  股票代码: {data.get('ts_code', 'N/A')}")
        print(f"  交易日期: {data.get('trade_date', 'N/A')}")
        print(f"  收盘价: {data.get('close', 'N/A'):.2f}")

        print(f"\n【股本信息】")
        print(f"  总股本(万股): {data.get('total_share', 'N/A'):,.2f}")
        print(f"  流通股本(万股): {data.get('float_share', 'N/A'):,.2f}")
        print(f"  自由流通股本(万股): {data.get('free_share', 'N/A'):,.2f}")

        print(f"\n【市值信息】")
        print(f"  总市值(亿元): {data.get('total_mv', 'N/A')/10000:,.2f}")
        print(f"  流通市值(亿元): {data.get('circ_mv', 'N/A')/10000:,.2f}")

        print(f"\n【估值指标】")
        print(f"  市盈率(PE): {data.get('pe', 'N/A'):.2f}")
        print(f"  市盈率(TTM): {data.get('pe_ttm', 'N/A'):.2f}")
        print(f"  市净率(PB): {data.get('pb', 'N/A'):.2f}")
        print(f"  市销率(PS): {data.get('ps', 'N/A'):.2f}")
        print(f"  市销率(TTM): {data.get('ps_ttm', 'N/A'):.2f}")

        print(f"\n【市场指标】")
        print(f"  换手率: {data.get('turnover_rate', 'N/A'):.2f}%")
        print(f"  自由流通股换手率: {data.get('turnover_rate_f', 'N/A'):.2f}%")
        print(f"  量比: {data.get('volume_ratio', 'N/A'):.2f}")

        print(f"\n【分红指标】")
        print(f"  股息率: {data.get('dv_ratio', 'N/A'):.2f}%")
        print(f"  股息率(TTM): {data.get('dv_ttm', 'N/A'):.2f}%")

    except Exception as e:
        print(f"获取每日基本面指标失败: {e}")
        import traceback
        traceback.print_exc()


def get_stock_basic_tushare():
    """
    使用Tushare stock_basic接口查询股票基本信息（模式8）
    stock_basic接口返回股票的基本信息，包括：
    - 股票代码、名称、上市日期
    - 交易所、行业、概念等
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        print(f"正在获取 {ts_code} 的股票基本信息...")
        print(f"使用stock_basic接口（模式8）获取股票基本信息...")

        df = pro.stock_basic(
            ts_code=ts_code,
            fields='ts_code,symbol,name,area,industry,list_date,market,exchange,curr_type,list_status,delist_date,is_hs'
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 的股票基本信息")
            return

        print(f"\n{'='*80}")
        print(f"股票基本信息数据概览 (stock_basic接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")
        print(f"\n实际返回的数据预览:")
        print(df.head(1).to_string(index=False))

        print(f"\n{'='*80}")
        print(f"完整股票基本信息数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        print(f"\n{'='*80}")
        print(f"股票基本信息详细信息:")
        print(f"{'='*80}")

        data = df.iloc[0]

        print(f"\n【基本信息】")
        print(f"  股票代码: {data.get('ts_code', 'N/A')}")
        print(f"  股票简称: {data.get('name', 'N/A')}")
        print(f"  交易代码: {data.get('symbol', 'N/A')}")
        print(f"  上市日期: {data.get('list_date', 'N/A')}")

        print(f"\n【市场信息】")
        print(f"  交易所: {data.get('exchange', 'N/A')}")
        print(f"  市场类型: {data.get('market', 'N/A')}")
        print(f"  交易货币: {data.get('curr_type', 'N/A')}")
        list_status = data.get('list_status', 'N/A')
        list_status_desc = {
            'L': '上市',
            'D': '退市',
            'G': '过会未交易',
            'P': '暂停上市'
        }.get(list_status, list_status)
        print(f"  上市状态: {list_status} ({list_status_desc})")

        print(f"\n【行业信息】")
        print(f"  所在地区: {data.get('area', 'N/A')}")
        print(f"  所属行业: {data.get('industry', 'N/A')}")

        print(f"\n【其他信息】")
        print(f"  是否沪深港通标的: {'是' if data.get('is_hs', 'N') == 'H' else '否'}")
        print(f"  退市日期: {data.get('delist_date', 'N/A')}")

    except Exception as e:
        print(f"获取股票基本信息失败: {e}")
        import traceback
        traceback.print_exc()


def get_opening_snapshot_goldminer():
    """
    使用掘金API获取指定股票指定日期9:30后第一张快照的开盘信息（模式5）
    """
    try:
        from gm.api import set_token, history

        # 初始化掘金API
        GOLD_MINER_API_TOKEN = "2e664976b46df6a0903672349c30226ac68e7bf3"
        set_token(GOLD_MINER_API_TOKEN)

        # 构建掘金格式的股票代码
        if GET_STOCK_CODE.startswith('6'):
            symbol = f"SHSE.{GET_STOCK_CODE}"
        else:
            symbol = f"SZSE.{GET_STOCK_CODE}"

        print(f"正在获取 {symbol} 在 {GET_DATE} 9:30后的开盘快照...")
        print(f"使用掘金history接口（模式3）获取开盘信息...")

        # 使用掘金history接口获取9:30的分钟数据
        full_start = f"{GET_DATE} 09:30:00"
        full_end = f"{GET_DATE} 09:35:00"

        df = history(
            symbol=symbol,
            frequency='60s',
            start_time=full_start,
            end_time=full_end,
            fields='symbol,eob,open,close,high,low,volume,amount',
            df=True
        )

        if df is None or df.empty:
            print(f"未获取到 {symbol} 在 {GET_DATE} 的分钟数据")
            return

        # 获取第一条分钟数据（9:30-9:31）
        first_bar = df.iloc[0]

        print(f"\n{'='*80}")
        print(f"9:30后第一张分钟K线快照 (模式3 - 掘金接口):")
        print(f"{'='*80}")
        print(f"  时间: {first_bar.get('eob', 'N/A')}")
        print(f"  开盘价: {first_bar.get('open', 'N/A')}")
        print(f"  收盘价: {first_bar.get('close', 'N/A')}")
        print(f"  最高价: {first_bar.get('high', 'N/A')}")
        print(f"  最低价: {first_bar.get('low', 'N/A')}")
        print(f"  成交量: {first_bar.get('volume', 'N/A')}")
        print(f"  成交额: {first_bar.get('amount', 'N/A')}")

        # 显示前5条分钟数据
        if len(df) > 1:
            print(f"\n{'='*80}")
            print(f"前5条分钟K线数据:")
            print(f"{'='*80}")
            print(f"\n{'序号':<6} {'时间':<20} {'开盘':<10} {'收盘':<10} {'最高':<10} {'最低':<10} {'成交量':<12} {'成交额':<15}")
            print("-" * 100)

            for i, (_, row) in enumerate(df.head(5).iterrows()):
                time_str = str(row.get('eob', 'N/A'))
                open_price = row.get('open', 0)
                close_price = row.get('close', 0)
                high_price = row.get('high', 0)
                low_price = row.get('low', 0)
                volume = row.get('volume', 0)
                amount = row.get('amount', 0)

                print(f"{i+1:<6} {time_str:<20} {open_price:<10.2f} {close_price:<10.2f} {high_price:<10.2f} {low_price:<10.2f} {volume:<12,.0f} {amount:<15.2f}")

        # 统计信息
        print(f"\n{'='*80}")
        print(f"开盘数据统计:")
        print(f"{'='*80}")
        print(f"  开盘价: {first_bar.get('open', 'N/A')}")
        print(f"  当前价: {first_bar.get('close', 'N/A')}")
        print(f"  涨跌幅: {((first_bar.get('close', 0) - first_bar.get('open', 0)) / first_bar.get('open', 1) * 100):.2f}%" if first_bar.get('open', 0) != 0 else "  涨跌幅: N/A")
        print(f"  成交量: {first_bar.get('volume', 'N/A')}")
        print(f"  成交额: {first_bar.get('amount', 'N/A'):.2f}元")

    except Exception as e:
        print(f"获取开盘快照失败: {e}")
        import traceback
        traceback.print_exc()

def get_fina_indicator_tushare():
    """
    使用Tushare fina_indicator接口获取指定股票的财务指标数据（模式9）
    fina_indicator接口返回财务指标数据，包括：
    - 盈利能力指标（毛利率、净利率、ROE、ROA等）
    - 偿债能力指标（资产负债率、流动比率等）
    - 成长能力指标（营收增长率、净利润增长率等）
    - 营运能力指标（存货周转率、应收账款周转率等）
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        end_dt = datetime.strptime(GET_DATE, '%Y-%m-%d')
        start_dt = end_dt - timedelta(days=365)
        start_date = start_dt.strftime('%Y%m%d')
        end_date = end_dt.strftime('%Y%m%d')

        print(f"正在获取 {ts_code} 在 {start_date} 到 {end_date} 的财务指标数据...")
        print(f"使用fina_indicator接口（模式9）获取财务数据...")

        df = pro.fina_indicator(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {start_date} 到 {end_date} 的财务指标数据")
            return

        print(f"\n{'='*80}")
        print(f"财务指标数据概览 (fina_indicator接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        print(f"\n{'='*80}")
        print(f"完整财务指标数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        print(f"\n{'='*80}")
        print(f"主要财务指标详细信息:")
        print(f"{'='*80}")

        for _, row in df.iterrows():
            trade_date = row.get('end_date', 'N/A')
            print(f"\n【报告期: {trade_date}】")

            print(f"\n  【盈利能力】")
            print(f"    毛利率(grossprofit_margin): {row.get('grossprofit_margin', 'N/A')}%")
            print(f"    净利率(netprofit_margin): {row.get('netprofit_margin', 'N/A')}%")
            print(f"    净资产收益率(roe): {row.get('roe', 'N/A')}%")
            print(f"    总资产收益率(roa): {row.get('roa', 'N/A')}%")

            print(f"\n  【偿债能力】")
            print(f"    资产负债率(debt_to_assets): {row.get('debt_to_assets', 'N/A')}%")
            print(f"    流动比率(current_ratio): {row.get('current_ratio', 'N/A')}")
            print(f"    速动比率(quick_ratio): {row.get('quick_ratio', 'N/A')}")

            print(f"\n  【成长能力】")
            print(f"    营业收入增长率(revenue_yoy): {row.get('revenue_yoy', 'N/A')}%")
            print(f"    净利润增长率(netprofit_yoy): {row.get('netprofit_yoy', 'N/A')}%")
            print(f"    总资产增长率(assets_yoy): {row.get('assets_yoy', 'N/A')}%")

            print(f"\n  【营运能力】")
            print(f"    存货周转率(inventory_turn): {row.get('inventory_turn', 'N/A')}")
            print(f"    应收账款周转率(ar_turn): {row.get('ar_turn', 'N/A')}")
            print(f"    总资产周转率(total_assets_turn): {row.get('total_assets_turn', 'N/A')}")

            print(f"\n  【每股指标】")
            print(f"    每股收益(eps): {row.get('eps', 'N/A')}")
            print(f"    每股净资产(bps): {row.get('bps', 'N/A')}")
            print(f"    每股经营现金流(cfps): {row.get('cfps', 'N/A')}")

    except Exception as e:
        print(f"获取财务指标数据失败: {e}")
        import traceback
        traceback.print_exc()


def get_industry_index_pe_pb():
    """
    使用Tushare daily_basic接口获取德明利主营行业的PE、PB均值（模式10）
    德明利的主营行业为半导体，我们获取该行业所有股票的估值指标并计算均值
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        print(f"正在获取半导体行业股票的估值数据（PE、PB均值）...")
        print(f"使用stock_basic + daily_basic接口（模式10）获取行业估值...")
        print(f"查询日期: {GET_DATE}\n")

        # 第一步：获取所有半导体行业的股票
        print("正在获取半导体行业的股票列表...")
        all_stocks_df = pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,industry,list_date'
        )

        if all_stocks_df is None or all_stocks_df.empty:
            print("未获取到股票列表")
            return

        # 筛选出半导体行业的股票
        semiconductor_stocks = all_stocks_df[all_stocks_df['industry'] == '半导体']
        print(f"✓ 找到 {len(semiconductor_stocks)} 只半导体行业股票\n")

        if len(semiconductor_stocks) == 0:
            print("未找到半导体行业的股票")
            return

        # 显示半导体行业的股票列表（前10只）
        print(f"半导体行业股票列表（前10只）:")
        print(semiconductor_stocks[['ts_code', 'name']].head(10).to_string(index=False))
        print(f"... 共 {len(semiconductor_stocks)} 只股票\n")

        # 第二步：获取这些股票在指定日期的 daily_basic 数据
        print(f"正在获取半导体行业股票在 {GET_DATE} 的估值数据...")

        # 分批获取（避免一次请求过多）
        batch_size = 50
        all_daily_data = []

        for i in range(0, len(semiconductor_stocks), batch_size):
            batch_stocks = semiconductor_stocks.iloc[i:i + batch_size]
            ts_codes_str = ','.join(batch_stocks['ts_code'].tolist())

            try:
                daily_df = pro.daily_basic(
                    ts_code=ts_codes_str,
                    trade_date=trade_date,
                    fields='ts_code,trade_date,close,pe,pe_ttm,pb,total_share'
                )
                if daily_df is not None and not daily_df.empty:
                    all_daily_data.append(daily_df)
                    print(f"✓ 成功获取第 {i+1}-{min(i+batch_size, len(semiconductor_stocks))} 只股票的数据，共 {len(daily_df)} 条")
                else:
                    print(f"- 第 {i+1}-{min(i+batch_size, len(semiconductor_stocks))} 只股票在 {trade_date} 无数据")
            except Exception as e:
                print(f"✗ 获取第 {i+1}-{min(i+batch_size, len(semiconductor_stocks))} 只股票数据时出错: {e}")

        if not all_daily_data:
            print(f"\n未获取到任何半导体行业股票在 {trade_date} 的估值数据")
            return

        # 合并所有数据
        merged_data = pd.concat(all_daily_data, ignore_index=True)

        # 与股票名称合并
        merged_data = merged_data.merge(
            semiconductor_stocks[['ts_code', 'name']],
            on='ts_code',
            how='left'
        )

        # 重新排列列顺序
        merged_data = merged_data[['ts_code', 'name', 'trade_date', 'close', 'pe', 'pe_ttm', 'pb', 'total_share']]

        print(f"\n{'='*80}")
        print(f"半导体行业股票估值数据概览:")
        print(f"{'='*80}")
        print(f"数据条数: {len(merged_data)}")
        print(f"数据列: {merged_data.columns.tolist()}")

        print(f"\n{'='*80}")
        print(f"半导体行业股票估值数据（前20条）:")
        print(f"{'='*80}")
        print(merged_data.head(20).to_string(index=False))

        # 计算 PE、PB 均值
        print(f"\n{'='*80}")
        print(f"半导体行业估值均值统计:")
        print(f"{'='*80}")

        pe_list = []
        pe_ttm_list = []
        pb_list = []

        # 用于计算市值加权平均的数据
        total_market_cap = 0
        weighted_pe_sum = 0
        weighted_pe_ttm_sum = 0
        weighted_pb_sum = 0

        for _, row in merged_data.iterrows():
            pe = row.get('pe')
            pe_ttm = row.get('pe_ttm')
            pb = row.get('pb')
            close = row.get('close')
            total_share = row.get('total_share')

            # 简单平均
            if pe is not None and not pd.isna(pe) and pe > 0:  # 排除负值
                pe_list.append(pe)
            if pe_ttm is not None and not pd.isna(pe_ttm) and pe_ttm > 0:
                pe_ttm_list.append(pe_ttm)
            if pb is not None and not pd.isna(pb) and pb > 0:
                pb_list.append(pb)

            # 市值加权平均
            if close is not None and not pd.isna(close) and total_share is not None and not pd.isna(total_share):
                market_cap = close * total_share  # 总市值
                total_market_cap += market_cap

                if pe is not None and not pd.isna(pe) and pe > 0:
                    weighted_pe_sum += pe * market_cap
                if pe_ttm is not None and not pd.isna(pe_ttm) and pe_ttm > 0:
                    weighted_pe_ttm_sum += pe_ttm * market_cap
                if pb is not None and not pd.isna(pb) and pb > 0:
                    weighted_pb_sum += pb * market_cap

        if pe_list:
            pe_mean = sum(pe_list) / len(pe_list)
            print(f"\n【简单平均】")
            print(f"  市盈率(PE)均值: {pe_mean:.2f}")
            print(f"    数据来源: {len(pe_list)} 只股票")
            print(f"    数值范围: {min(pe_list):.2f} ~ {max(pe_list):.2f}")

            if total_market_cap > 0:
                weighted_pe_mean = weighted_pe_sum / total_market_cap
                print(f"\n【市值加权平均】")
                print(f"  市盈率(PE)均值: {weighted_pe_mean:.2f}")

        if pe_ttm_list:
            pe_ttm_mean = sum(pe_ttm_list) / len(pe_ttm_list)
            print(f"\n【简单平均】")
            print(f"  市盈率(TTM)均值: {pe_ttm_mean:.2f}")
            print(f"    数据来源: {len(pe_ttm_list)} 只股票")
            print(f"    数值范围: {min(pe_ttm_list):.2f} ~ {max(pe_ttm_list):.2f}")

            if total_market_cap > 0:
                weighted_pe_ttm_mean = weighted_pe_ttm_sum / total_market_cap
                print(f"\n【市值加权平均】")
                print(f"  市盈率(TTM)均值: {weighted_pe_ttm_mean:.2f}")

        if pb_list:
            pb_mean = sum(pb_list) / len(pb_list)
            print(f"\n【简单平均】")
            print(f"  市净率(PB)均值: {pb_mean:.2f}")
            print(f"    数据来源: {len(pb_list)} 只股票")
            print(f"    数值范围: {min(pb_list):.2f} ~ {max(pb_list):.2f}")

            if total_market_cap > 0:
                weighted_pb_mean = weighted_pb_sum / total_market_cap
                print(f"\n【市值加权平均】")
                print(f"  市净率(PB)均值: {weighted_pb_mean:.2f}")

        if not (pe_list or pe_ttm_list or pb_list):
            print("\n  未能获取到有效的半导体行业 PE、PB 数据")

    except Exception as e:
        print(f"获取半导体行业股票估值数据失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if RUN_MODE == 1:
        print(f"运行模式: 查询竞价数据")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_auction_data()
    elif RUN_MODE == 2:
        print(f"运行模式: 使用Tushare stk_mins接口查询9:30后开盘快照")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_opening_snapshot_tushare()
    elif RUN_MODE == 3:
        print(f"运行模式: 使用Tushare stk_auction_o接口获取早盘竞价信息")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_morning_auction_tushare()
    elif RUN_MODE == 4:
        print(f"运行模式: 使用Tushare moneyflow_ths接口获取资金流向数据")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_money_flow_tushare()
    elif RUN_MODE == 5:
        print(f"运行模式: 使用掘金接口查询9:30后开盘快照")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_opening_snapshot_goldminer()
    elif RUN_MODE == 6:
        print(f"运行模式: 使用Tushare moneyflow_dc接口获取资金信息")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_money_flow_dc_tushare()
    elif RUN_MODE == 7:
        print(f"运行模式: 使用Tushare daily_basic接口获取每日指标（基本面信息）")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_daily_basic_tushare()
    elif RUN_MODE == 8:
        print(f"运行模式: 使用Tushare stock_basic接口查询股票基本信息")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_stock_basic_tushare()
    elif RUN_MODE == 9:
        print(f"运行模式: 使用Tushare fina_indicator接口查询财务数据")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_fina_indicator_tushare()
    elif RUN_MODE == 10:
        print(f"运行模式: 使用Tushare index_dailybasic接口获取德明利主营行业的PE、PB均值")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_industry_index_pe_pb()
    else:
        print(f"错误: 未知的运行模式 {RUN_MODE}")
        print("请使用 RUN_MODE = 1~10")
