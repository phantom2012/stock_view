import tushare as ts
import pandas as pd
from datetime import datetime

GET_STOCK_CODE = "603601"
GET_DATE = "2026-04-30"

# 运行模式配置
# RUN_MODE = 1: 查询当日竞价数据（早盘+尾盘）
# RUN_MODE = 2: 使用Tushare stk_mins接口查询9:30后开盘信息
# RUN_MODE = 3: 使用Tushare stk_auction_o接口获取早盘竞价信息
# RUN_MODE = 4: 使用Tushare moneyflow_ths接口获取资金流向数据
# RUN_MODE = 5: 使用掘金接口查询9:30后开盘快照
# RUN_MODE = 6: 使用Tushare moneyflow_dc接口获取资金信息
# RUN_MODE = 7: 使用Tushare daily_basic接口获取每日指标（基本面信息）
RUN_MODE = 7

# Tushare API Token
TUSHARE_API_TOKEN = "Zku47OUVCydb1095ShpVSzn4u7pea7bFvgLNoCjIENA"
# Tushare 代理地址
TUSHARE_PROXY_URL = "http://47.109.59.144:8989/dataapi"

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

        start_date = "20260422"
        end_date = "20260430"

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
    使用Tushare moneyflow_dc接口获取指定股票指定日期范围的资金信息（模式6）
    moneyflow_dc接口返回北向资金、南向资金等资金流向数据，包含net_amount净流入金额
    """
    try:
        pro = ts.pro_api(TUSHARE_API_TOKEN)
        pro._DataApi__http_url = TUSHARE_PROXY_URL

        if GET_STOCK_CODE.startswith('6'):
            ts_code = f"{GET_STOCK_CODE}.SH"
        else:
            ts_code = f"{GET_STOCK_CODE}.SZ"

        start_date = "20260422"
        end_date = "20260430"

        print(f"正在获取 {ts_code} 在 {start_date} 到 {end_date} 的资金信息...")
        print(f"使用moneyflow_dc接口（模式6）获取资金信息...")

        df = pro.moneyflow_dc(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {start_date} 到 {end_date} 的资金信息")
            return

        print(f"\n{'='*80}")
        print(f"资金信息数据概览 (moneyflow_dc接口):")
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
    else:
        print(f"错误: 未知的运行模式 {RUN_MODE}")
        print("请使用 RUN_MODE = 1~7")
