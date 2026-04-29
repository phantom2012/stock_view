import tushare as ts
import pandas as pd
from datetime import datetime

GET_STOCK_CODE = "600726"
GET_DATE = "2026-04-28"

# 运行模式配置
# RUN_MODE = 1: 查询当日竞价数据（早盘+尾盘）
# RUN_MODE = 2: 使用Tushare stk_mins接口查询9:30后开盘信息
# RUN_MODE = 3: 使用Tushare stk_auction_o接口获取早盘竞价信息
# RUN_MODE = 4: 使用Tushare moneyflow接口获取资金流向数据
# RUN_MODE = 5: 使用掘金接口查询9:30后开盘快照
RUN_MODE = 4

# Tushare API Token
TUSHARE_API_TOKEN = "aeb08b4b67a00b77b8c8041b8e183e9c07c350fbe31691ede2913291"
# Tushare 代理地址
TUSHARE_PROXY_URL = "http://tsy.xiaodefa.cn"

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
    使用Tushare moneyflow_ths接口获取指定股票指定日期的资金流向数据（模式4）
    moneyflow_ths接口返回主力净流入、大单/中单/小单流向等数据
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

        # 转换日期格式为YYYYMMDD
        trade_date = GET_DATE.replace('-', '')

        print(f"正在获取 {ts_code} 在 {GET_DATE} 的资金流向数据...")
        print(f"使用moneyflow_ths接口（模式4）获取资金流向信息...")

        # 调用Tushare的moneyflow_ths接口获取资金流向数据
        # moneyflow_ths返回的字段: trade_date, ts_code, name, pct_change, latest,
        # net_amount, net_d5_amount, buy_lg_amount, buy_lg_amount_rate,
        # buy_md_amount, buy_md_amount_rate, buy_sm_amount, buy_sm_amount_rate
        df = pro.moneyflow_ths(
            ts_code=ts_code,
            start_date=trade_date,
            end_date=trade_date
        )

        if df is None or df.empty:
            print(f"未获取到 {ts_code} 在 {GET_DATE} 的资金流向数据")
            return

        print(f"\n{'='*80}")
        print(f"资金流向数据概览 (moneyflow_ths接口):")
        print(f"{'='*80}")
        print(f"数据条数: {len(df)}")
        print(f"数据列: {df.columns.tolist()}")

        # 显示完整的资金流向数据
        print(f"\n{'='*80}")
        print(f"完整资金流向数据:")
        print(f"{'='*80}")
        print(df.to_string(index=False))

        # 详细显示资金流向信息
        print(f"\n{'='*80}")
        print(f"资金流向详细信息:")
        print(f"{'='*80}")

        for _, row in df.iterrows():
            print(f"\n  股票代码: {row.get('ts_code', 'N/A')}")
            print(f"  股票名称: {row.get('name', 'N/A')}")
            print(f"  交易日期: {row.get('trade_date', 'N/A')}")
            print(f"  涨跌幅: {row.get('pct_change', 'N/A')}%")
            print(f"  最新价: {row.get('latest', 'N/A')}")

            # 主力净流入数据
            print(f"\n  【主力净流入】")
            print(f"    今日净流入额: {row.get('net_amount', 'N/A'):,} 万元")
            print(f"    5日净流入额: {row.get('net_d5_amount', 'N/A'):,} 万元")

            # 大单数据
            print(f"\n  【大单】")
            print(f"    净流入额: {row.get('buy_lg_amount', 'N/A'):,} 万元")
            print(f"    净流入占比: {row.get('buy_lg_amount_rate', 'N/A')}%")

            # 中单数据
            print(f"\n  【中单】")
            print(f"    净流入额: {row.get('buy_md_amount', 'N/A'):,} 万元")
            print(f"    净流入占比: {row.get('buy_md_amount_rate', 'N/A')}%")

            # 小单数据
            print(f"\n  【小单】")
            print(f"    净流入额: {row.get('buy_sm_amount', 'N/A'):,} 万元")
            print(f"    净流入占比: {row.get('buy_sm_amount_rate', 'N/A')}%")

        # 统计汇总
        print(f"\n{'='*80}")
        print(f"资金流向统计汇总:")
        print(f"{'='*80}")

        print(f"  股票代码: {ts_code}")
        print(f"  统计周期: {GET_DATE}")
        print(f"  今日主力净流入总额: {df['net_amount'].sum():,.2f} 万元")
        print(f"  5日主力净流入总额: {df['net_d5_amount'].sum():,.2f} 万元")
        print(f"  大单净流入总额: {df['buy_lg_amount'].sum():,.2f} 万元")
        print(f"  中单净流入总额: {df['buy_md_amount'].sum():,.2f} 万元")
        print(f"  小单净流入总额: {df['buy_sm_amount'].sum():,.2f} 万元")

    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
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
        print(f"运行模式: 使用Tushare moneyflow接口获取资金流向数据")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_money_flow_tushare()
    elif RUN_MODE == 5:
        print(f"运行模式: 使用掘金接口查询9:30后开盘快照")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_opening_snapshot_goldminer()
    else:
        print(f"错误: 未知的运行模式 {RUN_MODE}")
        print("请使用 RUN_MODE = 1 (竞价数据) 或 RUN_MODE = 2 (Tushare stk_mins) 或 RUN_MODE = 3 (Tushare stk_auction_o) 或 RUN_MODE = 4 (Tushare moneyflow) 或 RUN_MODE = 5 (掘金接口)")
