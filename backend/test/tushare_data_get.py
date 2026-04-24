import tushare as ts
import pandas as pd
from datetime import datetime

GET_STOCK_CODE = "002361"
GET_DATE = "2026-04-23"

# 运行模式配置
# RUN_MODE = 1: 查询竞价数据（早盘+尾盘）
# RUN_MODE = 2: 使用Tushare stk_mins接口查询9:30后开盘信息
RUN_MODE = 1

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


def get_opening_snapshot_goldminer():
    """
    使用掘金API获取指定股票指定日期9:30后第一张快照的开盘信息（模式3）
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
        print(f"运行模式: 使用掘金接口查询9:30后开盘快照")
        print(f"股票代码: {GET_STOCK_CODE}, 日期: {GET_DATE}\n")
        get_opening_snapshot_goldminer()
    else:
        print(f"错误: 未知的运行模式 {RUN_MODE}")
        print("请使用 RUN_MODE = 1 (竞价数据) 或 RUN_MODE = 2 (Tushare接口) 或 RUN_MODE = 3 (掘金接口)")