import akshare as ak

def test_stock_auction_history():
    stock_code = "002980"
    target_date = "20260429"

    print(f"查询股票 {stock_code} 在 {target_date} 的早盘竞价数据...")
    print(f"当前akshare版本: {ak.__version__}")
    print()

    try:
        start_time = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]} 09:00:00"
        end_time = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]} 09:35:00"

        df = ak.stock_zh_a_hist_min_em(
            symbol=stock_code,
            start_date=start_time,
            end_date=end_time,
            period='1',
            adjust=''
        )

        if df.empty:
            print(f"警告: 未获取到 {target_date} 的数据，可能该日期非交易日")
            print("尝试获取今日(最后交易日)的盘前数据...")
            df = ak.stock_zh_a_hist_pre_min_em(symbol=stock_code, start_time='09:00:00', end_time='09:30:00')

        print("成功获取股票数据！")
        print("-" * 80)
        print(df)
        print("-" * 80)
        return df
    except Exception as e:
        print(f"查询失败: {e}")
        return None

if __name__ == "__main__":
    test_stock_auction_history()
