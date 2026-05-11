import akshare as ak
import pandas as pd
import time

symbol = "002384"
# 先尝试获取最近交易日的数据
try:
    # 先获取最新行情来确定一个有效日期
    df_quote = ak.stock_zh_a_spot_em()
    print("获取行情成功")
    
    # 尝试获取最近一个月的数据
    df_day = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                start_date="20260401", end_date="20260509", adjust="")
    
    if len(df_day) == 0:
        print("未找到历史数据")
        exit(1)
    
    # 取最近一天的数据
    last_row = df_day.iloc[-1]
    close = last_row["收盘"]
    actual_date = last_row["日期"]
    print(f"使用日期: {actual_date}, 收盘价: {close}")
    print()
except Exception as e:
    print(f"获取行情失败: {e}")
    exit(1)

# 2. 流通股本（万股）
try:
    df_info = ak.stock_individual_info_em(symbol=symbol)
    print("股票基本信息：")
    print(df_info)
    print()
    
    # 实际字段是"流通股"
    circ_share = float(df_info[df_info["item"] == "流通股"]["value"].iloc[0])
except Exception as e:
    print(f"获取基本信息失败: {e}")
    exit(1)

# 3. 十大流通股东（取最新，近似当日）
try:
    df_cir_holder = ak.stock_zh_a_holder_cir_em(symbol=symbol)
    print("十大流通股东：")
    print(df_cir_holder.head(10))
    print()
    
    df_cir_holder["持股比例"] = pd.to_numeric(df_cir_holder["持股比例"], errors="coerce")
    
    # 只算持股≥5%的流通股东
    block_holders = df_cir_holder[df_cir_holder["持股比例"] >= 5.0]
    block_pct_sum = block_holders["持股比例"].sum()
    
    # 估算自由流通股本（万股）
    free_share_est = circ_share * (1 - block_pct_sum / 100)
    
    # 自由流通市值（万元）
    free_mv_est = free_share_est * close
    
    print("=" * 80)
    print("【结果】")
    print("=" * 80)
    print(f"收盘价：{close}")
    print(f"流通股本（股）：{circ_share:,.0f}")
    print(f"十大流通股东持股≥5%合计比例：{block_pct_sum:.2f}%")
    print(f"估算自由流通股本（股）：{free_share_est:,.0f}")
    print(f"估算自由流通市值（元）：{free_mv_est:,.0f}")
    
except Exception as e:
    print(f"获取十大流通股东失败: {e}")
    import traceback
    traceback.print_exc()