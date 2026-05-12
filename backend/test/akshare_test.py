import akshare as ak

# ============== 配置变量 ==============
# 运行模式配置
# RUN_MODE = 1: 查询早盘竞价历史数据
# RUN_MODE = 2: 查询股票资金流向数据
# RUN_MODE = 3: 查询股票基本信息（流通股本、流通市值）
RUN_MODE = 2

# 股票代码配置
STOCK_CODE = "000823"  # 测试股票代码   001309  000823  002008
TARGET_DATE = "2026-05-07"  # 目标日期（YYYY-MM-DD格式）
FUND_FLOW_DAYS = 10  # 资金流向数据保留最近N天
# ============== 配置结束 ==============

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
            start_time=start_time,
            end_time=end_time,
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

def test_stock_fund_flow_individual():
    target_date_ymd = TARGET_DATE
    market = "sz" if STOCK_CODE.startswith("002") else "sh"

    print(f"查询股票 {STOCK_CODE} 最近 {FUND_FLOW_DAYS} 天的资金流向数据...")
    print(f"当前akshare版本: {ak.__version__}")
    print()

    try:
        # 获取近100个交易日的资金流向数据（接口限制）
        df = ak.stock_individual_fund_flow(
            stock=STOCK_CODE,
            market=market
        )

        # 只保留最近N天的数据
        df_recent = df.tail(FUND_FLOW_DAYS)

        print("成功获取资金流向数据！")
        print("-" * 80)
        print(f"原始数据: {len(df)} 条，保留最近: {len(df_recent)} 条")
        print("-" * 80)

        # 筛选指定日期的数据
        df_target = df_recent[df_recent['日期'] == target_date_ymd]

        if df_target.empty:
            print(f"未找到 {target_date_ymd} 的数据，可能该日期非交易日")
            print(f"以下是最近 {FUND_FLOW_DAYS} 条数据：")
            print(df_recent)
        else:
            print(f"{target_date_ymd} 的资金流向数据：")
            print(df_target)

        print("-" * 80)
        return df_recent
    except Exception as e:
        print(f"查询失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_stock_individual_info():
    print(f"查询股票 {STOCK_CODE} 的基本信息...")
    print(f"当前akshare版本: {ak.__version__}")
    print("=" * 80)
    print()

    results = {}
    current_price = None

    # === 尝试1: 东方财富基本信息 ===
    print("[尝试1] 东方财富接口 (stock_individual_info_em)...")
    try:
        df_em = ak.stock_individual_info_em(symbol=STOCK_CODE)
        print("✅ 成功")
        results['em'] = df_em
    except Exception as e:
        print(f"❌ 失败: {e}")

    # === 尝试2: 雪球基本信息 ===
    print("\n[尝试2] 雪球接口 (stock_individual_basic_info_xq)...")
    try:
        symbol_xq = f"SZ{STOCK_CODE}" if STOCK_CODE.startswith('00') or STOCK_CODE.startswith('30') else f"SH{STOCK_CODE}"
        df_xq = ak.stock_individual_basic_info_xq(symbol=symbol_xq)
        print("✅ 成功")
        results['xq'] = df_xq
    except Exception as e:
        print(f"❌ 失败: {e}")

    # === 尝试3: 个股档案 ===
    print("\n[尝试3] 个股档案接口 (stock_individual_spot_em)...")
    try:
        df_spot = ak.stock_individual_spot_em(symbol=STOCK_CODE)
        print("✅ 成功")
        results['spot'] = df_spot
    except Exception as e:
        print(f"❌ 失败: {e}")

    # === 尝试4: 个股档案(另一个接口) ===
    print("\n[尝试4] 个股档案接口 (stock_a_share_profile)...")
    try:
        df_profile = ak.stock_a_share_profile(symbol=STOCK_CODE)
        print("✅ 成功")
        results['profile'] = df_profile
    except Exception as e:
        print(f"❌ 失败: {e}")

    # === 尝试5: 获取最新股价 ===
    print("\n[尝试5] 获取最新股价...")
    try:
        df_quote = ak.stock_zh_a_spot_em()
        for _, row in df_quote.iterrows():
            code_str = str(row['代码'])
            if code_str == STOCK_CODE or code_str.endswith(STOCK_CODE):
                current_price = row['最新价']
                print(f"✅ 成功, 当前股价: {current_price}")
                break
        if current_price is None:
            print("⚠️ 未找到对应的股价")
    except Exception as e:
        print(f"❌ 失败: {e}")

    # === 打印所有结果 ===
    print("\n" + "=" * 80)
    print("【所有获取到的数据】")
    print("=" * 80)

    for key, df in results.items():
        print(f"\n--- 接口: {key} ---")
        print(df)

    # === 尝试提取信息 ===
    print("\n" + "=" * 80)
    print("【信息汇总】")
    print("=" * 80)

    total_share = None
    float_share = None
    free_share = None
    float_mv = None
    total_mv = None

    if 'em' in results:
        df_em = results['em']
        for _, row in df_em.iterrows():
            if row['item'] == '总股本':
                total_share = row['value']
            elif row['item'] == '流通股':
                float_share = row['value']
            elif row['item'] == '总市值':
                total_mv = row['value']
            elif row['item'] == '流通市值':
                float_mv = row['value']

    if 'xq' in results:
        df_xq = results['xq']
        for _, row in df_xq.iterrows():
            item = str(row['item'])
            if '自由流通' in item or '自由流通股' in item or '自由流通股本' in item:
                free_share = row['value']
                print(f"✅ 找到自由流通股本: {item} = {free_share}")

    print(f"\n股票代码: {STOCK_CODE}")
    print(f"总股本: {total_share}")
    print(f"流通股: {float_share}")
    print(f"自由流通股本: {free_share if free_share else '未找到'}")
    print(f"总市值: {total_mv}")
    print(f"流通市值: {float_mv}")
    print(f"当前股价: {current_price}")

    # === 验证 ===
    if float_share and current_price:
        try:
            float_share_num = float(float_share)
            price_num = float(current_price)
            calc_mv = float_share_num * price_num
            print(f"\n【验证】")
            print(f"流通股 × 股价 = {calc_mv:,.2f}")
            if float_mv:
                print(f"接口返回流通市值 = {float_mv}")
                diff = abs(calc_mv - float(float_mv))
                if diff < calc_mv * 0.01:
                    print(f"✅ 数据基本一致")
                else:
                    print(f"⚠️ 差异较大 (差 {diff:,.0f})")
        except Exception as e:
            print(f"验证失败: {e}")

    return results

if __name__ == "__main__":
    if RUN_MODE == 1:
        test_stock_auction_history()
    elif RUN_MODE == 2:
        test_stock_fund_flow_individual()
    elif RUN_MODE == 3:
        test_stock_individual_info()
    else:
        print(f"未知的 RUN_MODE: {RUN_MODE}")
