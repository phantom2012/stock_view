import sys
sys.path.append('..')

import tushare as ts

# Tushare API Token
TUSHARE_API_TOKEN = "aeb08b4b67a00b77b8c8041b8e183e9c07c350fbe31691ede2913291"

print("="*60)
print("测试Tushare获取股票基本信息")
print("="*60)

# 初始化Tushare
print("\n初始化Tushare Pro API...")
pro = ts.pro_api(TUSHARE_API_TOKEN)

# 设置代理（与后端配置一致）
TUSHARE_PROXY_URL = "http://tsy.xiaodefa.cn"
pro._DataApi__http_url = TUSHARE_PROXY_URL
print(f"✓ 使用代理: {TUSHARE_PROXY_URL}")

# 测试2：直接查询002834
print("\n" + "="*60)
print("测试2：直接查询002834的基本信息...")
try:
    df = pro.stock_basic(ts_code='002980.SZ', fields='ts_code,symbol,name,area,industry,market,list_date')
    
    if df is not None and not df.empty:
        print(f"✓ 成功获取002834的信息:")
        print(df)
    else:
        print("✗ 未获取到002834的信息")
except Exception as e:
    print(f"✗ 测试2失败: {e}")
    import traceback
    traceback.print_exc()
