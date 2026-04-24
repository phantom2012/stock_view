import sys
sys.path.append('..')

import tushare as ts

# Tushare API Token
TUSHARE_API_TOKEN = "aeb08b4b67a00b77b8c8041b8e183e9c07c350fbe31691ede2913291"
TUSHARE_PROXY_URL = "http://tsy.xiaodefa.cn"

print("="*60)
print("查询002834的历史状态")
print("="*60)

# 初始化Tushare
pro = ts.pro_api(TUSHARE_API_TOKEN)
pro._DataApi__http_url = TUSHARE_PROXY_URL

# 查询所有状态的股票（包括退市的）
print("\n查询所有状态的股票（包括已退市）...")
try:
    df = pro.stock_basic(exchange='', list_status='L,D,P', fields='ts_code,symbol,name,area,industry,market,list_date,delist_date')
    
    if df is not None and not df.empty:
        print(f"✓ 成功获取 {len(df)} 条股票数据（包含已退市）")
        
        # 查找002834
        print("\n查找002834:")
        matched = df[df['ts_code'].str.contains('002834', na=False)]
        
        if not matched.empty:
            print(f"\n✓ 找到 {len(matched)} 条匹配记录:")
            for idx, row in matched.iterrows():
                print(f"  ts_code: {row.get('ts_code')}")
                print(f"  name: {row.get('name')}")
                print(f"  list_status: 需要查看原始数据")
                print(f"  list_date: {row.get('list_date')}")
                print(f"  delist_date: {row.get('delist_date', 'N/A')}")
                print(f"  industry: {row.get('industry')}")
        else:
            print("\n✗ Tushare数据库中完全没有002834的记录")
            print("\n可能的原因:")
            print("  1. 002834已经退市且不在Tushare的股票列表中")
            print("  2. 股票代码有误")
            print("  3. 数据源本身缺少这只股票")
            
    else:
        print("✗ 获取失败或返回空数据")
        
except Exception as e:
    print(f"✗ 查询失败: {e}")
    import traceback
    traceback.print_exc()

# 再试试查询002830-002840之间的所有股票
print("\n" + "="*60)
print("查询002830-002840之间的所有股票:")
try:
    all_stocks = pro.stock_basic(exchange='', list_status='L,D,P', fields='ts_code,name')
    
    if all_stocks is not None and not all_stocks.empty:
        # 过滤002830-002840
        matched_range = all_stocks[all_stocks['ts_code'].str.contains('^0028(3[0-9]|40)', na=False, regex=True)]
        
        if not matched_range.empty:
            print(f"找到 {len(matched_range)} 只股票:")
            for idx, row in matched_range.sort_values('ts_code').iterrows():
                print(f"  {row.get('ts_code')}: {row.get('name')}")
        else:
            print("未找到")
            
except Exception as e:
    print(f"✗ 查询失败: {e}")
