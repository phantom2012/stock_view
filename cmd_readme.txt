# 关闭所有python启动服务的命令
taskkill /F /IM python.exe

# 计算转强得分
curl.exe -X POST "http://localhost:8000/api/data/recalculate/turn-strong" -H "Content-Type: application/json" -d "{}"

# 计算升浪得分
curl.exe -X POST "http://localhost:8000/api/data/recalculate/rising-wave" -H "Content-Type: application/json" -d "{}"
