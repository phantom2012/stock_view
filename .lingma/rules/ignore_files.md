---
trigger: always_on
---
# 通义灵码 - 项目忽略规则（ignore_files.md）
# 以下文件/目录请**完全忽略**，不进行任何分析、修改、关联、读取或生成建议
---

## 一、敏感配置与环境文件
- `.env`
- `.env.*`
- `.env.local`
- `.env.production`
- `.env.development`
- `*.key`
- `*.pem`

## 二、日志与临时文件
- `*.log`
- `*.log.*`
- `server.log`
- `*.tmp`
- `*.temp`
- `*.swp`
- `*.swo`

## 三、Python 编译与缓存文件
- `__pycache__/`
- `*.pyc`
- `*.pyo`
- `*.pyd`
- `*.pyc.*`
- `.pytest_cache/`
- `.coverage`

## 四、数据文件与数据库
- `*.db`
- `*.sqlite`
- `*.sqlite3`
- `*.csv`
- `*.xlsx`
- `*.xls`
- `*.json.tmp`
- `*.parquet`

## 五、前端与项目构建产物
- `node_modules/`
- `dist/`
- `build/`
- `.nuxt/`
- `.cache/`
- `.vitepress/dist/`

## 六、IDE 与系统文件
- `.vscode/`
- `.idea/`
- `.DS_Store`
- `Thumbs.db`

## 七、测试与非核心文档（可选）
> 如果你不想灵码处理这些，也可以加上
- `mytest/`
- `docs/`
- `README.md`（除非你需要它帮你写文档）

---

## 【重要说明】
1.  请只关注以下核心业务代码文件：
    - `backend/` 目录下的所有 `.py` 文件
    - `dashboard/src/` 目录下的 `.vue` / `.js` / `.ts` 文件
    - 项目根目录下的核心业务脚本（如 `get_stock_info.py`）
2.  对于上述忽略列表中的文件，**禁止读取、修改、分析、生成建议**，也不要在代码关联中提及它们。
3.  遇到 `.lingma/` 目录下的规则文件（如 `yufa_guifan.md`、`interface_standard.md`），请仅在编码时遵循其中的规范，不要修改这些规则文件本身。