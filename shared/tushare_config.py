"""
Tushare API 配置
集中管理 Tushare 的 enable 开关、token 和 proxy_url
各模块统一从此处导入，避免配置分散
"""
import os

TUSHARE_CONFIG = {
    'enable': int(os.getenv('TUSHARE_ENABLE', '0')),
    'token': os.getenv('TUSHARE_TOKEN', '17bf2b4e7bffa84e9b02a52f026df310c03badcb29c63533e935353c'),
    'proxy_url': os.getenv('TUSHARE_PROXY_URL', 'http://a.sszhixia.cn/'),
}
