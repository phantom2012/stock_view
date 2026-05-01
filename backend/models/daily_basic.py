"""
DailyBasic 模型类
用于存储 Tushare daily_basic 接口返回的每日基本面指标数据
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class DailyBasic:
    """
    Tushare daily_basic 接口返回的数据模型
    
    字段说明：
    - close: 当日收盘价
    - turnover_rate: 换手率（%）
    - turnover_rate_f: 换手率（自由流通股）
    - volume_ratio: 量比
    - pe: 市盈率（总市值/净利润，亏损的PE为空）
    - pe_ttm: 市盈率（TTM，亏损的PE为空）
    - pb: 市净率（总市值/净资产）
    - ps: 市销率
    - ps_ttm: 市销率（TTM）
    - dv_ratio: 股息率（%），除息日发生在去年期间的派现
    - dv_ttm: 股息率（TTM）（%），除息日在近12个月且分红报告期在12个月以内的派现
    - total_share: 总股本（万股）
    - float_share: 流通股本（万股）
    - free_share: 自由流通股本（万）
    - total_mv: 总市值（万元）
    - circ_mv: 流通市值（万元）
    """
    
    ts_code: str                # 股票代码（Tushare格式：600487.SH）
    trade_date: str             # 交易日期（YYYYMMDD）
    close: Optional[float] = None           # 当日收盘价
    turnover_rate: Optional[float] = None   # 换手率（%）
    turnover_rate_f: Optional[float] = None # 换手率（自由流通股）
    volume_ratio: Optional[float] = None    # 量比
    pe: Optional[float] = None              # 市盈率
    pe_ttm: Optional[float] = None          # 市盈率（TTM）
    pb: Optional[float] = None              # 市净率
    ps: Optional[float] = None              # 市销率
    ps_ttm: Optional[float] = None          # 市销率（TTM）
    dv_ratio: Optional[float] = None        # 股息率（%）
    dv_ttm: Optional[float] = None          # 股息率（TTM）（%）
    total_share: Optional[float] = None     # 总股本（万股）
    float_share: Optional[float] = None     # 流通股本（万股）
    free_share: Optional[float] = None      # 自由流通股本（万）
    total_mv: Optional[float] = None        # 总市值（万元）
    circ_mv: Optional[float] = None         # 流通市值（万元）
    
    @property
    def symbol(self) -> str:
        """获取掘金格式的股票代码"""
        if self.ts_code.endswith('.SH'):
            return f"SHSE.{self.ts_code.replace('.SH', '')}"
        elif self.ts_code.endswith('.SZ'):
            return f"SZSE.{self.ts_code.replace('.SZ', '')}"
        return self.ts_code
    
    @property
    def trade_date_formatted(self) -> str:
        """获取格式化的交易日期（YYYY-MM-DD）"""
        if len(self.trade_date) == 8:
            return f"{self.trade_date[:4]}-{self.trade_date[4:6]}-{self.trade_date[6:8]}"
        return self.trade_date
    
    @property
    def total_mv_billion(self) -> Optional[float]:
        """获取总市值（亿元）"""
        if self.total_mv is not None:
            return self.total_mv / 10000
        return None
    
    @property
    def circ_mv_billion(self) -> Optional[float]:
        """获取流通市值（亿元）"""
        if self.circ_mv is not None:
            return self.circ_mv / 10000
        return None
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'ts_code': self.ts_code,
            'symbol': self.symbol,
            'trade_date': self.trade_date,
            'trade_date_formatted': self.trade_date_formatted,
            'close': self.close,
            'turnover_rate': self.turnover_rate,
            'turnover_rate_f': self.turnover_rate_f,
            'volume_ratio': self.volume_ratio,
            'pe': self.pe,
            'pe_ttm': self.pe_ttm,
            'pb': self.pb,
            'ps': self.ps,
            'ps_ttm': self.ps_ttm,
            'dv_ratio': self.dv_ratio,
            'dv_ttm': self.dv_ttm,
            'total_share': self.total_share,
            'float_share': self.float_share,
            'free_share': self.free_share,
            'total_mv': self.total_mv,
            'total_mv_billion': self.total_mv_billion,
            'circ_mv': self.circ_mv,
            'circ_mv_billion': self.circ_mv_billion,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DailyBasic':
        """从字典创建 DailyBasic 对象"""
        return cls(
            ts_code=data.get('ts_code', ''),
            trade_date=data.get('trade_date', ''),
            close=data.get('close'),
            turnover_rate=data.get('turnover_rate'),
            turnover_rate_f=data.get('turnover_rate_f'),
            volume_ratio=data.get('volume_ratio'),
            pe=data.get('pe'),
            pe_ttm=data.get('pe_ttm'),
            pb=data.get('pb'),
            ps=data.get('ps'),
            ps_ttm=data.get('ps_ttm'),
            dv_ratio=data.get('dv_ratio'),
            dv_ttm=data.get('dv_ttm'),
            total_share=data.get('total_share'),
            float_share=data.get('float_share'),
            free_share=data.get('free_share'),
            total_mv=data.get('total_mv'),
            circ_mv=data.get('circ_mv'),
        )
