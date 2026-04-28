from pydantic import BaseModel, field_validator
from typing import Optional


class StockResult(BaseModel):
    """
    股票结果模型
    用于存储股票筛选后的完整结果数据，支持自动解析字典数据并进行类型转换
    """
    symbol: str = ""
    code: str = ""
    stock_name: str = ""

    pre_avg_price: float = 0.0
    pre_close_price: float = 0.0
    pre_price_gain: float = 0.0

    open_price: float = 0.0
    tail_57_price: float = 0.0
    close_price: float = 0.0
    next_close_price: float = 0.0

    price_diff: float = 0.0
    open_volume: Optional[float] = 0.0
    open_volume_ratio: float = 0.0

    interval_max_rise: float = 0.0
    max_day_rise: float = 0.0

    trade_date: str = ""

    exp_score: float = 0.0
    rising_wave_score: float = 0.0

    weipan_exceed: int = 0
    zaopan_exceed: int = 0
    rising_wave: int = 0

    @field_validator('open_volume', mode='before')
    @classmethod
    def validate_open_volume(cls, v):
        return v if v is not None else 0.0

    def to_dict(self) -> dict:
        """转换为字典格式（用于数据库存储或API返回）"""
        return self.model_dump()

    @classmethod
    def create(
        cls,
        symbol: str,
        code: str,
        stock_name: str,
        auction_data: Optional[dict] = None,
        open_volume_ratio: float = 0.0,
        interval_max_rise: float = 0.0,
        max_day_rise: float = 0.0,
        today_gain: float = 0.0,
        next_day_gain: float = 0.0,
        trade_date: str = '',
        exp_score: float = 0.0,
        rising_wave_score: float = 0.0,
        weipan_exceed: int = 0,
        zaopan_exceed: int = 0,
        rising_wave: int = 0,
        pre_avg_price: float = 0.0,
        pre_close_price: float = 0.0,
        pre_price_gain: float = 0.0,
        open_price: float = 0.0,
        close_price: float = 0.0,
        next_close_price: float = 0.0
    ) -> 'StockResult':
        """
        创建股票筛选结果实例

        Args:
            symbol: 股票代码（掘金格式，如 'SH.600000'）
            code: 股票代码（纯数字，如 '600000'）
            stock_name: 股票名称
            auction_data: 竞价数据字典
            open_volume_ratio: 开盘量比
            interval_max_rise: 区间最大涨幅
            max_day_rise: 单日最大涨幅
            today_gain: 当日涨幅
            next_day_gain: 次日涨幅
            trade_date: 交易日期
            exp_score: 预期得分（取rising_wave_score的值）
            rising_wave_score: 升浪形态得分
            weipan_exceed: 尾盘超预期
            zaopan_exceed: 早盘超预期
            rising_wave: 上升形态
            pre_avg_price: 昨均价
            pre_close_price: 昨收盘价
            pre_price_gain: 昨涨幅
            open_price: 今开盘价
            close_price: 今收盘价
            next_close_price: 次日收盘价

        Returns:
            StockResult实例
        """
        data = {k: v for k, v in locals().items() if k != 'cls' and k != 'auction_data'}
        if auction_data:
            data['open_volume'] = auction_data.get('open_volume', 0.0)
            data['open_price'] = auction_data.get('open_price', open_price)
        return cls.model_validate(data)
