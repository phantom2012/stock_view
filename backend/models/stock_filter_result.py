from dataclasses import dataclass
from typing import Optional
from .auction_data import AuctionData


@dataclass
class StockFilterResult:
    """
    股票筛选结果模型
    用于存储股票筛选后的完整结果数据
    """
    symbol: str
    code: str
    stock_name: str
    
    pre_avg_price: float = 0.0
    pre_close_price: float = 0.0
    pre_price_gain: float = 0.0
    
    open_price: float = 0.0
    close_price: float = 0.0
    next_close_price: float = 0.0
    
    auction_start_price: float = 0.0
    auction_end_price: float = 0.0
    price_diff: float = 0.0
    volume_ratio: float = 0.0
    
    interval_max_rise: float = 0.0
    max_day_rise: float = 0.0
    
    today_gain: float = 0.0
    next_day_gain: float = 0.0
    
    trade_date: str = ''
    
    higher_score: float = 0.0
    rising_wave_score: int = 0
    
    weipan_exceed: int = 0
    zaopan_exceed: int = 0
    rising_wave: int = 0
    
    @property
    def auction_data(self) -> AuctionData:
        """获取竞价数据对象"""
        return AuctionData(
            auction_start_price=self.auction_start_price,
            auction_end_price=self.auction_end_price,
            volume_ratio=self.volume_ratio
        )
    
    def to_dict(self) -> dict:
        """转换为字典格式（用于数据库存储或API返回）"""
        return {
            'symbol': self.symbol,
            'code': self.code,
            'stock_name': self.stock_name,
            'pre_avg_price': self.pre_avg_price,
            'pre_close_price': self.pre_close_price,
            'pre_price_gain': self.pre_price_gain,
            'open_price': self.open_price,
            'close_price': self.close_price,
            'next_close_price': self.next_close_price,
            'auction_start_price': self.auction_start_price,
            'auction_end_price': self.auction_end_price,
            'price_diff': self.price_diff,
            'volume_ratio': self.volume_ratio,
            'interval_max_rise': self.interval_max_rise,
            'max_day_rise': self.max_day_rise,
            'today_gain': self.today_gain,
            'next_day_gain': self.next_day_gain,
            'trade_date': self.trade_date,
            'higher_score': self.higher_score,
            'rising_wave_score': self.rising_wave_score,
            'weipan_exceed': self.weipan_exceed,
            'zaopan_exceed': self.zaopan_exceed,
            'rising_wave': self.rising_wave
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'StockFilterResult':
        """从字典创建实例"""
        return cls(
            symbol=data.get('symbol', ''),
            code=data.get('code', ''),
            stock_name=data.get('stock_name', ''),
            pre_avg_price=data.get('pre_avg_price', 0.0),
            pre_close_price=data.get('pre_close_price', 0.0),
            pre_price_gain=data.get('pre_price_gain', 0.0),
            open_price=data.get('open_price', 0.0),
            close_price=data.get('close_price', 0.0),
            next_close_price=data.get('next_close_price', 0.0),
            auction_start_price=data.get('auction_start_price', 0.0),
            auction_end_price=data.get('auction_end_price', 0.0),
            price_diff=data.get('price_diff', 0.0),
            volume_ratio=data.get('volume_ratio', 0.0),
            interval_max_rise=data.get('interval_max_rise', 0.0),
            max_day_rise=data.get('max_day_rise', 0.0),
            today_gain=data.get('today_gain', 0.0),
            next_day_gain=data.get('next_day_gain', 0.0),
            trade_date=data.get('trade_date', ''),
            higher_score=data.get('higher_score', 0.0),
            rising_wave_score=data.get('rising_wave_score', 0),
            weipan_exceed=data.get('weipan_exceed', 0),
            zaopan_exceed=data.get('zaopan_exceed', 0),
            rising_wave=data.get('rising_wave', 0)
        )
    
    @classmethod
    def create(
        cls,
        symbol: str,
        code: str,
        stock_name: str,
        auction_data: Optional[dict] = None,
        volume_ratio: float = 0.0,
        interval_max_rise: float = 0.0,
        max_day_rise: float = 0.0,
        today_gain: float = 0.0,
        next_day_gain: float = 0.0,
        trade_date: str = '',
        higher_score: float = 0.0,
        rising_wave_score: int = 0,
        weipan_exceed: int = 0,
        zaopan_exceed: int = 0,
        rising_wave: int = 0,
        pre_avg_price: float = 0.0,
        pre_close_price: float = 0.0,
        pre_price_gain: float = 0.0,
        open_price: float = 0.0,
        close_price: float = 0.0,
        next_close_price: float = 0.0
    ) -> 'StockFilterResult':
        """
        创建股票筛选结果实例
        
        Args:
            symbol: 股票代码（掘金格式，如 'SH.600000'）
            code: 股票代码（纯数字，如 '600000'）
            stock_name: 股票名称
            auction_data: 竞价数据字典
            volume_ratio: 开盘量比
            interval_max_rise: 区间最大涨幅
            max_day_rise: 单日最大涨幅
            today_gain: 当日涨幅
            next_day_gain: 次日涨幅
            trade_date: 交易日期
            higher_score: 超预期得分
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
            StockFilterResult实例
        """
        auction_start_price = 0.0
        auction_end_price = 0.0
        price_diff = 0.0
        
        if auction_data:
            auction_start_price = auction_data.get('auction_start_price', 0.0)
            auction_end_price = auction_data.get('auction_end_price', 0.0)
            price_diff = round(auction_end_price - auction_start_price, 2)
        
        return cls(
            symbol=symbol,
            code=code,
            stock_name=stock_name,
            pre_avg_price=pre_avg_price,
            pre_close_price=pre_close_price,
            pre_price_gain=pre_price_gain,
            open_price=open_price,
            close_price=close_price,
            next_close_price=next_close_price,
            auction_start_price=auction_start_price,
            auction_end_price=auction_end_price,
            price_diff=price_diff,
            volume_ratio=volume_ratio,
            interval_max_rise=interval_max_rise,
            max_day_rise=max_day_rise,
            today_gain=today_gain,
            next_day_gain=next_day_gain,
            trade_date=trade_date,
            higher_score=higher_score,
            rising_wave_score=rising_wave_score,
            weipan_exceed=weipan_exceed,
            zaopan_exceed=zaopan_exceed,
            rising_wave=rising_wave
        )
