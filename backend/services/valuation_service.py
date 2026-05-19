"""
股价估值评估服务
通过多因素（PE/PB-ROE/PEG/PS/资产负债健康度）交叉验证评估股价当前所处位置
使用行业估值基准（PE/PB中位数）替代硬编码阈值
"""
import logging
from typing import List, Dict, Any, Optional
from statistics import mean

from shared.db import get_session_ro, StockFinancial, StockDaily, StockInfo, StockIndustry, IndustryValuation

logger = logging.getLogger(__name__)


class ValuationService:
    """
    估值评估服务
    使用5个维度交叉验证，引用行业PE/PB/PS基准：
    1. PE (市盈率) 估值 — 参考行业PE中位数
    2. PB / ROE 估值 — 参考行业PB中位数，结合ROE调整
    3. PEG (市盈率/增长率) 估值
    4. PS (市销率) 估值 — 参考行业PS中位数
    5. 资产负债健康度
    """

    def evaluate_stock(self, code: str) -> Dict[str, Any]:
        try:
            with get_session_ro() as db:
                financial_records = db.query(StockFinancial).filter(
                    StockFinancial.code == code
                ).order_by(StockFinancial.end_date.desc()).all()

                if not financial_records:
                    return self._build_empty_result(code, "无财务数据")

                latest_daily = db.query(StockDaily).filter(
                    StockDaily.code == code
                ).order_by(StockDaily.trade_date.desc()).first()

                if not latest_daily or not latest_daily.close:
                    return self._build_empty_result(code, "无日线数据")

                stock_info = db.query(StockInfo).filter(
                    StockInfo.code == code
                ).first()

            current_price = latest_daily.close
            trade_date = latest_daily.trade_date
            stock_name = stock_info.name if stock_info else ""

            industry_benchmarks = self._get_industry_benchmarks(code)
            industry_name = industry_benchmarks.get('industry', '')

            records_dict = {r.end_date: r for r in financial_records}
            end_dates = sorted(records_dict.keys(), reverse=True)

            ttm_eps = self._calc_ttm_eps(records_dict, end_dates)
            latest = records_dict[end_dates[0]] if end_dates else None

            bps = latest.bps if latest else None
            roe_val = latest.roe if latest else None
            netprofit_yoy = latest.netprofit_yoy if latest else None
            gross_margin = latest.grossprofit_margin if latest else None
            net_margin = latest.netprofit_margin if latest else None
            debt_to_assets = latest.debt_to_assets if latest else None
            current_ratio = latest.current_ratio if latest else None
            quick_ratio = latest.quick_ratio if latest else None

            latest_annual_op_income = None
            annual_op_income_source = None
            for end_date in end_dates:
                if end_date and end_date.endswith('-12-31'):
                    r = records_dict[end_date]
                    if r.op_income:
                        latest_annual_op_income = r.op_income
                        annual_op_income_source = 'annual'
                        break

            if latest_annual_op_income is None and end_dates:
                latest_r = records_dict[end_dates[0]]
                if latest_r and latest_r.op_income:
                    ed = end_dates[0]
                    if ed.endswith('-03-31'):
                        latest_annual_op_income = latest_r.op_income * 4
                    elif ed.endswith('-06-30'):
                        latest_annual_op_income = latest_r.op_income * 2
                    elif ed.endswith('-09-30'):
                        latest_annual_op_income = latest_r.op_income / 3 * 4
                    else:
                        latest_annual_op_income = latest_r.op_income
                    annual_op_income_source = 'estimated'

            total_mv = None
            if stock_info and stock_info.circ_mv:
                total_mv = stock_info.circ_mv * 10000

            factors = {}
            scores = []
            fair_prices = []

            if ttm_eps is not None and current_price > 0:
                if ttm_eps > 0:
                    pe = current_price / ttm_eps
                    pe_result = self._evaluate_pe(pe, ttm_eps, industry_benchmarks)
                else:
                    pe_result = self._evaluate_loss_pe(current_price, ttm_eps, industry_benchmarks)
                factors['pe'] = pe_result
                if pe_result['score'] is not None:
                    scores.append(pe_result['score'])
                if pe_result.get('fair_price'):
                    fair_prices.append(pe_result['fair_price'])

            if bps and bps > 0 and current_price > 0:
                pb = current_price / bps
                pb_result = self._evaluate_pb_roe(pb, bps, roe_val, industry_benchmarks)
                factors['pb_roe'] = pb_result
                if pb_result['score'] is not None:
                    scores.append(pb_result['score'])
                if pb_result.get('fair_price'):
                    fair_prices.append(pb_result['fair_price'])

            growth_rate = self._get_growth_rate(netprofit_yoy, records_dict, end_dates)
            if ttm_eps is not None and current_price > 0:
                pe = current_price / ttm_eps
                if ttm_eps > 0 and growth_rate > 0:
                    if growth_rate > 1.5:
                        peg_result = {
                            'peg': round(pe / growth_rate, 2),
                            'pe': round(pe, 2),
                            'growth_rate': round(growth_rate * 100, 2),
                            'status': '无法评估',
                            'detail': f'原始增长率({growth_rate*100:.0f}%)过高不可持续，PEG法不适用',
                            'score': 45,
                            'fair_price': None,
                        }
                    else:
                        peg_result = self._evaluate_peg(pe / growth_rate, pe, growth_rate, ttm_eps, current_price)
                else:
                    peg_result = self._evaluate_loss_peg(pe, ttm_eps, growth_rate)
                factors['peg'] = peg_result
                if peg_result['score'] is not None:
                    scores.append(peg_result['score'])
                if peg_result.get('fair_price'):
                    fair_prices.append(peg_result['fair_price'])

            if latest_annual_op_income is not None and total_mv and total_mv > 0:
                if latest_annual_op_income > 0:
                    ps = total_mv / latest_annual_op_income
                    ps_result = self._evaluate_ps(ps, industry_benchmarks)
                else:
                    ps_result = self._evaluate_loss_ps(latest_annual_op_income)
                factors['ps'] = ps_result
                if ps_result['score'] is not None:
                    scores.append(ps_result['score'])
                if ps_result.get('fair_price'):
                    fair_prices.append(ps_result['fair_price'])

            health_result = self._evaluate_health(debt_to_assets, current_ratio, quick_ratio)
            factors['health'] = health_result
            if health_result['score'] is not None:
                scores.append(health_result['score'])

            overall = self._calc_overall(scores, factors, current_price, fair_prices)

            result = {
                'code': code,
                'name': stock_name,
                'industry': industry_name,
                'current_price': current_price,
                'trade_date': trade_date,
                'financial': {
                    'ttm_eps': round(ttm_eps, 4) if ttm_eps is not None else None,
                    'bps': round(bps, 2) if bps is not None else None,
                    'roe': round(roe_val, 2) if roe_val is not None else None,
                    'netprofit_yoy': round(netprofit_yoy, 2) if netprofit_yoy is not None else None,
                    'grossprofit_margin': round(gross_margin, 2) if gross_margin is not None else None,
                    'netprofit_margin': round(net_margin, 2) if net_margin is not None else None,
                    'growth_rate': round(growth_rate, 4) if growth_rate is not None else None,
                },
                'industry_benchmark': industry_benchmarks,
                'valuation': overall,
                'valuation_score': self._calc_valuation_score(overall, factors),
                'factors': factors,
            }

            return result

        except Exception as e:
            logger.error(f"评估 {code} 估值失败: {e}")
            import traceback; traceback.print_exc()
            return self._build_empty_result(code, f"评估异常: {str(e)}")

    def evaluate_stocks(self, codes: List[str]) -> List[Dict[str, Any]]:
        results = []
        for code in codes:
            try:
                result = self.evaluate_stock(code)
                results.append(result)
            except Exception as e:
                logger.error(f"评估 {code} 失败: {e}")
                results.append(self._build_empty_result(code, str(e)))
        return results

    def evaluate_all(self) -> List[Dict[str, Any]]:
        try:
            with get_session_ro() as db:
                codes = db.query(StockFinancial.code).distinct().all()
                codes = [row[0] for row in codes if row[0]]
            return self.evaluate_stocks(codes)
        except Exception as e:
            logger.error(f"全量评估失败: {e}")
            return []

    def _get_industry_benchmarks(self, code: str) -> Dict[str, Any]:
        """
        获取股票的行业估值基准
        返回: { industry, pe_median, pb_median, ps_median, stock_count }
        """
        result = {
            'industry': '',
            'pe_median': None,
            'pb_median': None,
            'ps_median': None,
            'stock_count': 0,
        }
        try:
            with get_session_ro() as db:
                stock_industry = db.query(StockIndustry).filter(
                    StockIndustry.code == code
                ).first()

                if not stock_industry or not stock_industry.industry:
                    return result

                result['industry'] = stock_industry.industry

                latest_val = db.query(IndustryValuation).filter(
                    IndustryValuation.industry == stock_industry.industry
                ).order_by(IndustryValuation.trade_date.desc()).first()

                if latest_val:
                    result['pe_median'] = latest_val.pe_median or latest_val.pe_ttm_median
                    result['pb_median'] = latest_val.pb_median
                    result['ps_median'] = latest_val.ps_median
                    result['stock_count'] = latest_val.stock_count or 0
                    result['trade_date'] = latest_val.trade_date

                return result
        except Exception as e:
            logger.warning(f"获取 {code} 行业基准失败: {e}")
            return result

    def _calc_ttm_eps(self, records_dict: Dict[str, Any], end_dates: List[str]) -> Optional[float]:
        eps_values = {}
        for end_date in end_dates:
            r = records_dict[end_date]
            if r.eps is not None:
                eps_values[end_date] = r.eps
        if not eps_values:
            return None
        latest_end = end_dates[0]
        latest_eps = eps_values.get(latest_end)

        if latest_eps is None:
            if eps_values:
                most_recent_eps_date = max(eps_values.keys())
                return eps_values[most_recent_eps_date]
            return None

        is_q1 = latest_end.endswith('-03-31')
        is_q2 = latest_end.endswith('-06-30')
        is_q3 = latest_end.endswith('-09-30')
        is_year = latest_end.endswith('-12-31')
        if is_year:
            return latest_eps
        if is_q1:
            prev_year_end = None
            for ed in end_dates:
                if ed.endswith('-12-31') and ed < latest_end:
                    prev_year_end = ed
                    break
            if prev_year_end and prev_year_end in eps_values:
                prev_q1 = None
                for ed in end_dates:
                    if ed.endswith('-03-31') and ed[:4] == prev_year_end[:4]:
                        prev_q1 = ed
                        break
                if prev_q1 and prev_q1 in eps_values:
                    return eps_values[prev_year_end] - eps_values[prev_q1] + latest_eps
                return eps_values[prev_year_end] + latest_eps
        if is_q2:
            prev_year_end = None
            for ed in end_dates:
                if ed.endswith('-12-31') and ed < latest_end:
                    prev_year_end = ed
                    break
            if prev_year_end and prev_year_end in eps_values:
                prev_q1 = None
                prev_q2 = None
                for ed in end_dates:
                    if ed.endswith('-03-31') and ed[:4] == prev_year_end[:4]:
                        prev_q1 = ed
                    if ed.endswith('-06-30') and ed[:4] == prev_year_end[:4]:
                        prev_q2 = ed
                prev_h1_eps = 0
                if prev_q1 and prev_q1 in eps_values:
                    prev_h1_eps += eps_values[prev_q1]
                if prev_q2 and prev_q2 in eps_values:
                    prev_h1_eps += eps_values[prev_q2]
                if prev_q1 and prev_q1 in eps_values and prev_q2 and prev_q2 in eps_values:
                    return eps_values[prev_year_end] - prev_h1_eps + latest_eps
        if is_q3:
            prev_year_end = None
            for ed in end_dates:
                if ed.endswith('-12-31') and ed < latest_end:
                    prev_year_end = ed
                    break
            if prev_year_end and prev_year_end in eps_values:
                prev_9m_eps = 0
                for ed in end_dates:
                    if not ed.endswith('-12-31') and ed[:4] == prev_year_end[:4] and ed < prev_year_end:
                        prev_9m_eps += eps_values.get(ed, 0)
                if prev_9m_eps > 0:
                    return eps_values[prev_year_end] - prev_9m_eps + latest_eps
        return latest_eps

    def _get_growth_rate(self, netprofit_yoy: Optional[float], records_dict: Dict[str, Any], end_dates: List[str]) -> float:
        growth_value = None
        if netprofit_yoy is not None:
            growth_value = netprofit_yoy / 100.0
        if growth_value is None:
            for end_date in end_dates:
                r = records_dict[end_date]
                if r.netprofit_yoy is not None:
                    growth_value = r.netprofit_yoy / 100.0
                    break
        if growth_value is None:
            for end_date in end_dates:
                r = records_dict[end_date]
                if r.tr_yoy is not None:
                    growth_value = r.tr_yoy / 100.0
                    break
        if growth_value is None:
            return 0.0
        if growth_value < -0.9:
            return -0.9
        return growth_value

    def _evaluate_loss_pe(self, price: float, ttm_eps: float, benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """亏损状态下的PE评估"""
        ind_pe = benchmarks.get('pe_median')
        return {
            'pe': None,
            'industry_pe_median': round(ind_pe, 2) if ind_pe else None,
            'status': '亏损',
            'detail': f'净利润亏损(TTM EPS={ttm_eps:.2f})，PE不适用',
            'score': 5,
            'fair_price': None,
        }

    def _evaluate_loss_peg(self, pe: float, ttm_eps: float, growth_rate: float) -> Dict[str, Any]:
        """亏损状态下的PEG评估"""
        growth_pct = growth_rate * 100 if growth_rate > 0 else 0
        if ttm_eps <= 0:
            reason = f'净利润亏损(TTM EPS={ttm_eps:.2f})，PEG无法评估'
        else:
            reason = f'净利润同比下滑({growth_pct:.1f}%)，PEG无法评估'
        return {
            'peg': None, 'pe': round(pe, 2), 'growth_rate': round(growth_pct, 2),
            'status': '亏损', 'detail': reason,
            'score': 5, 'fair_price': None,
        }

    def _evaluate_loss_ps(self, op_income: float) -> Dict[str, Any]:
        """亏损状态下的PS评估"""
        return {
            'ps': None,
            'industry_ps_median': None,
            'status': '亏损', 'detail': f'营业收入为负({op_income:.0f})，PS无意义',
            'score': 5, 'fair_price': None,
        }

    def _evaluate_pe(self, pe: float, eps: float, benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """PE 维度评估 — 参考行业PE中位数"""
        if pe <= 0:
            return {'pe': round(pe, 2), 'status': '无法评估', 'detail': 'PE为负值', 'score': None, 'fair_price': None}

        ind_pe = benchmarks.get('pe_median')
        if ind_pe and ind_pe > 0:
            ratio = pe / ind_pe
            if ratio < 0.5:
                score = 95; status = '严重低估'; detail = f'PE({pe:.1f}) 远低于行业中值({ind_pe:.1f})'
                fair_pe = ind_pe * 0.8
            elif ratio < 0.7:
                score = 80; status = '低估'; detail = f'PE({pe:.1f}) 低于行业中值({ind_pe:.1f})'
                fair_pe = ind_pe * 0.9
            elif ratio < 1.0:
                score = 65; status = '合理偏低'; detail = f'PE({pe:.1f}) 低于行业中值({ind_pe:.1f})，在合理范围内'
                fair_pe = ind_pe
            elif ratio < 1.3:
                score = 50; status = '合理'; detail = f'PE({pe:.1f}) 接近行业中值({ind_pe:.1f})'
                fair_pe = pe
            elif ratio < 1.8:
                score = 35; status = '偏高'; detail = f'PE({pe:.1f}) 高于行业中值({ind_pe:.1f})'
                fair_pe = ind_pe * 1.2
            elif ratio < 2.5:
                score = 20; status = '高估'; detail = f'PE({pe:.1f}) 明显高于行业中值({ind_pe:.1f})'
                fair_pe = ind_pe
            else:
                score = 10; status = '严重高估'; detail = f'PE({pe:.1f}) 远超行业中值({ind_pe:.1f})'
                fair_pe = ind_pe * 0.8
        else:
            if pe < 8:
                score = 85; status = '低估'; detail = f'PE({pe:.1f}) 较低'; fair_pe = 15
            elif pe < 15:
                score = 70; status = '合理偏低'; detail = f'PE({pe:.1f}) 处于较低区间'; fair_pe = 18
            elif pe < 25:
                score = 55; status = '合理'; detail = f'PE({pe:.1f}) 处于合理区间'; fair_pe = pe
            elif pe < 35:
                score = 40; status = '偏高'; detail = f'PE({pe:.1f}) 偏高'; fair_pe = 20
            elif pe < 50:
                score = 20; status = '高估'; detail = f'PE({pe:.1f}) 明显偏高'; fair_pe = 18
            else:
                score = 10; status = '严重高估'; detail = f'PE({pe:.1f}) 远超合理区间'; fair_pe = 15

        return {
            'pe': round(pe, 2),
            'industry_pe_median': round(ind_pe, 2) if ind_pe else None,
            'status': status,
            'detail': detail,
            'score': score,
            'fair_price': round(fair_pe * eps, 2),
        }

    def _evaluate_pb_roe(self, pb: float, bps: float, roe: Optional[float], benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """PB/ROE 维度评估 — 参考行业PB中位数并考虑ROE差异"""
        ind_pb = benchmarks.get('pb_median')
        roe_str = f'{roe:.1f}%' if roe is not None else 'N/A'

        if ind_pb and ind_pb > 0:
            ind_pb_val = ind_pb
            if roe is not None and roe > 0:
                roe_pct = roe / 100.0
                if roe_pct > 0.20:
                    adj_pb_range = (ind_pb_val * 2, ind_pb_val * 5)
                elif roe_pct > 0.15:
                    adj_pb_range = (ind_pb_val * 1.5, ind_pb_val * 3.5)
                elif roe_pct > 0.10:
                    adj_pb_range = (ind_pb_val * 1.0, ind_pb_val * 2.5)
                elif roe_pct > 0.05:
                    adj_pb_range = (ind_pb_val * 0.7, ind_pb_val * 1.5)
                else:
                    adj_pb_range = (ind_pb_val * 0.3, ind_pb_val * 0.8)
            else:
                adj_pb_range = (ind_pb_val * 0.5, ind_pb_val * 2.0)
        else:
            if roe is not None and roe > 0:
                roe_pct = roe / 100.0
                if roe_pct > 0.20:
                    adj_pb_range = (3, 6)
                elif roe_pct > 0.15:
                    adj_pb_range = (2, 5)
                elif roe_pct > 0.10:
                    adj_pb_range = (1.5, 3.5)
                elif roe_pct > 0.05:
                    adj_pb_range = (1, 2.5)
                else:
                    adj_pb_range = (0.5, 1.5)
            else:
                adj_pb_range = (1, 3)

        low, high = adj_pb_range

        if pb < low * 0.7:
            score = 90; status = '低估'; fair_pb = low
            detail = f'PB({pb:.2f}) 低于合理范围({low:.1f}-{high:.1f})'
        elif pb < low:
            score = 75; status = '偏低'; fair_pb = low
            detail = f'PB({pb:.2f}) 略低于合理范围({low:.1f}-{high:.1f})'
        elif pb <= high:
            score = 60; status = '合理'; fair_pb = pb
            detail = f'PB({pb:.2f}) 处于合理范围({low:.1f}-{high:.1f})'
        elif pb <= high * 1.3:
            score = 40; status = '偏高'; fair_pb = high
            detail = f'PB({pb:.2f}) 略高于合理范围({low:.1f}-{high:.1f})'
        else:
            score = 20; status = '高估'; fair_pb = high
            detail = f'PB({pb:.2f}) 明显高于合理范围({low:.1f}-{high:.1f})'

        return {
            'pb': round(pb, 2),
            'roe': round(roe, 2) if roe else None,
            'industry_pb_median': round(ind_pb, 2) if ind_pb else None,
            'reasonable_pb_range': [round(low, 2), round(high, 2)],
            'status': status,
            'detail': detail + f' (ROE={roe_str})',
            'score': score,
            'fair_price': round(fair_pb * bps, 2),
        }

    def _evaluate_peg(self, peg: float, pe: float, growth_rate: float,
                      eps: float, price: float) -> Dict[str, Any]:
        growth_pct = growth_rate * 100
        if growth_rate <= 0 or growth_rate < 0.03:
            return {'peg': None, 'pe': round(pe, 2), 'growth_rate': round(growth_pct, 2),
                    'status': '亏损/微利', 'detail': f'净利润增长率({growth_pct:.1f}%)过低，PEG无法评估',
                    'score': 30, 'fair_price': None}
        if peg < 0.5:
            score = 95; status = '严重低估'; fair_peg = 1.2
        elif peg < 1:
            score = 80; status = '低估'; fair_peg = 1.0
        elif peg < 1.5:
            score = 60; status = '合理'; fair_peg = peg
        elif peg < 2:
            score = 40; status = '偏高'; fair_peg = 1.0
        elif peg < 3:
            score = 20; status = '高估'; fair_peg = 0.8
        else:
            score = 10; status = '严重高估'; fair_peg = 0.5
        fair_pe_peg = fair_peg * growth_rate * 100
        fair_price_peg = round(fair_pe_peg * eps, 2) if 0 < fair_pe_peg < 100 else None
        return {
            'peg': round(peg, 2), 'pe': round(pe, 2), 'growth_rate': round(growth_pct, 2),
            'status': status, 'detail': f'PEG({peg:.2f}) {status}',
            'score': score, 'fair_price': fair_price_peg,
        }

    def _evaluate_ps(self, ps: float, benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """PS 维度评估 — 参考行业PS中位数"""
        ind_ps = benchmarks.get('ps_median')

        if ind_ps and ind_ps > 0:
            ratio = ps / ind_ps
            if ratio < 0.3:
                score = 90; status = '低估'; detail = f'PS({ps:.2f}) 远低于行业中值({ind_ps:.2f})'
            elif ratio < 0.5:
                score = 75; status = '偏低'; detail = f'PS({ps:.2f}) 低于行业中值({ind_ps:.2f})'
            elif ratio < 1.0:
                score = 55; status = '合理'; detail = f'PS({ps:.2f}) 低于行业中值({ind_ps:.2f})，合理区间'
            elif ratio < 1.5:
                score = 40; status = '偏高'; detail = f'PS({ps:.2f}) 高于行业中值({ind_ps:.2f})'
            elif ratio < 2.5:
                score = 20; status = '高估'; detail = f'PS({ps:.2f}) 明显高于行业中值({ind_ps:.2f})'
            else:
                score = 10; status = '严重高估'; detail = f'PS({ps:.2f}) 远超行业中值({ind_ps:.2f})'
        else:
            if ps < 1:
                score = 85; status = '低估'; detail = f'PS({ps:.2f}) 极低'
            elif ps < 3:
                score = 65; status = '偏低'; detail = f'PS({ps:.2f}) 较低'
            elif ps < 5:
                score = 45; status = '合理'; detail = f'PS({ps:.2f}) 处于合理区间'
            elif ps < 8:
                score = 30; status = '偏高'; detail = f'PS({ps:.2f}) 偏高'
            else:
                score = 15; status = '高估'; detail = f'PS({ps:.2f}) 过高'

        return {
            'ps': round(ps, 2),
            'industry_ps_median': round(ind_ps, 2) if ind_ps else None,
            'status': status,
            'detail': detail,
            'score': score,
            'fair_price': None,
        }

    def _evaluate_health(self, debt_to_assets: Optional[float],
                         current_ratio: Optional[float],
                         quick_ratio: Optional[float]) -> Dict[str, Any]:
        sub_scores = []
        details = []

        if debt_to_assets is not None:
            if debt_to_assets <= 30:
                d_score = 35; d_detail = f'资产负债率({debt_to_assets:.1f}%)很低'
            elif debt_to_assets <= 50:
                d_score = 30; d_detail = f'资产负债率({debt_to_assets:.1f}%)适中'
            elif debt_to_assets <= 65:
                d_score = 20; d_detail = f'资产负债率({debt_to_assets:.1f}%)略高'
            elif debt_to_assets <= 80:
                d_score = 12; d_detail = f'资产负债率({debt_to_assets:.1f}%)偏高'
            else:
                d_score = 5; d_detail = f'资产负债率({debt_to_assets:.1f}%)过高'
            sub_scores.append(d_score); details.append(d_detail)

        if current_ratio is not None:
            if current_ratio >= 2.5:
                c_score = 30; c_detail = f'流动比率({current_ratio:.2f})充足'
            elif current_ratio >= 1.5:
                c_score = 25; c_detail = f'流动比率({current_ratio:.2f})健康'
            elif current_ratio >= 1:
                c_score = 15; c_detail = f'流动比率({current_ratio:.2f})尚可'
            else:
                c_score = 5; c_detail = f'流动比率({current_ratio:.2f})偏低'
            sub_scores.append(c_score); details.append(c_detail)

        if quick_ratio is not None:
            if quick_ratio >= 1.5:
                q_score = 25; q_detail = f'速动比率({quick_ratio:.2f})充足'
            elif quick_ratio >= 1:
                q_score = 20; q_detail = f'速动比率({quick_ratio:.2f})健康'
            elif quick_ratio >= 0.5:
                q_score = 12; q_detail = f'速动比率({quick_ratio:.2f})尚可'
            else:
                q_score = 5; q_detail = f'速动比率({quick_ratio:.2f})偏低'
            sub_scores.append(q_score); details.append(q_detail)

        total_possible = 0
        if debt_to_assets is not None: total_possible += 35
        if current_ratio is not None: total_possible += 30
        if quick_ratio is not None: total_possible += 25
        if total_possible == 0:
            return {'status': '未知', 'detail': '无资产负债数据', 'score': None}

        normalized = round(sum(sub_scores) / total_possible * 100)
        if normalized >= 80: status = '健康'
        elif normalized >= 60: status = '一般'
        elif normalized >= 40: status = '偏低'
        else: status = '较差'

        return {
            'debt_to_assets': round(debt_to_assets, 2) if debt_to_assets else None,
            'current_ratio': round(current_ratio, 2) if current_ratio else None,
            'quick_ratio': round(quick_ratio, 2) if quick_ratio else None,
            'status': status, 'detail': '; '.join(details), 'score': normalized,
        }

    def _calc_overall(self, scores: List[float], factors: Dict[str, Any],
                      current_price: float, fair_prices: List[float]) -> Dict[str, Any]:
        if not scores:
            return {'overall_status': '无法评估', 'confidence': '低', 'avg_score': None,
                    'upside_potential': None, 'downside_risk': None, 'fair_price': None, 'factor_count': 0}

        avg_score = mean(scores)
        factor_count = len([f for f in factors.values() if f.get('score') is not None])

        if avg_score >= 80: overall_status = '低估'
        elif avg_score >= 65: overall_status = '合理偏低'
        elif avg_score >= 45: overall_status = '合理'
        elif avg_score >= 30: overall_status = '偏高'
        else: overall_status = '高估'

        if factor_count >= 4: confidence = '高'
        elif factor_count >= 3: confidence = '中'
        else: confidence = '低'

        valid_fair_prices = [p for p in fair_prices if p and p > 0]
        upside = None; downside = None; fair_price_val = None

        if valid_fair_prices and current_price > 0:
            fair_price_val = round(mean(valid_fair_prices), 2)
            upside = round((fair_price_val - current_price) / current_price * 100, 2)
            if len(valid_fair_prices) >= 2:
                worst_fair = min(valid_fair_prices)
                downside = round((worst_fair - current_price) / current_price * 100, 2)
            else:
                downside = round((fair_price_val * 0.85 - current_price) / current_price * 100, 2)

        return {
            'overall_status': overall_status, 'confidence': confidence,
            'avg_score': round(avg_score, 1), 'factor_count': factor_count,
            'fair_price': fair_price_val, 'upside_potential': upside, 'downside_risk': downside,
        }

    def _build_empty_result(self, code: str, reason: str) -> Dict[str, Any]:
        return {
            'code': code, 'name': '', 'current_price': None, 'trade_date': None,
            'financial': None, 'valuation': {
                'overall_status': '无法评估', 'confidence': '低', 'avg_score': None,
                'factor_count': 0, 'fair_price': None, 'upside_potential': None, 'downside_risk': None,
            },
            'valuation_score': None,
            'factors': {}, 'error': reason,
        }

    def _calc_valuation_score(self, overall: Dict[str, Any], factors: Dict[str, Any]) -> Optional[int]:
        """
        计算综合估值分（-100 ~ +100）
        基于5个维度的评分，按不同权重加权

        权重分配（PE/PEG占大头，反映市场定价和成长性）:
          - PE: 25% (估值核心)
          - PB/ROE: 20% (资产质量)
          - PEG: 25% (成长性)
          - PS: 15% (营收估值)
          - 资产负债: 15% (安全边际)

        核心逻辑：
          每个因子得分0~100映射到正负分贡献，得分越高（越低估）越正，
          得分越低（越高估）越负。
          最终结果限制在[-100, +100]区间。
        """
        weights = {
            'pe': 0.25,
            'pb_roe': 0.20,
            'peg': 0.25,
            'ps': 0.15,
            'health': 0.15,
        }

        weighted_sum = 0.0
        total_weight = 0.0

        for key, weight in weights.items():
            factor = factors.get(key)
            if factor and factor.get('score') is not None:
                factor_score = factor['score']
                centered = (factor_score - 50) * 2
                weighted_sum += centered * weight
                total_weight += weight

        if total_weight == 0:
            return None

        final_score = round(weighted_sum / total_weight)
        return max(-100, min(100, final_score))

    def batch_get_valuation_scores(self, codes: List[str]) -> Dict[str, Optional[int]]:
        """
        批量获取多个股票的估值分
        返回: { code: score }
        """
        results = {}
        for code in codes:
            try:
                result = self.evaluate_stock(code)
                results[code] = result.get('valuation_score')
            except Exception as e:
                logger.warning(f"获取 {code} 估值分失败: {e}")
                results[code] = None
        return results


_valuation_service = None


def get_valuation_service() -> ValuationService:
    global _valuation_service
    if _valuation_service is None:
        _valuation_service = ValuationService()
    return _valuation_service
