/**
 * 股票计算工具函数
 * 提供涨幅、乖离率等通用计算方法
 */

/**
 * 计算涨幅百分比
 * @param {number} current - 当前价格
 * @param {number} base - 基准价格
 * @returns {number|null} 涨幅百分比（保留2位小数），无法计算时返回 null
 */
export const calcGain = (current, base) => {
  if (!current || !base || base <= 0) return null
  return parseFloat(((current - base) / base * 100).toFixed(2))
}

/**
 * 计算今日涨幅（基于收盘价和昨收盘价）
 * @param {Object} row - 数据行，需包含 close_price, pre_close_price
 * @returns {number|null}
 */
export const calcTodayGain = (row) => {
  return calcGain(row?.close_price, row?.pre_close_price)
}

/**
 * 计算次日涨幅（基于次日收盘价和今日收盘价）
 * @param {Object} row - 数据行，需包含 next_close_price, close_price
 * @returns {number|null}
 */
export const calcNextDayRise = (row) => {
  return calcGain(row?.next_close_price, row?.close_price)
}

/**
 * 计算开盘涨幅（基于开盘价和昨收盘价）
 * @param {Object} row - 数据行，需包含 open_price, pre_close_price
 * @returns {number|null}
 */
export const calcOpenGain = (row) => {
  return calcGain(row?.open_price, row?.pre_close_price)
}

/**
 * 计算昨乖离率（基于昨收盘价和昨均价）
 * @param {Object} row - 数据行，需包含 pre_close_price, pre_avg_price
 * @returns {number|null}
 */
export const calcYesterdayBias = (row) => {
  return calcGain(row?.pre_close_price, row?.pre_avg_price)
}

/**
 * 计算均价（成交额/成交量）
 * @param {number} amount - 成交额
 * @param {number} volume - 成交量
 * @returns {number} 均价，无法计算时返回 0
 */
export const calcAvgPrice = (amount, volume) => {
  if (!volume || volume <= 0) return 0
  return amount / volume
}

/**
 * 计算偏离开盘价的乖离率（收盘价相对均价的偏离）
 * @param {number} close - 收盘价
 * @param {number} avgPrice - 均价
 * @returns {number} 乖离率百分比，无法计算时返回 0
 */
export const calcBias = (close, avgPrice) => {
  if (!avgPrice || avgPrice <= 0) return 0
  return parseFloat(((close - avgPrice) / avgPrice * 100).toFixed(2))
}

/**
 * 计算涨跌幅（基于收盘价和昨收盘价）
 * @param {number} close - 收盘价
 * @param {number} preClose - 昨收盘价
 * @returns {number} 涨跌幅百分比，无法计算时返回 0
 */
export const calcChangePct = (close, preClose) => {
  if (!preClose || preClose <= 0) return 0
  return parseFloat(((close - preClose) / preClose * 100).toFixed(2))
}
