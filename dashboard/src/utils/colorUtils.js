// 颜色工具函数

/**
 * 根据数值的正负返回对应的颜色类名
 * @param {number} value - 要判断的数值
 * @returns {string} 颜色类名
 */
export const getValueColor = (value) => {
  if (value === null || value === undefined) return ''
  const num = parseFloat(value)
  if (num > 0) return 'text-red-600'
  if (num < 0) return 'text-green-600'
  return ''
}

/**
 * 根据竞价成交额的变化率返回对应的颜色类名
 * @param {number} currentAmount - 当前成交额
 * @param {number} prevAmount - 前一天成交额
 * @returns {string} 颜色类名
 */
export const getAuctionAmountColor = (currentAmount, prevAmount) => {
  if (!currentAmount && currentAmount !== 0) {
    return ''
  }
  if (prevAmount === undefined || prevAmount === null) {
    return ''
  }
  const current = parseFloat(currentAmount)
  const prev = parseFloat(prevAmount)
  if (isNaN(current) || isNaN(prev)) {
    return ''
  }
  if (prev === 0) {
    return ''  // 除数为0时不做比较
  }

  const ratio = (current - prev) / prev * 100

  if (ratio >= 100) {
    return 'text-red-600'     // 增长>=100%，红色
  }
  if (ratio >= 50) {
    return 'text-red-500'      // 增长>=50%，红色(稍淡)
  }
  if (ratio >= 30) {
    return 'text-orange-500'   // 增长>=30%，橙色
  }
  if (ratio >= 10) {
    return 'text-blue-500'     // 增长>=10%，蓝色
  }
  if (ratio >= 0) {
    return ''                    // 增长>=0%且<10%，黑色不变
  }
  if (ratio >= -10) {
    return 'text-green-500'   // 下降<10%，绿色
  }
  return 'text-purple-600'                    // 下降>=10%，紫色
}

/**
 * 计算竞价成交额的变化率
 * @param {number} currentAmount - 当前成交额
 * @param {number} prevAmount - 前一天成交额
 * @returns {string|null} 变化率百分比
 */
export const getAuctionAmountChange = (currentAmount, prevAmount) => {
  if (!currentAmount && currentAmount !== 0) return null
  if (prevAmount === undefined || prevAmount === null) return null
  const current = parseFloat(currentAmount)
  const prev = parseFloat(prevAmount)
  if (isNaN(current) || isNaN(prev)) return null
  if (prev === 0) return null
  return ((current - prev) / prev * 100).toFixed(2)
}
