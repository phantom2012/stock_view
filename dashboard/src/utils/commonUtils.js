// 通用工具函数

/**
 * 格式化金额，自动转换为亿或万单位
 * @param {number} value - 金额数值
 * @param {string} unit - 输入单位，'yuan'(元) 或 'wan'(万元)，默认 'yuan'
 * @returns {string|null} 格式化后的字符串，如 "1.23亿" 或 "5678.90万"
 */
export const formatAmount = (value, unit = 'yuan') => {
  if (value === null || value === undefined) return null
  const num = parseFloat(value)
  if (isNaN(num)) return null

  const valueInYuan = unit === 'wan' ? num * 10000 : num
  const absValue = Math.abs(valueInYuan)

  let result
  if (absValue >= 100000000) {
    result = (absValue / 100000000).toFixed(2) + '亿'
  } else if (absValue >= 10000000) {
    result = (absValue / 10000000).toFixed(2) + '千万'
  } else {
    result = (absValue / 10000).toFixed(2) + '万'
  }

  return valueInYuan < 0 ? '-' + result : result
}
