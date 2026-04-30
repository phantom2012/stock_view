<template>
  <div>
    <div style="display: flex; align-items: center; gap: 16px; margin-top: 10px; margin-bottom: 16px; width: 100%; flex-wrap: nowrap; justify-content: flex-end;">
      <div style="display: flex; align-items: center; gap: 8px; margin-right: auto;">
        <el-input
          v-model="stockCode"
          placeholder="请输入6位股票代码"
          maxlength="6"
          :validate-event="false"
          @input="validateStockCode"
          style="width: 160px;"
        >
          <template #append>
            <el-button
              type="success"
              @click="queryStock"
              :disabled="!isValidStockCode"
            >
              查询个股
            </el-button>
          </template>
        </el-input>
        <el-message
          v-if="stockCodeError"
          type="error"
          :message="stockCodeError"
          class="mt-0"
        />
      </div>

      <el-button
        type="primary"
        icon="Back"
        @click="handleBack"
        style="background-color: #2563eb; border-color: #2563eb;"
      >
        返回列表
      </el-button>
    </div>

    <div v-if="stockInfo" class="space-y-4">
      <!-- 信息卡片行 -->
      <div style="display: flex; flex-wrap: wrap; gap: 16px; width: 100%;">
        <!-- 基本信息卡片 -->
        <el-card style="flex: 1; min-width: 300px; background: white; border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <template #header>
            <div style="display: flex; align-items: center; justify-content: space-between;">
              <span style="font-size: 18px; font-weight: 600; color: #1f2937;">{{ stockInfo.stock_name }}</span>
              <span style="font-size: 14px; color: #4b5563;">{{ stockInfo.code }}</span>
            </div>
          </template>

          <div style="display: flex; flex-wrap: wrap; gap: 16px; position: relative; min-height: 100px; align-items: flex-start;">
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">开盘价：</span>
              <span style="font-size: 18px; font-weight: 600; color: #1f2937;">{{ stockInfo.open_price }}</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">开盘成交量：</span>
              <span style="font-size: 18px; font-weight: 600; color: #1f2937;">{{ stockInfo.open_volume }}</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">价格差异：</span>
              <span style="font-size: 18px; font-weight: 600;" :class="getDiffColor(stockInfo.price_diff)">{{ stockInfo.price_diff }}</span>
            </div>
            <div style="position: absolute; bottom: 10px; right: 10px; display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">超预期分：</span>
              <span style="font-size: 18px; font-weight: 600; color: #3b82f6;">{{ stockInfo.exp_score }}</span>
            </div>
          </div>
        </el-card>

        <!-- 涨幅信息卡片 -->
        <el-card style="flex: 1; min-width: 300px; background: white; border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <template #header>
            <span style="font-size: 18px; font-weight: 600; color: #1f2937;">多日涨幅信息</span>
          </template>

          <div style="display: flex; flex-wrap: wrap; gap: 16px;">
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">区间最大涨幅：</span>
              <span style="font-size: 18px; font-weight: 600;" :class="getGainColor(stockInfo.max_gain)">{{ stockInfo.max_gain }}%</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">最大单日涨幅：</span>
              <span style="font-size: 18px; font-weight: 600;" :class="getGainColor(stockInfo.max_daily_gain)">{{ stockInfo.max_daily_gain }}%</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">当日涨幅：</span>
              <span style="font-size: 18px; font-weight: 600;" :class="getGainColor(stockInfo.today_gain)">{{ stockInfo.today_gain }}%</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">次日涨幅：</span>
              <span style="font-size: 18px; font-weight: 600;" :class="getGainColor(stockInfo.next_day_rise)">{{ stockInfo.next_day_rise }}%</span>
            </div>
          </div>
        </el-card>

        <!-- 升浪形态评分卡片 -->
        <el-card style="flex: 1; min-width: 300px; background: white; border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <template #header>
            <span style="font-size: 18px; font-weight: 600; color: #1f2937;">形态评分</span>
          </template>

          <div style="display: flex; flex-wrap: wrap; gap: 16px; position: relative; min-height: 100px; align-items: flex-start;">
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">交易日期：</span>
              <span style="font-size: 18px; font-weight: 600; color: #1f2937;">{{ stockInfo.trade_date }}</span>
            </div>
            <div style="display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">筛选配置：</span>
              <span style="font-size: 14px; color: #4b5563;">
                尾盘: {{ stockInfo.weipan_exceed ? '✓' : '✗' }}
                早盘: {{ stockInfo.zaopan_exceed ? '✓' : '✗' }}
                升浪: {{ stockInfo.rising_wave ? '✓' : '✗' }}
              </span>
            </div>
            <div style="position: absolute; bottom: 10px; right: 10px; display: flex; align-items: center;">
              <span style="font-size: 14px; color: #4b5563; margin-right: 8px;">形态评分：</span>
              <span style="font-size: 18px; font-weight: 600; color: #f97316;">{{ stockInfo.rising_wave_score }}</span>
            </div>
          </div>
        </el-card>
      </div>

      <!-- 30日行情表格 -->
      <el-card class="bg-white border border-gray-200 shadow-lg">
        <template #header>
          <span class="text-lg font-semibold text-gray-800">最近30日行情</span>
        </template>

        <el-table
          v-loading="historyLoading"
          :data="stockHistory"
          stripe
          style="width: 100%"
          header-row-class-name="bg-gray-50 text-gray-800"
          row-class-name="bg-white"
          :max-height="460"
          :scrollbar-always-on="true"
        >
          <el-table-column prop="date" label="日期" width="120" />
          <el-table-column label="开盘涨幅" width="100">
            <template #default="scope">
              <span :class="getGainColor(scope.row.open_change_pct)">
                {{ scope.row.open_change_pct !== undefined && scope.row.open_change_pct !== null ? scope.row.open_change_pct.toFixed(2) + '%' : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="open_score" label="开盘得分" width="80">
            <template #default="scope">
              <span :class="getGainColor(scope.row.open_score)">
                {{ scope.row.open_score !== undefined && scope.row.open_score !== null ? scope.row.open_score : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="收盘涨幅" width="80">
            <template #default="scope">
              <span :class="getGainColor(scope.row.change_pct)">
                {{ scope.row.change_pct !== undefined && scope.row.change_pct !== null ? scope.row.change_pct + '%' : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="收盘乖离率" width="100">
            <template #default="scope">
              <span :class="getBiasColor(scope.row.bias)">
                {{ scope.row.bias !== undefined && scope.row.bias !== null ? scope.row.bias + '%' : '-' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="竞价成交额" width="110">
            <template #default="scope">
              <template v-if="scope.row.open_amount">
                <span :class="getAuctionAmountColor(scope.row.open_amount, scope.$index < stockHistory.length - 1 ? stockHistory[scope.$index + 1]?.open_amount : null)">
                  {{ formatAmount(scope.row.open_amount) || '-' }}
                </span>
                <!-- <span v-if="scope.$index < stockHistory.length - 1" class="text-xs ml-1" :class="getAuctionAmountColor(scope.row.open_amount, stockHistory[scope.$index + 1]?.open_amount)">
                  ({{ getAuctionAmountChange(scope.row.open_amount, stockHistory[scope.$index + 1]?.open_amount) !== null ? (getAuctionAmountChange(scope.row.open_amount, stockHistory[scope.$index + 1]?.open_amount) > 0 ? '+' : '') + getAuctionAmountChange(scope.row.open_amount, stockHistory[scope.$index + 1]?.open_amount) + '%' : '' }})
                </span> -->
              </template>
              <template v-else>-</template>
            </template>
          </el-table-column>
          <el-table-column prop="open" label="开盘价" width="90" />
          <el-table-column prop="close" label="收盘价" width="90" />
          <el-table-column prop="avg_price" label="收盘均价" width="90" />
          <el-table-column prop="low" label="最低价" width="90" />
          <el-table-column prop="high" label="最高价" width="90" />
        </el-table>
      </el-card>
    </div>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import axios from 'axios'
import { getValueColor, getAuctionAmountColor, getAuctionAmountChange } from '../../utils/colorUtils.js'
import { formatAmount } from '../../utils/commonUtils.js'

const props = defineProps({
  selectedStock: {
    type: Object,
    default: null
  }
})

const emit = defineEmits(['back'])

const stockHistory = ref([])
const historyLoading = ref(false)
const stockInfo = ref(null)

// 股票查询相关
const stockCode = ref('')
const stockCodeError = ref('')
const isValidStockCode = ref(false)

// 使用共用的颜色函数
const getDiffColor = getValueColor
const getGainColor = getValueColor
const getBiasColor = getValueColor

const loadStockHistory = async (code) => {
  historyLoading.value = true
  try {
    const res = await axios.get(`http://127.0.0.1:8000/api/stock/get-stock-history?code=${code}&days=30`)
    stockHistory.value = res.data || []
  } catch (error) {
    console.error('获取历史数据失败:', error)
    stockHistory.value = []
  } finally {
    historyLoading.value = false
  }
}

const handleBack = () => {
  emit('back')
}

// 验证股票代码
const validateStockCode = () => {
  const code = stockCode.value.trim()
  if (!code) {
    stockCodeError.value = ''
    isValidStockCode.value = false
    return
  }

  if (!/^\d{6}$/.test(code)) {
    stockCodeError.value = '请输入6位数字股票代码'
    isValidStockCode.value = false
    return
  }

  stockCodeError.value = ''
  isValidStockCode.value = true
}

// 查询个股
const queryStock = async () => {
  if (!isValidStockCode.value) return

  const code = stockCode.value.trim()

  try {
    // 先获取股票基本信息
    const infoRes = await axios.get(`http://127.0.0.1:8000/api/stock/get-stock-info?code=${code}`)
    const stockInfo = infoRes.data

    if (stockInfo) {
      // 加载历史数据
      await loadStockHistory(code)
    } else {
      console.error('未找到股票信息')
    }
  } catch (error) {
    console.error('查询股票失败:', error)
  }
}

// 监听selectedStock变化，自动加载完整股票信息和历史数据
watch(() => props.selectedStock, async (newStock) => {
  if (newStock) {
    stockCode.value = newStock.code
    validateStockCode()

    try {
      // 先获取股票基本信息
      const infoRes = await axios.get(`http://127.0.0.1:8000/api/stock/get-stock-info?code=${newStock.code}`)
      const stockInfoData = infoRes.data

      if (stockInfoData) {
        stockInfo.value = stockInfoData
        // 加载历史数据
        await loadStockHistory(newStock.code)
      } else {
        console.error('未找到股票信息')
      }
    } catch (error) {
      console.error('查询股票失败:', error)
    }
  }
}, { immediate: true })
</script>
