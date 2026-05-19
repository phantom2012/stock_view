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

      <!-- 估值信息卡片 -->
      <el-card v-if="valuationData" class="bg-white" style="border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <template #header>
          <div style="display: flex; align-items: center; justify-content: space-between;">
            <span style="font-size: 18px; font-weight: 600; color: #1f2937;">股价估值评估</span>
            <div style="display: flex; align-items: center; gap: 12px;">
              <span style="font-size: 13px; color: #6b7280;">
                当前价: <strong>{{ formatPrice(valuationData.current_price) }}</strong>
              </span>
              <span v-if="valuationData.trade_date" style="font-size: 13px; color: #6b7280;">
                数据日期: {{ valuationData.trade_date }}
              </span>
            </div>
          </div>
        </template>

        <div v-loading="valuationLoading">
          <!-- 第一行：综合结论 -->
          <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #f3f4f6;">
            <div style="display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px; background: #f0f9ff;">
              <span style="font-size: 13px; color: #4b5563;">综合判定:</span>
              <span :style="{
                fontSize: '20px', fontWeight: '700',
                color: getValuationStatusColor(valuationData.valuation.overall_status)
              }">
                {{ valuationData.valuation.overall_status }}
              </span>
              <span style="font-size: 12px; color: #9ca3af; margin-left: 4px;">
                (均分 {{ valuationData.valuation.avg_score }})
              </span>
            </div>

            <div style="display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px; background: #faf5ff;">
              <span style="font-size: 13px; color: #4b5563;">置信度:</span>
              <span :style="{
                fontSize: '16px', fontWeight: '600',
                color: valuationData.valuation.confidence === '高' ? '#059669' :
                       valuationData.valuation.confidence === '中' ? '#d97706' : '#9ca3af'
              }">
                {{ valuationData.valuation.confidence }}
              </span>
              <span style="font-size: 12px; color: #9ca3af;">
                ({{ valuationData.valuation.factor_count }}/5因子生效)
              </span>
            </div>

            <div v-if="valuationData.valuation.fair_price" style="display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px; background: #ecfdf5;">
              <span style="font-size: 13px; color: #4b5563;">合理公平价:</span>
              <span style="font-size: 18px; font-weight: 700; color: #059669;">
                {{ formatPrice(valuationData.valuation.fair_price) }}
              </span>
            </div>

            <div v-if="valuationData.valuation.upside_potential !== null" style="display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px;" :style="{background: valuationData.valuation.upside_potential > 0 ? '#f0fdf4' : '#fef2f2'}">
              <span style="font-size: 13px; color: #4b5563;">上涨空间:</span>
              <span style="font-size: 16px; font-weight: 700;" :style="{color: valuationData.valuation.upside_potential > 0 ? '#16a34a' : '#dc2626'}">
                {{ valuationData.valuation.upside_potential > 0 ? '+' : '' }}{{ valuationData.valuation.upside_potential }}%
              </span>
            </div>

            <div v-if="valuationData.valuation.downside_risk !== null" style="display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-radius: 8px; background: #fef2f2;">
              <span style="font-size: 13px; color: #4b5563;">回调风险:</span>
              <span style="font-size: 16px; font-weight: 700; color: #dc2626;">
                {{ valuationData.valuation.downside_risk }}%
              </span>
            </div>
          </div>

          <!-- 第二行：财务关键指标 -->
          <div v-if="valuationData.financial" style="display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #f3f4f6;">
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 70px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">TTM EPS</span>
              <span style="font-size: 15px; font-weight: 600; color: #1f2937;">{{ valuationData.financial.ttm_eps }}</span>
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 70px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">每股净资产</span>
              <span style="font-size: 15px; font-weight: 600; color: #1f2937;">{{ valuationData.financial.bps }}</span>
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 70px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">ROE</span>
              <span :style="{fontSize: '15px', fontWeight: '600', color: (valuationData.financial.roe || 0) > 15 ? '#059669' : '#d97706'}">
                {{ valuationData.financial.roe }}%
              </span>
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 80px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">毛利率</span>
              <span style="font-size: 15px; font-weight: 600; color: #1f2937;">{{ valuationData.financial.grossprofit_margin }}%</span>
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 80px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">净利率</span>
              <span style="font-size: 15px; font-weight: 600; color: #1f2937;">{{ valuationData.financial.netprofit_margin }}%</span>
            </div>
            <div style="display: flex; flex-direction: column; align-items: center; min-width: 80px;">
              <span style="font-size: 11px; color: #9ca3af; margin-bottom: 2px;">净利润增速</span>
              <span :style="{fontSize: '15px', fontWeight: '600', color: (valuationData.financial.growth_rate || 0) > 0 ? '#059669' : '#dc2626'}">
                {{ valuationData.financial.growth_rate > 0 ? '+' : '' }}{{ (valuationData.financial.growth_rate * 100).toFixed(1) }}%
              </span>
            </div>
          </div>

          <!-- 第三行：各维度详情 -->
          <div style="display: flex; flex-wrap: wrap; gap: 12px;">
            <!-- PE 维度 -->
            <div v-if="valuationData.factors.pe" style="flex: 1; min-width: 180px; padding: 12px; border-radius: 8px; background: #f9fafb; border: 1px solid #e5e7eb;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span style="font-size: 13px; font-weight: 600; color: #374151;">① PE估值</span>
                <el-tag :type="getFactorTagType(valuationData.factors.pe.status)" size="small" effect="plain">
                  {{ valuationData.factors.pe.status }}
                </el-tag>
              </div>
              <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                PE: <strong>{{ valuationData.factors.pe.pe }}</strong>
                <span v-if="valuationData.factors.pe.fair_price"> | 公平价: <strong>{{ formatPrice(valuationData.factors.pe.fair_price) }}</strong></span>
              </div>
              <div style="width: 100%; height: 4px; background: #e5e7eb; border-radius: 2px;">
                <div :style="{width: getFactorScoreWidth(valuationData.factors.pe.score) + '%', height: '100%', borderRadius: '2px', background: getFactorScoreColor(valuationData.factors.pe.score)}"></div>
              </div>
            </div>

            <!-- PB/ROE 维度 -->
            <div v-if="valuationData.factors.pb_roe" style="flex: 1; min-width: 180px; padding: 12px; border-radius: 8px; background: #f9fafb; border: 1px solid #e5e7eb;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span style="font-size: 13px; font-weight: 600; color: #374151;">② PB/ROE估值</span>
                <el-tag :type="getFactorTagType(valuationData.factors.pb_roe.status)" size="small" effect="plain">
                  {{ valuationData.factors.pb_roe.status }}
                </el-tag>
              </div>
              <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                PB: <strong>{{ valuationData.factors.pb_roe.pb }}</strong>
                | ROE: <strong>{{ valuationData.factors.pb_roe.roe }}%</strong>
                <span v-if="valuationData.factors.pb_roe.fair_price"> | 公平价: <strong>{{ formatPrice(valuationData.factors.pb_roe.fair_price) }}</strong></span>
              </div>
              <div style="width: 100%; height: 4px; background: #e5e7eb; border-radius: 2px;">
                <div :style="{width: getFactorScoreWidth(valuationData.factors.pb_roe.score) + '%', height: '100%', borderRadius: '2px', background: getFactorScoreColor(valuationData.factors.pb_roe.score)}"></div>
              </div>
            </div>

            <!-- PEG 维度 -->
            <div v-if="valuationData.factors.peg" style="flex: 1; min-width: 180px; padding: 12px; border-radius: 8px; background: #f9fafb; border: 1px solid #e5e7eb;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span style="font-size: 13px; font-weight: 600; color: #374151;">③ PEG成长估值</span>
                <el-tag :type="getFactorTagType(valuationData.factors.peg.status)" size="small" effect="plain">
                  {{ valuationData.factors.peg.status }}
                </el-tag>
              </div>
              <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                <span v-if="valuationData.factors.peg.peg">PEG: <strong>{{ valuationData.factors.peg.peg }}</strong></span>
                <span v-if="valuationData.factors.peg.pe"> | PE: <strong>{{ valuationData.factors.peg.pe }}</strong></span>
                <span v-if="valuationData.factors.peg.fair_price"> | 公平价: <strong>{{ formatPrice(valuationData.factors.peg.fair_price) }}</strong></span>
              </div>
              <div style="width: 100%; height: 4px; background: #e5e7eb; border-radius: 2px;">
                <div :style="{width: getFactorScoreWidth(valuationData.factors.peg.score) + '%', height: '100%', borderRadius: '2px', background: getFactorScoreColor(valuationData.factors.peg.score)}"></div>
              </div>
            </div>

            <!-- PS 维度 -->
            <div v-if="valuationData.factors.ps" style="flex: 1; min-width: 150px; padding: 12px; border-radius: 8px; background: #f9fafb; border: 1px solid #e5e7eb;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span style="font-size: 13px; font-weight: 600; color: #374151;">④ PS市销率</span>
                <el-tag :type="getFactorTagType(valuationData.factors.ps.status)" size="small" effect="plain">
                  {{ valuationData.factors.ps.status }}
                </el-tag>
              </div>
              <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                PS: <strong>{{ valuationData.factors.ps.ps }}</strong>
              </div>
              <div style="width: 100%; height: 4px; background: #e5e7eb; border-radius: 2px;">
                <div :style="{width: getFactorScoreWidth(valuationData.factors.ps.score) + '%', height: '100%', borderRadius: '2px', background: getFactorScoreColor(valuationData.factors.ps.score)}"></div>
              </div>
            </div>

            <!-- 健康度 -->
            <div v-if="valuationData.factors.health" style="flex: 1; min-width: 150px; padding: 12px; border-radius: 8px; background: #f9fafb; border: 1px solid #e5e7eb;">
              <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <span style="font-size: 13px; font-weight: 600; color: #374151;">⑤ 资产负债</span>
                <el-tag :type="getFactorTagType(valuationData.factors.health.status)" size="small" effect="plain">
                  {{ valuationData.factors.health.status }}
                </el-tag>
              </div>
              <div style="font-size: 12px; color: #6b7280; margin-bottom: 4px;">
                负债率: <strong>{{ valuationData.factors.health.debt_to_assets }}%</strong>
                <span v-if="valuationData.factors.health.current_ratio"> | 流动比: <strong>{{ valuationData.factors.health.current_ratio }}</strong></span>
              </div>
              <div style="width: 100%; height: 4px; background: #e5e7eb; border-radius: 2px;">
                <div :style="{width: getFactorScoreWidth(valuationData.factors.health.score) + '%', height: '100%', borderRadius: '2px', background: getFactorScoreColor(valuationData.factors.health.score)}"></div>
              </div>
            </div>
          </div>
        </div>
      </el-card>

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
import { formatAmount, formatPrice } from '../../utils/commonUtils.js'

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
const valuationData = ref(null)
const valuationLoading = ref(false)

// 股票查询相关
const stockCode = ref('')
const stockCodeError = ref('')
const isValidStockCode = ref(false)

// 使用共用的颜色函数
const getDiffColor = getValueColor
const getGainColor = getValueColor
const getBiasColor = getValueColor

const getValuationStatusColor = (status) => {
  const colorMap = {
    '低估': '#059669',
    '合理偏低': '#0284c7',
    '合理': '#2563eb',
    '偏高': '#d97706',
    '高估': '#dc2626',
    '严重低估': '#16a34a',
    '严重高估': '#b91c1c',
    '亏损': '#991b1b',
  }
  return colorMap[status] || '#6b7280'
}

const getFactorTagType = (status) => {
  if (!status) return 'info'
  if (status === '亏损') return 'danger'
  if (status.includes('低估')) return 'success'
  if (status === '合理' || status === '健康' || status === '一般') return 'primary'
  if (status.includes('偏高') || status.includes('偏低')) return 'warning'
  if (status.includes('高估') || status.includes('较差')) return 'danger'
  return 'info'
}

const getFactorScoreWidth = (score) => {
  if (score === null || score === undefined) return 0
  return Math.min(score, 100)
}

const getFactorScoreColor = (score) => {
  if (score === null || score === undefined) return '#d1d5db'
  if (score >= 70) return '#10b981'
  if (score >= 50) return '#3b82f6'
  if (score >= 30) return '#f59e0b'
  return '#ef4444'
}

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

const loadValuation = async (code) => {
  valuationLoading.value = true
  valuationData.value = null
  try {
    const res = await axios.get(`http://127.0.0.1:8000/api/data/valuation/${code}`)
    if (res.data && res.data.status === 'success' && res.data.result) {
      valuationData.value = res.data.result
    }
  } catch (error) {
    console.error('获取估值数据失败:', error)
    valuationData.value = null
  } finally {
    valuationLoading.value = false
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
      // 加载历史数据和估值数据
      await Promise.all([
        loadStockHistory(code),
        loadValuation(code)
      ])
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
        await Promise.all([
          loadStockHistory(newStock.code),
          loadValuation(newStock.code)
        ])
      } else {
        console.error('未找到股票信息')
      }
    } catch (error) {
      console.error('查询股票失败:', error)
    }
  }
}, { immediate: true })
</script>
