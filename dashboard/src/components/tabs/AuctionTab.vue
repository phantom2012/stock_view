<template>
  <div>
    <div style="background-color: white; border: 1px solid #e0e0e0;" class="rounded-lg p-4 mb-3 shadow-lg">
      <!-- 筛选条件区域 -->
      <div class="filter-form mb-1">
        <el-form :inline="true" :model="filterForm" class="demo-form-inline">
          <!-- 第一行：筛选条件 -->
          <el-form-item>
            <span class="mr-2">最近</span>
            <el-input
              v-model.number="filterForm.recentDays"
              placeholder="天数"
              style="width: 50px;"
              @input="handleDaysInput"
            />
            <span class="mx-2">日内最大涨幅</span>
            <el-input
              v-model.number="filterForm.maxGain"
              placeholder="百分比"
              style="width: 50px;"
              @input="handleGainInput"
            />
            <span class="ml-1 mr-4">%</span>

            <span class="mr-2">最近</span>
            <el-input
              v-model.number="filterForm.dailyGainDays"
              placeholder="天数"
              style="width: 40px;"
              @input="handleDailyGainDaysInput"
            />
            <span class="mx-2">日单日最大涨幅></span>
            <el-input
              v-model.number="filterForm.dailyGainThreshold"
              placeholder="百分比"
              style="width: 40px;"
              @input="handleDailyGainThresholdInput"
            />
            <span class="ml-1 mr-4">%</span>

            <span class="mr-2">股价不低于近期高点</span>
            <el-input
              v-model.number="filterForm.priceRatio"
              placeholder="百分比"
              style="width: 100px;"
              @input="handleRatioInput"
            />
            <span class="ml-1 mr-4">%</span>
          </el-form-item>
        </el-form>
      </div>

      <!-- 第一行：日期选择和复选框 -->
      <div style="display: flex; align-items: center; gap: 24px; flex-wrap: nowrap; width: 100%; margin-bottom: 16px;">
        <!-- 日期选择器 -->
        <div class="auction-date-picker-wrap" style="margin-right: 20px;">
          <el-date-picker
            v-model="selectedDate"
            type="date"
            placeholder="选择日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            :clearable="false"
            :disabled-date="disabledDate"
            @change="handleDateChange"
            class="!bg-white !text-gray-800 !border-gray-300"
            style="width: 150px;"
          />
        </div>

        <!-- 复选框 -->
        <el-checkbox v-model="filters.zaopan_exceed" class="!text-gray-800">早盘超预期</el-checkbox>
        <el-checkbox v-model="filters.weipan_exceed" class="!text-gray-800">尾盘超预期</el-checkbox>
        <el-checkbox v-model="filters.rising_wave" class="!text-gray-800">上升形态</el-checkbox>

        <!-- 仅筛选主板 -->
        <el-checkbox v-model="filterForm.onlyMainBoard" class="!text-gray-800">仅筛选主板</el-checkbox>
      </div>

      <!-- 第二行：板块选择和刷新按钮 -->
      <div style="display: flex; align-items: center; gap: 16px; flex-wrap: nowrap; width: 100%;">
        <!-- 板块选择 -->
        <div style="display: flex; align-items: center; gap: 8px;">
          <span class="text-gray-700 whitespace-nowrap">选择板块</span>
          <el-select
            v-model="selectedBlock"
            filterable
            placeholder="搜索并选择板块"
            style="width: 200px;"
            @change="handleBlockSelect"
          >
            <el-option
              v-for="block in blockList"
              :key="block.code"
              :label="block.name"
              :value="block.code"
            >
              <span style="float: left">{{ block.name }}</span>
              <span style="float: right; color: #8492a6; font-size: 13px">{{ block.code }}</span>
            </el-option>
          </el-select>
        </div>

        <!-- 已选板块标签 -->
        <div v-if="selectedBlocks.length > 0" style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap; flex: 1;">
          <el-tag
            v-for="block in selectedBlocks"
            :key="block.code"
            closable
            @close="removeBlock(block.code)"
            size="small"
          >
            {{ block.name }}
          </el-tag>
        </div>

        <!-- 按钮组（放在行末尾） -->
        <div class="flex items-center gap-4" style="margin-left: auto;">
          <el-button
            type="primary"
            icon="Refresh"
            :loading="loading"
            @click="runStrategy"
          >
            筛选
          </el-button>
          <el-button
            type="success"
            @click="loadAuctionData"
            :loading="loadingAuction"
            icon="Download"
          >
            加载竞价
          </el-button>
          <el-button
            type="success"
            @click="loadMoneyFlowData"
            :loading="loadingMoneyFlow"
            icon="Wallet"
          >
            加载资金
          </el-button>
        </div>
      </div>
    </div>

    <el-card class="bg-white border border-gray-200 shadow-lg rounded-lg p-4">
      <el-table
        :data="list"
        stripe
        style="width: 100%;"
        height="600"
        header-row-class-name="bg-gray-50 text-gray-800"
        row-class-name="bg-white hover:bg-gray-50 cursor-pointer"
        @row-click="handleRowClick"
        @sort-change="handleSortChange"
      >
        <el-table-column type="index" label="序号" width="60" />
        <el-table-column label="股票代码" width="80">
          <template #default="scope">
            {{ scope.row.code }}
          </template>
        </el-table-column>
        <el-table-column prop="stock_name" label="名称" width="90" />
        <el-table-column label="昨均价" width="80">
          <template #default="scope">
            {{ scope.row.pre_avg_price || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="昨收盘价" width="80">
          <template #default="scope">
            {{ scope.row.pre_close_price || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="昨乖离率" width="80">
          <template #default="scope">
            <span :class="getValueColor(calcYesterdayBias(scope.row))">{{ calcYesterdayBias(scope.row) !== null ? calcYesterdayBias(scope.row) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column label="昨涨幅" width="80">
          <template #default="scope">
            <span :class="getValueColor(scope.row.pre_price_gain)">{{ scope.row.pre_price_gain !== undefined && scope.row.pre_price_gain !== null ? scope.row.pre_price_gain : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column label="今开盘价" width="80">
          <template #default="scope">
            {{ scope.row.open_price || '-' }}
          </template>
        </el-table-column>
        <el-table-column label="开盘涨幅" width="80">
          <template #default="scope">
            <span :class="getValueColor(calcOpenGain(scope.row))">{{ calcOpenGain(scope.row) !== null ? calcOpenGain(scope.row) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column label="今日涨幅" width="80" sortable="custom" prop="today_gain">
          <template #default="scope">
            <span :class="getValueColor(calcTodayGain(scope.row))">{{ calcTodayGain(scope.row) !== null ? calcTodayGain(scope.row) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column label="开盘量比" width="80">
          <template #default="scope">
            {{ scope.row.open_volume_ratio !== undefined && scope.row.open_volume_ratio !== null ? parseFloat(scope.row.open_volume_ratio).toFixed(2) : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="次日涨幅" width="80">
          <template #default="scope">
            <span :class="getValueColor(calcNextDayRise(scope.row))">{{ calcNextDayRise(scope.row) !== null ? calcNextDayRise(scope.row) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column prop="interval_max_rise" label="最大涨幅" width="80" sortable="custom">
          <template #default="scope">
            <span :class="getValueColor(scope.row.interval_max_rise)">{{ scope.row.interval_max_rise !== undefined && scope.row.interval_max_rise !== null ? parseFloat(scope.row.interval_max_rise).toFixed(1) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column prop="turn_start_net_amount" label="启动净额" width="90" sortable="custom">
          <template #default="scope">
            <span :class="getValueColor(scope.row.turn_start_net_amount)">{{ formatAmount(scope.row.turn_start_net_amount, 'wan') || '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="turn_start_net_amount_rate" label="启动净量" width="90" sortable="custom">
          <template #default="scope">
            <span :class="getValueColor(scope.row.turn_start_net_amount_rate)">{{ scope.row.turn_start_net_amount_rate !== undefined && scope.row.turn_start_net_amount_rate !== null ? scope.row.turn_start_net_amount_rate.toFixed(3) : '-' }}%</span>
          </template>
        </el-table-column>
        <el-table-column prop="exp_score" label="预期分" width="70" sortable="custom" />
        <el-table-column prop="trade_date" label="交易日期" width="110" />
      </el-table>
      <div class="text-sm text-gray-400 mt-2">总选出数量：{{ list.length }}</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import axios from 'axios'
import { ElMessage } from 'element-plus'
import { getValueColor } from '../../utils/colorUtils.js'
import { formatAmount } from '../../utils/commonUtils.js'
import { calcTodayGain, calcNextDayRise, calcOpenGain, calcYesterdayBias } from '../../utils/stockCalcUtils.js'
import { useBlockSelection } from '../../composables/useBlockSelection.js'
import { useCalendarStore } from '../../stores/calendarStore.js'
import { API_BASE_URL } from '../../api/config.js'

const props = defineProps({
  activeTab: {
    type: String,
    default: 'auction'
  }
})

const emit = defineEmits(['tabChange', 'selectStock'])

const calendarStore = useCalendarStore()
const loading = ref(false)
const loadingAuction = ref(false)
const loadingMoneyFlow = ref(false)
const list = ref([])

// SSE 相关
let eventSource = null
const sseConnected = ref(false)

// 当前显示的加载消息（用于完成后关闭）
let currentLoadingMsg = null
const selectedDate = ref('')
const filters = ref({
  'weipan_exceed': false,
  'zaopan_exceed': false,
  'rising_wave': true  // 默认勾选上升形态
})

// 筛选表单
const filterForm = ref({
  recentDays: 50,
  maxGain: 30,
  dailyGainDays: 5,
  dailyGainThreshold: 7,
  priceRatio: 80,
  onlyMainBoard: true  // 默认勾选仅筛选主板
})

// 使用板块选择 composable
const {
  blockList,
  selectedBlock,
  selectedBlocks,
  loadBlockList,
  setDefaultBlockCodes,
  handleBlockSelect,
  removeBlock
} = useBlockSelection()

// 从数据库加载筛选配置(type=1)
const loadFilterConfig = async () => {
  try {
    const response = await axios.get('http://127.0.0.1:8000/api/config/get-filter-config', {
      params: { config_type: 1 }
    })
    if (response.data) {
      filterForm.value.recentDays = response.data.interval_days
      filterForm.value.maxGain = response.data.interval_max_rise
      filterForm.value.dailyGainDays = response.data.recent_days
      filterForm.value.dailyGainThreshold = response.data.recent_max_day_rise
      filterForm.value.priceRatio = response.data.prev_high_price_rate

      if (response.data.trade_date) {
        selectedDate.value = response.data.trade_date
      }

      if (response.data.select_blocks) {
        const blockCodes = response.data.select_blocks.split(',')
        const blocks = blockList.value.filter(block => blockCodes.includes(block.code))
        selectedBlocks.value = blocks
      }
    }
  } catch (error) {
    console.error('加载筛选配置失败:', error)
  }
}

const handleDateChange = (date) => {
  selectedDate.value = date
}

const disabledDate = (date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const dateStr = `${year}-${month}-${day}`
  return calendarStore.isNonTradingDate(dateStr)
}

// 处理天数输入，限制为1-365的数字
const handleDaysInput = (value) => {
  const numValue = parseInt(value)
  if (isNaN(numValue)) {
    filterForm.value.recentDays = ''
    return
  }

  if (numValue < 1) {
    filterForm.value.recentDays = 1
  } else if (numValue > 365) {
    filterForm.value.recentDays = 365
  }
}

// 处理涨幅输入，不限制上限
const handleGainInput = (value) => {
  const numValue = parseFloat(value)
  if (isNaN(numValue)) {
    filterForm.value.maxGain = ''
    return
  }

  if (numValue < 0) {
    filterForm.value.maxGain = 0
  }
}

// 处理高点比例输入，限制为0-100的数字
const handleRatioInput = (value) => {
  const numValue = parseFloat(value)
  if (isNaN(numValue)) {
    filterForm.value.priceRatio = ''
    return
  }

  if (numValue < 0) {
    filterForm.value.priceRatio = 0
  } else if (numValue > 100) {
    filterForm.value.priceRatio = 100
  }
}

// 处理日内最大涨幅阈值输入，不限制上限
const handleDailyGainThresholdInput = (value) => {
  const numValue = parseFloat(value)
  if (isNaN(numValue)) {
    filterForm.value.dailyGainThreshold = ''
    return
  }

  if (numValue < 0) {
    filterForm.value.dailyGainThreshold = 0
  }
}

// 处理日内最大涨幅天数输入，限制为1-30的数字
const handleDailyGainDaysInput = (value) => {
  const numValue = parseInt(value)
  if (isNaN(numValue)) {
    filterForm.value.dailyGainDays = ''
    return
  }

  if (numValue < 1) {
    filterForm.value.dailyGainDays = 1
  } else if (numValue > 30) {
    filterForm.value.dailyGainDays = 30
  }
}

// 加载板块列表（由composable提供）
// 重写loadBlockList，加载完成后获取筛选配置
const originalLoadBlockList = loadBlockList
const loadBlockListWithConfig = async () => {
  await originalLoadBlockList()
  await loadFilterConfig()
}

// 处理板块选择（由composable提供）
// 已在模板中调用 handleBlockSelect

// 移除板块（由composable提供）
// 已在模板中调用 removeBlock

const runStrategy = async () => {
  loading.value = true
  try {
    const params = new URLSearchParams()
    if (selectedDate.value) {
      params.append('trade_date', selectedDate.value)
    }
    Object.entries(filters.value).forEach(([key, value]) => {
      params.append(key, value ? '1' : '0')
    })
    // 添加板块筛选参数
    if (selectedBlocks.value.length > 0) {
      const blockCodes = selectedBlocks.value.map(b => b.code).join(',')
      params.append('select_blocks', blockCodes)
    }
    // 添加筛选条件参数
    params.append('interval_days', filterForm.value.recentDays)
    params.append('interval_max_rise', filterForm.value.maxGain)
    params.append('recent_days', filterForm.value.dailyGainDays)
    params.append('recent_max_day_rise', filterForm.value.dailyGainThreshold)
    params.append('prev_high_price_rate', filterForm.value.priceRatio)
    params.append('only_main_board', filterForm.value.onlyMainBoard ? '1' : '0')

    const response = await axios.get(`${API_BASE_URL}/api/strategy/refresh-exceed-list?${params.toString()}`)
    await getData()
    ElMessage.success(`筛选完成，共选出 ${list.value.length} 只股票`)
  } catch (error) {
    ElMessage.error('筛选失败：' + (error.response?.data?.msg || error.message))
  } finally {
    loading.value = false
  }
}

const getData = async () => {
  const res = await axios.get(`${API_BASE_URL}/api/strategy/get-exceed-list`)
  let data = res.data || []
  data.sort((a, b) => {
    const riseA = parseFloat(a.interval_max_rise) || 0
    const riseB = parseFloat(b.interval_max_rise) || 0
    return riseB - riseA
  })
  list.value = data
}

// 加载竞价数据
const loadAuctionData = async () => {
  if (list.value.length === 0) {
    ElMessage.warning('请先筛选股票后再加载竞价数据')
    return
  }

  loadingAuction.value = true

  const loadingMsg = ElMessage({
    message: `正在加载竞价数据，共 ${list.value.length} 只股票...`,
    type: 'info',
    duration: 0
  })

  try {
    const response = await axios.post('http://127.0.0.1:8000/api/data/load-auction-data', list.value, {
      params: { days: 30 }
    })

    loadingMsg.close()

    if (response.data.status === 'success') {
      const result = response.data.data
      ElMessage.success(`加载竞价数据完成：成功 ${result.success} 只，失败 ${result.failed} 只，总计 ${result.total} 只股票`)
    } else {
      ElMessage.error('加载竞价数据失败：' + (response.data.msg || '未知错误'))
    }
  } catch (error) {
    loadingMsg.close()
    console.error('加载竞价数据失败:', error)
    ElMessage.error('加载竞价数据失败，请稍后重试')
  } finally {
    loadingAuction.value = false
  }
}

// 加载资金流向数据（异步方式，等待 SSE 通知）
const loadMoneyFlowData = async () => {
  if (list.value.length === 0) {
    ElMessage.warning('请先筛选股票后再加载资金流向数据')
    return
  }

  loadingMoneyFlow.value = true

  // 保存加载消息引用，以便后续关闭
  currentLoadingMsg = ElMessage({
    message: `正在加载资金流向数据，共 ${list.value.length} 只股票，请稍候...`,
    type: 'info',
    duration: 0
  })

  try {
    if (!sseConnected.value) {
      initSSE()
    }

    const response = await axios.post('http://127.0.0.1:8000/api/data/load-money-flow', list.value, {
      params: { days: filterForm.value.recentDays }
    })

    if (response.data.status !== 'success') {
      if (currentLoadingMsg) {
        currentLoadingMsg.close()
        currentLoadingMsg = null
      }
      ElMessage.error('触发资金流向数据同步失败：' + (response.data.msg || '未知错误'))
      loadingMoneyFlow.value = false
      return
    }

    console.log('资金流向同步已触发，等待后台处理完成...')

  } catch (error) {
    if (currentLoadingMsg) {
      currentLoadingMsg.close()
      currentLoadingMsg = null
    }
    console.error('触发资金流向数据同步失败:', error)
    ElMessage.error('触发资金流向数据同步失败，请稍后重试')
    loadingMoneyFlow.value = false
  }
}

// 处理资金流向同步完成
const handleMoneyFlowComplete = async (success, message) => {
  loadingMoneyFlow.value = false

  // 关闭之前显示的加载消息
  if (currentLoadingMsg) {
    currentLoadingMsg.close()
    currentLoadingMsg = null
  }

  if (success) {
    try {
      // 刷新表格数据
      await getData()
      ElMessage.success(`资金流向数据加载完成！${message || ''}`)
    } catch (error) {
      console.error('刷新表格数据失败:', error)
      ElMessage.success(`资金流向数据加载完成，但刷新表格失败：${error.message}`)
    }
  } else {
    ElMessage.error('资金流向数据加载失败：' + (message || '未知错误'))
  }
}

// 初始化 SSE 连接
const initSSE = () => {
  if (eventSource) {
    return
  }

  try {
    eventSource = new EventSource('http://127.0.0.1:8000/api/data/sse')

    eventSource.onopen = () => {
      console.log('SSE 连接已建立')
      sseConnected.value = true
    }

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        console.log('收到 SSE 消息:', data)

        if (data.type === 'sync_complete') {
          if (data.sync_type === 'money_flow') {
            handleMoneyFlowComplete(data.success, data.message)
          }
        }
      } catch (error) {
        console.error('解析 SSE 消息失败:', error)
      }
    }

    eventSource.onerror = (error) => {
      console.error('SSE 连接错误:', error)
      sseConnected.value = false
      setTimeout(() => {
        if (eventSource) {
          eventSource.close()
          eventSource = null
        }
        initSSE()
      }, 5000)
    }
  } catch (error) {
    console.error('初始化 SSE 失败:', error)
  }
}

// 关闭 SSE 连接
const closeSSE = () => {
  if (eventSource) {
    eventSource.close()
    eventSource = null
    sseConnected.value = false
    console.log('SSE 连接已关闭')
  }
}

const handleSortChange = (sort) => {
  if (!sort.prop || !sort.order) {
    return
  }

  const { prop, order } = sort

  list.value.sort((a, b) => {
    let valA, valB

    if (prop === 'today_gain') {
      valA = calcTodayGain(a) || 0
      valB = calcTodayGain(b) || 0
    } else {
      valA = parseFloat(a[prop]) || 0
      valB = parseFloat(b[prop]) || 0
    }

    if (order === 'ascending') {
      return valB - valA
    } else {
      return valA - valB
    }
  })
}

const handleRowClick = (row, column, event) => {
  if (!event || !event.target) {
    return
  }

  const cell = event.target.closest('td')
  if (!cell) {
    return
  }

  const cellIndex = Array.from(cell.parentElement.children).indexOf(cell)

  // 检查是否点击的是前两列（序号和股票代码）
  if (cellIndex < 2) {
    return // 前两列不触发跳转
  }

  emit('selectStock', row, 'auction')
}

onMounted(async () => {
  await calendarStore.fetchCalendarData()  // 加载交易日历数据
  await loadBlockListWithConfig()  // 加载板块列表和筛选配置
  await getData()
  initSSE()
})

onUnmounted(() => {
  closeSSE()
})
</script>

<style scoped>
:deep(.el-table__sort-icon),
:deep(.caret-wrapper),
:deep(.el-icon-caret-top),
:deep(.el-icon-caret-bottom),
:deep(.el-icon-arrow-up),
:deep(.el-icon-arrow-down) {
  display: none !important;
}
</style>
