<template>
  <div>
    <div style="background-color: white; border: 1px solid #e0e0e0;" class="rounded-lg p-4 mb-6 shadow-lg">
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
            @change="handleDateChange"
            class="!bg-white !text-gray-800 !border-gray-300"
            style="width: 150px;"
          />
        </div>

        <!-- 复选框 -->
        <el-checkbox v-model="filters.weipan_exceed" class="!text-gray-800">尾盘超预期</el-checkbox>
        <el-checkbox v-model="filters.zaopan_exceed" class="!text-gray-800">早盘超预期</el-checkbox>
        <el-checkbox v-model="filters.rising_wave" class="!text-gray-800">上升形态</el-checkbox>
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

        <!-- 刷新按钮和最后更新（放在行末尾） -->
        <div class="flex items-center gap-4" style="margin-left: auto;">
          <el-button
            type="primary"
            icon="Refresh"
            :loading="loading"
            @click="runStrategy"
            class="!bg-blue-600 hover:!bg-blue-700"
          >
            刷新获取数据
          </el-button>
          <span class="text-sm text-gray-600 whitespace-nowrap">
            最后更新：{{ lastUpdate || '未更新' }}
          </span>
        </div>
      </div>
    </div>

    <el-card class="bg-white border border-gray-200 shadow-lg rounded-lg p-4">
      <el-table
        :data="list"
        stripe
        style="width: 100%"
        header-row-class-name="bg-gray-50 text-gray-800"
        row-class-name="bg-white hover:bg-gray-50 cursor-pointer"
        @row-click="handleRowClick"
      >
        <el-table-column type="index" label="序号" width="60" />
        <el-table-column label="股票代码" width="90">
          <template #default="scope">
            {{ scope.row.code }}
          </template>
        </el-table-column>
        <el-table-column prop="stock_name" label="名称" width="90" />
        <el-table-column prop="auction_start_price" label="竞价开始价" width="100" />
        <el-table-column prop="auction_end_price" label="竞价结束价" width="100" />
        <el-table-column label="价格差异" width="80">
          <template #default="scope">
            <span :class="getValueColor(scope.row.price_diff)">{{ scope.row.price_diff }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="max_gain" label="最大涨幅" width="90" />
        <el-table-column prop="max_daily_gain" label="最大日涨幅" width="100" />
        <el-table-column label="今日涨幅" width="80">
          <template #default="scope">
            <span :class="getValueColor(scope.row.today_gain)">{{ scope.row.today_gain }}%</span>
          </template>
        </el-table-column>
        <el-table-column label="次日涨幅" width="80">
          <template #default="scope">
            <span :class="getValueColor(scope.row.next_day_gain)">{{ scope.row.next_day_gain }}%</span>
          </template>
        </el-table-column>
        <el-table-column prop="trade_date" label="交易日期" width="120" />
        <el-table-column prop="higher_score" label="超预期分" width="80" />
      </el-table>
      <div class="text-sm text-gray-400 mt-2">总选出数量：{{ list.length }}</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'
import { ElMessage } from 'element-plus'
import { getValueColor } from '../../utils/colorUtils.js'

const props = defineProps({
  activeTab: {
    type: String,
    default: 'auction'
  }
})

const emit = defineEmits(['tabChange', 'selectStock'])

const loading = ref(false)
const list = ref([])
const lastUpdate = ref('')
const selectedDate = ref('')
const tradeDates = ref([])
const filters = ref({
  'weipan_exceed': false,
  'zaopan_exceed': false,
  'rising_wave': true  // 默认勾选上升形态
})

// 板块相关
const blockList = ref([])
const selectedBlock = ref('')
const selectedBlocks = ref([])

// 默认选中的板块代码
const defaultBlockCodes = ['880656', '880670', '880550', '880672'] // CPO概念, 光通信, PCB概念, 存储芯片

const handleDateChange = (date) => {
  selectedDate.value = date
}

// 加载板块列表
const loadBlockList = async () => {
  try {
    const response = await axios.get('http://127.0.0.1:8000/get-block-list')
    blockList.value = response.data || []
    
    // 设置默认选中的板块
    const defaultBlocks = blockList.value.filter(block => 
      defaultBlockCodes.includes(block.code)
    )
    selectedBlocks.value = defaultBlocks
  } catch (error) {
    console.error('加载板块列表失败:', error)
    ElMessage.error('加载板块列表失败')
  }
}

// 处理板块选择
const handleBlockSelect = (blockCode) => {
  const block = blockList.value.find(b => b.code === blockCode)
  if (block && !selectedBlocks.value.some(b => b.code === blockCode)) {
    selectedBlocks.value.push(block)
  }
  selectedBlock.value = '' // 清空选择
}

// 移除板块
const removeBlock = (blockCode) => {
  selectedBlocks.value = selectedBlocks.value.filter(b => b.code !== blockCode)
}

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
      params.append('block_codes', blockCodes)
    }
    await axios.get(`http://127.0.0.1:8000/run-strategy?${params.toString()}`)
    await getData()
    lastUpdate.value = new Date().toLocaleString()
  } finally {
    loading.value = false
  }
}

const getData = async () => {
  const res = await axios.get('http://127.0.0.1:8000/get-data')
  let data = res.data || []
  data.sort((a, b) => {
    const scoreA = parseFloat(a.higher_score) || 0
    const scoreB = parseFloat(b.higher_score) || 0
    return scoreB - scoreA
  })
  list.value = data
}

const getTradeDates = async () => {
  const res = await axios.get('http://127.0.0.1:8000/get-trade-dates')
  tradeDates.value = res.data || []
}

const isTradingDay = (date) => {
  return tradeDates.value.includes(date)
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
  await getTradeDates()
  await loadBlockList()  // 加载板块列表
  await getData()
})
</script>
