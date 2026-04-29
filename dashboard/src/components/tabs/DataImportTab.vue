<template>
  <div class="data-import-container">
    <!-- 筛选条件区域 -->
    <el-card class="filter-card mb-4">
      <template #header>
        <span class="text-lg font-semibold text-gray-800">股票筛选条件</span>
      </template>

      <div class="filter-form">
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

        <!-- 第二行：板块选择、主板勾选项和按钮 -->
        <div class="flex items-center justify-between mt-4">
          <div class="flex items-center gap-4">
            <div class="flex items-center">
              <span class="mr-2 text-gray-700">选择板块</span>
              <el-select
                v-model="selectedBlock"
                filterable
                placeholder="搜索并选择板块"
                style="width: 250px;"
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

            <!-- 仅筛选主板勾选项 -->
            <el-checkbox v-model="filterForm.onlyMainBoard" size="large">
              仅筛选主板
            </el-checkbox>
          </div>

          <div class="flex items-center gap-2">
            <el-button
              type="primary"
              @click="handleFilter"
              :loading="loading"
              icon="Search"
            >
              筛选
            </el-button>
            <el-button @click="resetFilter" icon="Refresh">
              重置
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
    </el-card>

    <!-- 筛选结果表格 -->
    <el-card v-if="filteredStocks.length > 0 || selectedBlocks.length > 0" class="result-card">
      <template #header>
        <div class="flex justify-between items-center">
          <div class="flex items-center gap-4">
            <span class="text-lg font-semibold text-gray-800">筛选结果</span>
            <!-- 已选板块展示 -->
            <div v-if="selectedBlocks.length > 0" class="flex items-center gap-2">
              <span class="text-sm text-gray-600">已选板块：</span>
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
          </div>
          <span class="text-sm text-gray-600">共 {{ totalCount }} 只股票</span>
        </div>
      </template>

      <el-table
        v-loading="loading"
        :data="currentPageData"
        stripe
        style="width: 100%"
        header-row-class-name="bg-gray-50 text-gray-800"
        row-class-name="bg-white cursor-pointer"
        :cell-style="cellStyle"
        :header-cell-style="headerCellStyle"
        @row-click="handleRowClick"
      >
        <!-- 使用v-for循环渲染3组列 -->
        <template v-for="group in 3" :key="group">
          <!-- 序号列 -->
          <!-- <el-table-column label="序号" width="50">
            <template #default="scope">
              <span>{{ scope.row[`index${group}`] || '-' }}</span>
            </template>
          </el-table-column> -->
          <!-- 股票代码列 -->
          <el-table-column label="股票代码" width="80">
            <template #default="scope">
              <span>{{ scope.row[`code${group}`] || '-' }}</span>
            </template>
          </el-table-column>
          <!-- 股票名称列 -->
          <el-table-column label="股票名称" width="90">
            <template #default="scope">
              <span>{{ scope.row[`name${group}`] || '-' }}</span>
            </template>
          </el-table-column>
          <!-- 区间涨幅列 -->
          <el-table-column label="区间涨幅" width="90">
            <template #default="scope">
              <span v-if="scope.row[`gain${group}`] !== null && scope.row[`gain${group}`] !== undefined" :style="{color: getGainColor(scope.row[`gain${group}`])}">
                {{ scope.row[`gain${group}`].toFixed(2) }}%
              </span>
              <span v-else>-</span>
            </template>
          </el-table-column>
          <!-- 单日涨幅列（最后一组不加group-end）-->
          <el-table-column
            label="单日最大涨幅"
            width="group < 3 ? 160 : 110"
            :class-name="group < 3 ? 'group-end' : ''"
          >
            <template #default="scope">
              <span v-if="scope.row[`maxDailyGain${group}`] !== null && scope.row[`maxDailyGain${group}`] !== undefined" :style="{color: getGainColor(scope.row[`maxDailyGain${group}`])}">
                {{ scope.row[`maxDailyGain${group}`].toFixed(2) }}%
              </span>
              <span v-else>-</span>
            </template>
          </el-table-column>
        </template>
      </el-table>

      <!-- 分页控件 -->
      <div class="pagination-container mt-4 flex justify-center">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[TABLE_PAGE_SIZE, TABLE_PAGE_SIZE * 2, TABLE_PAGE_SIZE * 3, TABLE_PAGE_SIZE * 4]"
          :total="totalCount"
          layout="total, sizes, prev, pager, next, jumper"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
        />
      </div>
    </el-card>

    <!-- 无结果提示 -->
    <el-empty
      v-else-if="!loading && hasSearched"
      description="未找到符合条件的股票"
      class="mt-8"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import axios from 'axios'
import { ElMessage } from 'element-plus'

const emit = defineEmits(['selectStock'])

// ========== 全局常量配置 ==========
const TABLE_ROWS_PER_PAGE = 6 // 表格每页显示的行数
const TABLE_COLS_PER_PAGE = 3 // 表格每行的列数
const TABLE_PAGE_SIZE = TABLE_ROWS_PER_PAGE * TABLE_COLS_PER_PAGE // 每页显示的总数量
// =================================

// 筛选表单
const filterForm = ref({
  recentDays: 50,
  maxGain: 30,
  dailyGainDays: 5,
  dailyGainThreshold: 7,
  priceRatio: 80,
  onlyMainBoard: true  // 默认勾选仅筛选主板
})

// 板块相关
const blockList = ref([])
const selectedBlock = ref('')
const selectedBlocks = ref([])

// 默认选中的板块代码
const defaultBlockCodes = ['880656', '880670', '880550', '880672', '880491'] // CPO概念, 光通信, PCB概念, 存储芯片, 半导体

// 状态管理
const loading = ref(false)
const loadingAuction = ref(false)
const loadingMoneyFlow = ref(false)
const hasSearched = ref(false)
const filteredStocks = ref([])
const currentPage = ref(1)
const pageSize = ref(TABLE_PAGE_SIZE) // 每页显示数量，由常量自动计算

// 加载板块列表
const loadBlockList = async () => {
  try {
    const response = await axios.get('http://127.0.0.1:8000/get-block-list')
    blockList.value = response.data || []
    await loadFilterConfig()
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

// 计算总记录数
const totalCount = computed(() => filteredStocks.value.length)

// 计算当前页数据
const currentPageData = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value
  const end = start + pageSize.value
  return formatTableData(filteredStocks.value.slice(start, end), start)
})

// 格式化表格数据为多列显示（纵向排列）
const formatTableData = (stocks, startIndex) => {
  const groupsCount = TABLE_COLS_PER_PAGE // 列数
  const totalStocks = stocks.length

  // 动态计算需要的行数：总数据量除以列数，向上取整
  const rowsNeeded = Math.ceil(totalStocks / groupsCount)
  const rowsPerPage = Math.min(rowsNeeded, TABLE_ROWS_PER_PAGE) // 最多显示的行数

  const result = []

  // 初始化行数据
  for (let i = 0; i < rowsPerPage; i++) {
    result.push({})
  }

  // 按列填充数据
  for (let group = 0; group < groupsCount; group++) {
    for (let row = 0; row < rowsPerPage; row++) {
      const index = group * rowsPerPage + row
      const stockIndex = startIndex + index
      if (index < totalStocks) {
        result[row][`index${group + 1}`] = startIndex + index + 1
        result[row][`code${group + 1}`] = stocks[index].code
        result[row][`name${group + 1}`] = stocks[index].name
        result[row][`gain${group + 1}`] = stocks[index].interval_max_rise
        result[row][`maxDailyGain${group + 1}`] = stocks[index].max_day_rise
        result[row][`stockIndex${group + 1}`] = stockIndex
      } else {
        result[row][`index${group + 1}`] = null
        result[row][`code${group + 1}`] = null
        result[row][`name${group + 1}`] = null
        result[row][`gain${group + 1}`] = null
        result[row][`maxDailyGain${group + 1}`] = null
        result[row][`stockIndex${group + 1}`] = null
      }
    }
  }

  return result
}

// 获取涨幅颜色
const getGainColor = (value) => {
  if (value === null || value === undefined) return '#9ca3af'
  if (value > 0) return '#ef4444'  // 红色
  if (value < 0) return '#22c55e'  // 绿色
  return '#9ca3af'  // 灰色
}

// 表头样式 - 为每组的最后一列添加更大间距
const headerCellStyle = ({ column }) => {
  if (column.className === 'group-end') {
    return { paddingRight: '0px' }  // 增大到20px
  }
  return {}
}

// 筛选处理
const handleFilter = async () => {
  if (selectedBlocks.value.length === 0) {
    ElMessage.warning('请至少选择一个板块')
    return
  }

  loading.value = true
  hasSearched.value = true

  try {
    const blockCodes = selectedBlocks.value.map(b => b.code)
    const response = await axios.get('http://127.0.0.1:8000/refresh-filter-2-result', {
      params: {
        interval_days: filterForm.value.recentDays,
        interval_max_rise: filterForm.value.maxGain,
        recent_days: filterForm.value.dailyGainDays,
        recent_max_day_rise: filterForm.value.dailyGainThreshold,
        prev_high_price_rate: filterForm.value.priceRatio,
        select_blocks: blockCodes.join(','),
        only_main_board: filterForm.value.onlyMainBoard
      }
    })

    filteredStocks.value = response.data || []

    // 按区间涨幅倒序排列
    filteredStocks.value.sort((a, b) => (b.interval_max_rise || 0) - (a.interval_max_rise || 0))

    // 保存到数据库
    try {
      await axios.post('http://127.0.0.1:8000/save-filter-stocks', filteredStocks.value)
    } catch (error) {
      console.error('保存筛选结果失败:', error)
    }

    currentPage.value = 1 // 重置到第一页

    if (filteredStocks.value.length > 0) {
      ElMessage.success(`找到 ${filteredStocks.value.length} 只符合条件的股票`)
    } else {
      ElMessage.info('未找到符合条件的股票')
    }
  } catch (error) {
    console.error('筛选失败:', error)
    ElMessage.error('筛选失败，请稍后重试')
    filteredStocks.value = []
  } finally {
    loading.value = false
  }
}

// 重置筛选条件
const resetFilter = () => {
  filterForm.value = {
    recentDays: 50,
    maxGain: 30,
    dailyGainDays: 5,
    dailyGainThreshold: 7,
    priceRatio: 80,
    onlyMainBoard: true  // 重置时保持默认勾选
  }
  selectedBlocks.value = []
  selectedBlock.value = ''
  filteredStocks.value = []
  hasSearched.value = false
  currentPage.value = 1
}

// 加载竞价数据
const loadAuctionData = async () => {
  if (filteredStocks.value.length === 0) {
    ElMessage.warning('请先筛选股票后再加载竞价数据')
    return
  }

  loadingAuction.value = true

  // 显示加载提示
  const loadingMsg = ElMessage({
    message: `正在加载竞价数据，共 ${filteredStocks.value.length} 只股票...`,
    type: 'info',
    duration: 0  // 不自动关闭
  })

  try {
    const response = await axios.post('http://127.0.0.1:8000/load-auction-data', filteredStocks.value, {
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

// 加载资金流向数据
const loadMoneyFlowData = async () => {
  if (filteredStocks.value.length === 0) {
    ElMessage.warning('请先筛选股票后再加载资金流向数据')
    return
  }

  loadingMoneyFlow.value = true

  // 显示加载提示
  const loadingMsg = ElMessage({
    message: `正在加载资金流向数据，共 ${filteredStocks.value.length} 只股票...`,
    type: 'info',
    duration: 0  // 不自动关闭
  })

  try {
    const response = await axios.post('http://127.0.0.1:8000/load-money-flow', filteredStocks.value, {
      params: { days: filterForm.value.recentDays }
    })

    loadingMsg.close()

    if (response.data.status === 'success') {
      const result = response.data.data
      ElMessage.success(`加载资金流向数据完成：成功 ${result.success} 只，失败 ${result.failed} 只，总计 ${result.total} 只股票`)
    } else {
      ElMessage.error('加载资金流向数据失败：' + (response.data.msg || '未知错误'))
    }
  } catch (error) {
    loadingMsg.close()
    console.error('加载资金流向数据失败:', error)
    ElMessage.error('加载资金流向数据失败，请稍后重试')
  } finally {
    loadingMoneyFlow.value = false
  }
}

// 处理天数输入，限制为1-365的数字
const handleDaysInput = (value) => {
  // 移除非数字字符
  const numValue = parseInt(value)
  if (isNaN(numValue)) {
    filterForm.value.recentDays = ''
    return
  }

  // 限制范围
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

  // 只限制最小值为0
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

  // 只限制最小值为0
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

  // 限制范围
  if (numValue < 1) {
    filterForm.value.dailyGainDays = 1
  } else if (numValue > 30) {
    filterForm.value.dailyGainDays = 30
  }
}

// 分页大小改变
const handleSizeChange = (val) => {
  pageSize.value = val
  currentPage.value = 1
}

// 当前页改变
const handleCurrentChange = (val) => {
  currentPage.value = val
}

// 处理行点击，跳转到个股详情
const handleRowClick = (row, column, event) => {
  if (!event || !event.target) {
    return
  }

  const cell = event.target.closest('td')
  if (!cell) {
    return
  }

  const cellIndex = Array.from(cell.parentElement.children).indexOf(cell)
  const colCountPerGroup = 4
  const group = Math.floor(cellIndex / colCountPerGroup) + 1

  // 检查是否点击的是股票代码列（每组的第1列，索引为0）
  const relativeCellIndex = cellIndex % colCountPerGroup
  if (relativeCellIndex === 0) {
    return // 股票代码列不触发跳转
  }

  const stockIndex = row[`stockIndex${group}`]
  if (stockIndex !== null && stockIndex !== undefined) {
    const stock = filteredStocks.value[stockIndex]
    if (stock) {
      emit('selectStock', stock, 'data_import')
    }
  }
}

// 组件挂载时加载板块列表和筛选结果
onMounted(() => {
  loadBlockList()
  loadFilterStocks()
})

// 从数据库加载筛选配置(type=2)
const loadFilterConfig = async () => {
  try {
    const response = await axios.get('http://127.0.0.1:8000/get-filter-config', {
      params: { config_type: 2 }
    })
    if (response.data) {
      filterForm.value.recentDays = response.data.interval_days
      filterForm.value.maxGain = response.data.interval_max_rise
      filterForm.value.dailyGainDays = response.data.recent_days
      filterForm.value.dailyGainThreshold = response.data.recent_max_day_rise
      filterForm.value.priceRatio = response.data.prev_high_price_rate

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

// 从数据库加载筛选结果(type=2)
const loadFilterStocks = async () => {
  try {
    const response = await axios.get('http://127.0.0.1:8000/get-filter-2-result')
    if (response.data && response.data.length > 0) {
      filteredStocks.value = response.data
      // 按区间涨幅倒序排列
      filteredStocks.value.sort((a, b) => (b.interval_max_rise || b.gain || 0) - (a.interval_max_rise || a.gain || 0))
      hasSearched.value = true
      currentPage.value = 1
    }
  } catch (error) {
    console.error('加载筛选结果失败:', error)
  }
}
</script>

<style scoped>
.data-import-container {
  padding: 20px 0;
}

.filter-card {
  background: white;
  border: 1px solid #e5e7eb;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.result-card {
  background: white;
  border: 1px solid #e5e7eb;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}

.filter-form {
  padding: 10px 0;
}

.pagination-container {
  padding: 20px 0;
}

.mb-4 {
  margin-bottom: 16px;
}

.mt-4 {
  margin-top: 16px;
}

.mt-8 {
  margin-top: 32px;
}

.ml-1 {
  margin-left: 4px;
}

.mr-2 {
  margin-right: 8px;
}

.mx-2 {
  margin-left: 8px;
  margin-right: 8px;
}

.mr-4 {
  margin-right: 16px;
}

.flex {
  display: flex;
}

.justify-between {
  justify-content: space-between;
}

.items-center {
  align-items: center;
}

.justify-center {
  justify-content: center;
}
</style>
