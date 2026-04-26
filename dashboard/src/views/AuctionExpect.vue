<template>
  <div class="h-auto min-h-screen bg-white text-gray-800">
    <div class="mb-0 w-full border-b border-gray-200 bg-white shadow-sm">
      <div class="mx-auto w-[83%] px-6" style="padding-left: 20px;">
        <el-tabs v-model="activeTab" class="header-tabs w-full !border-0 !bg-transparent" style="min-width: 500px;">
          <el-tab-pane label="超预期选股" name="auction" />
          <el-tab-pane label="个股详情" name="stock_info" />
          <el-tab-pane label="数据导入" name="data_import" />
          <el-tab-pane label="资金流向" name="flow" />
          <el-tab-pane label="板块强度" name="sector" />
        </el-tabs>
      </div>
    </div>

    <div class="mx-auto w-[85%] px-6 pb-6">
      <AuctionTab 
        v-if="activeTab === 'auction'" 
        :activeTab="activeTab"
        @selectStock="handleSelectStock"
      />
      
      <StockInfoTab 
        v-else-if="activeTab === 'stock_info'" 
        :selectedStock="selectedStock"
        @back="handleBack"
      />
      
      <DataImportTab v-else-if="activeTab === 'data_import'" @selectStock="handleSelectStock" />
      
      <FlowTab v-else-if="activeTab === 'flow'" />
      <SectorTab v-else-if="activeTab === 'sector'" />
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import AuctionTab from '../components/tabs/AuctionTab.vue'
import StockInfoTab from '../components/tabs/StockInfoTab.vue'
import DataImportTab from '../components/tabs/DataImportTab.vue'
import FlowTab from '../components/tabs/FlowTab.vue'
import SectorTab from '../components/tabs/SectorTab.vue'

const activeTab = ref('auction')
const selectedStock = ref(null)
const returnTab = ref('auction')

const handleSelectStock = (stock, fromTab = 'auction') => {
  selectedStock.value = stock
  returnTab.value = fromTab
  activeTab.value = 'stock_info'
}

const handleBack = (tab) => {
  activeTab.value = tab || returnTab.value
}
</script>

<style scoped>
.header-tabs :deep(.el-tabs__item) {
  height: 40px;
  padding: 0 20px;
  margin: 0;
  font-size: 0.9375rem;
  font-weight: 500;
  color: #111827 !important;
  background-color: #ffffff;
  border-radius: 0;
  border-right: 1px solid rgba(0, 0, 0, 0.08);
  border-bottom: 1px solid rgba(0, 0, 0, 0.08);
  box-sizing: border-box;
  min-width: 100px;
  white-space: nowrap;
  transition:
    color 0.15s ease,
    background-color 0.15s ease,
    box-shadow 0.15s ease;
}

.header-tabs :deep(.el-tabs__item:hover) {
  color: #3b82f6 !important;
  background-color: #f9fafb;
}

.header-tabs :deep(.el-tabs__active-bar) {
  background-color: #3b82f6;
  height: 3px;
}

.header-tabs :deep(.el-tabs__item.is-active) {
  color: #3b82f6 !important;
  background-color: #ffffff;
  border-bottom: 3px solid #3b82f6;
  border-right: 1px solid rgba(0, 0, 0, 0.08);
}

/* 第一个tab特殊处理，确保"超预期选股"完全显示 */
.header-tabs :deep(.el-tabs__item:nth-child(1)) {
  min-width: 130px;
}

/* 其他tab保持适当宽度 */
.header-tabs :deep(.el-tabs__item:nth-child(2)),
.header-tabs :deep(.el-tabs__item:nth-child(3)),
.header-tabs :deep(.el-tabs__item:nth-child(4)),
.header-tabs :deep(.el-tabs__item:nth-child(5)) {
  min-width: 100px;
  padding: 0 20px;
}
</style>
