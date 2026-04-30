import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import axios from 'axios'
import { API_BASE_URL } from '../api/config.js'

/**
 * 板块选择 Composable
 * 提供板块选择相关的状态和方法
 */
export function useBlockSelection() {
  const blockList = ref([])
  const selectedBlock = ref('')
  const selectedBlocks = ref([])

  // 默认选中的板块代码
  const defaultBlockCodes = ref([])

  // 加载板块列表
  const loadBlockList = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/api/config/get-block-list`)
      blockList.value = response.data || []
      
      // 如果设置了默认板块代码，加载默认选中的板块
      if (defaultBlockCodes.value.length > 0) {
        const defaultBlocks = blockList.value.filter(block => 
          defaultBlockCodes.value.includes(block.code)
        )
        selectedBlocks.value = defaultBlocks
      }
    } catch (error) {
      console.error('加载板块列表失败:', error)
      ElMessage.error('加载板块列表失败')
    }
  }

  // 设置默认板块代码
  const setDefaultBlockCodes = (codes) => {
    defaultBlockCodes.value = codes
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

  // 清空所有已选板块
  const clearSelectedBlocks = () => {
    selectedBlocks.value = []
    selectedBlock.value = ''
  }

  return {
    blockList,
    selectedBlock,
    selectedBlocks,
    loadBlockList,
    setDefaultBlockCodes,
    handleBlockSelect,
    removeBlock,
    clearSelectedBlocks
  }
}
