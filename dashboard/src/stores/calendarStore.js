import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'
import { API_BASE_URL } from '../api/config.js'

export const useCalendarStore = defineStore('calendar', () => {
  const nonTradingDates = ref([])
  const isLoading = ref(false)
  const isLoaded = ref(false)

  const nonTradingDateSet = computed(() => {
    return new Set(nonTradingDates.value)
  })

  async function fetchCalendarData() {
    if (isLoaded.value) return
    if (isLoading.value) return

    isLoading.value = true
    try {
      const res = await axios.get(`${API_BASE_URL}/api/calendar/non-trading-dates`)
      nonTradingDates.value = res.data || []
      isLoaded.value = true
    } catch (error) {
      console.error('获取非交易日数据失败:', error)
    } finally {
      isLoading.value = false
    }
  }

  function isNonTradingDate(dateStr) {
    return nonTradingDateSet.value.has(dateStr)
  }

  function reset() {
    nonTradingDates.value = []
    isLoaded.value = false
    isLoading.value = false
  }

  return {
    nonTradingDates,
    isLoading,
    isLoaded,
    nonTradingDateSet,
    fetchCalendarData,
    isNonTradingDate,
    reset
  }
})
