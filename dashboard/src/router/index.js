import { createRouter, createWebHistory } from 'vue-router'
import AuctionExpect from '../views/AuctionExpect.vue'

const routes = [
  {
    path: '/',
    redirect: '/auction-expect'
  },
  {
    path: '/auction-expect',
    name: 'AuctionExpect',
    component: AuctionExpect
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router