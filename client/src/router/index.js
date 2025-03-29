import { createRouter, createWebHistory } from 'vue-router';
import HomeView from '../views/HomeView.vue';
import NL2SQLView from '../views/NL2SQLView.vue';
import SQLOptimizeView from '../views/SQLOptimizeView.vue';
import LLMConfigView from '../views/LLMConfigView.vue';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'home',
      component: HomeView,
      meta: { keepAlive: false }
    },
    {
      path: '/nl2sql',
      name: 'nl2sql',
      component: NL2SQLView,
      meta: { keepAlive: false }
    },
    {
      path: '/sql-optimize',
      name: 'sql-optimize',
      component: SQLOptimizeView,
      meta: { keepAlive: false }
    },
    {
      path: '/llm-config',
      name: 'llm-config',
      component: LLMConfigView,
      meta: { keepAlive: false }
    }
  ],
  scrollBehavior(to, from, savedPosition) {
    if (savedPosition) {
      return savedPosition
    } else {
      return { top: 0 }
    }
  }
});

router.beforeEach((to, from, next) => {
  if (to.path === from.path) {
    to.query = { ...to.query, _t: Date.now() }
  }
  next()
})

export default router; 