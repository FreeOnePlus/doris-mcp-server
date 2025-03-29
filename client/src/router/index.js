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
      component: HomeView
    },
    {
      path: '/nl2sql',
      name: 'nl2sql',
      component: NL2SQLView
    },
    {
      path: '/sql-optimize',
      name: 'sql-optimize',
      component: SQLOptimizeView
    },
    {
      path: '/llm-config',
      name: 'llm-config',
      component: LLMConfigView
    }
  ]
});

export default router; 