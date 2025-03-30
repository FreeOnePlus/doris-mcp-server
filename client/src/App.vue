<template>
  <div class="app-container">
    <!-- 顶部导航 -->
    <el-menu
      :default-active="activeRoute"
      mode="horizontal"
      router
      class="main-menu"
      @select="handleSelect"
    >
      <el-menu-item index="/">
        <el-icon><HomeFilled /></el-icon>
        <span>{{ getMenuText('home') }}</span>
      </el-menu-item>
      <el-menu-item index="/nl2sql">
        <el-icon><ChatLineSquare /></el-icon>
        <span>NL2SQL</span>
      </el-menu-item>
      <el-menu-item index="/sql-optimize">
        <el-icon><MagicStick /></el-icon>
        <span>{{ getMenuText('sqlOptimize') }}</span>
      </el-menu-item>
      <el-menu-item index="/llm-config">
        <el-icon><Setting /></el-icon>
        <span>{{ getMenuText('configManagement') }}</span>
      </el-menu-item>
      
      <!-- 显示连接状态 -->
      <div class="connection-status">
        <!-- 添加语言切换器 -->
        <LanguageSwitcher class="global-lang-switcher" />
        
        <el-tooltip
          :content="mcpStore.isConnected ? getMenuText('connectedTooltip') : getMenuText('disconnectedTooltip')"
          placement="bottom"
        >
          <el-tag :type="mcpStore.isConnected ? 'success' : 'danger'" size="small">
            {{ mcpStore.isConnected ? getMenuText('connected') : getMenuText('disconnected') }}
          </el-tag>
        </el-tooltip>
      </div>
    </el-menu>
    
    <!-- 主内容区 -->
    <div class="main-content">
      <router-view :key="$route.fullPath + ($route.query._t || '')" v-slot="{ Component }">
        <component :is="Component" />
      </router-view>
    </div>
    
    <!-- 全局加载状态 -->
    <el-backtop />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useMCPStore } from './stores/mcp'
import { useLangStore } from './stores/langStore'
import { HomeFilled, ChatLineSquare, MagicStick, Setting } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import LanguageSwitcher from './components/LanguageSwitcher.vue'

const route = useRoute()
const router = useRouter()
const mcpStore = useMCPStore()
const langStore = useLangStore()

// 当前路由
const activeRoute = computed(() => route.path)

// 页面标题
const pageTitle = computed(() => {
  const currentRoute = route.name;
  if (currentRoute === 'nl2sql') {
    return langStore.currentLang === 'en' ? 'NL2SQL - Natural Language Query' : 'NL2SQL - 自然语言查询';
  }
  // 其他页面标题...
  return langStore.currentLang === 'en' ? 'Doris MCP' : 'Doris MCP 管理平台';
});

// 监听路由变化，更新页面标题
watch(() => route.name, () => {
  document.title = pageTitle.value;
}, { immediate: true });

// 监听语言变化，更新页面标题
watch(() => langStore.currentLang, () => {
  document.title = pageTitle.value;
});

// 菜单文本国际化
function getMenuText(key) {
  const menuTexts = {
    home: {
      en: 'Home',
      zh: '首页'
    },
    sqlOptimize: {
      en: 'SQL Optimization',
      zh: 'SQL优化'
    },
    configManagement: {
      en: 'Configuration',
      zh: '配置管理'
    },
    connected: {
      en: 'Connected',
      zh: '已连接'
    },
    disconnected: {
      en: 'Disconnected',
      zh: '未连接'
    },
    connectedTooltip: {
      en: 'Connected to MCP Service',
      zh: '已连接到MCP服务'
    },
    disconnectedTooltip: {
      en: 'Disconnected from MCP Service',
      zh: '未连接到MCP服务'
    }
  };

  const langCode = langStore.currentLang;
  return menuTexts[key]?.[langCode] || key;
}

// 菜单选择处理
function handleSelect(key) {
  console.log('选中菜单:', key)
  
  // 使用编程式导航，更有控制力
  try {
    if (key === route.path) {
      // 相同路由时强制刷新页面
      router.go(0)
    } else {
      // 不同路由时正常导航
      router.push(key).catch(err => {
        console.warn('路由导航被拒绝:', err)
      })
    }
  } catch (error) {
    console.error('路由导航出错:', error)
  }
}

// 监听连接状态变化
watch(() => mcpStore.isConnected, (newValue) => {
  if (newValue) {
    ElMessage.success(getMenuText('connectedTooltip'))
  } else if (!mcpStore.isConnecting) {
    ElMessage.error(getMenuText('disconnectedTooltip'))
  }
})

onMounted(() => {
  // 从本地存储加载语言设置
  langStore.initLanguage();
  
  // 初始化连接，添加超时控制
  if (!mcpStore.isConnected && !mcpStore.isConnecting) {
    // 使用短超时尝试连接，不阻塞应用启动
    setTimeout(() => {
      mcpStore.connect().catch(error => {
        // 只在第一次加载时静默处理错误，不显示错误消息
        console.warn('自动初始连接失败:', error)
      })
    }, 100)
    
    // 设置安全超时，确保不会阻塞应用
    setTimeout(() => {
      if (mcpStore.isConnecting) {
        console.warn('初始连接超时，重置连接状态')
        mcpStore.isConnecting = false
      }
    }, 5000)
  }
})
</script>

<style lang="scss">
// 全局样式
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: 'Helvetica Neue', Helvetica, 'PingFang SC', 'Hiragino Sans GB',
    'Microsoft YaHei', '微软雅黑', Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  background-color: #f5f7fa;
  color: #333;
}

// 应用容器
.app-container {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  
  // 导航菜单 - 固定在顶部
  .main-menu {
    padding: 0 20px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 999;
    background-color: white; // 确保背景色，避免内容透过
    
    .connection-status {
      position: absolute;
      right: 20px;
      top: 50%;
      transform: translateY(-50%);
      display: flex;
      align-items: center;
      gap: 15px;
      
      .global-lang-switcher {
        // 样式调整
        margin-right: 5px;
      }
    }
  }
  
  // 主内容区 - 添加顶部边距，避免被固定菜单遮挡
  .main-content {
    flex: 1;
    padding: 20px;
    max-width: 100%;
    overflow-x: hidden;
    margin-top: 60px; // 为固定的导航菜单留出空间，与el-menu的高度一致
  }
}

// 自定义滚动条
::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-track {
  background: #f1f1f1;
  border-radius: 4px;
}

::-webkit-scrollbar-thumb {
  background: #ccc;
  border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
  background: #aaa;
}
</style> 