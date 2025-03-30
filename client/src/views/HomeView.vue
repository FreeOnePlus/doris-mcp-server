<template>
  <div class="home-container">
    <div class="welcome-section">
      <h1>{{ t('home.title') }}</h1>
      <p class="subtitle">{{ t('home.subtitle') }}</p>
      
      <div class="features">
        <el-row :gutter="20">
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><ChatDotRound /></el-icon>
                    {{ t('home.features.nl2sql.title') }}
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>{{ t('home.features.nl2sql.description') }}</p>
                <el-button type="primary" @click="$router.push('/nl2sql')">{{ t('home.features.nl2sql.button') }}</el-button>
              </div>
            </el-card>
          </el-col>
          
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><MagicStick /></el-icon>
                    {{ t('home.features.sqlOptimize.title') }}
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>{{ t('home.features.sqlOptimize.description') }}</p>
                <el-button type="primary" @click="$router.push('/sql-optimize')">{{ t('home.features.sqlOptimize.button') }}</el-button>
              </div>
            </el-card>
          </el-col>
          
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><Setting /></el-icon>
                    {{ t('home.features.llmConfig.title') }}
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>{{ t('home.features.llmConfig.description') }}</p>
                <el-button type="primary" @click="$router.push('/llm-config')">{{ t('home.features.llmConfig.button') }}</el-button>
              </div>
            </el-card>
          </el-col>
        </el-row>
      </div>
    </div>
    
    <div class="system-status" v-if="mcpStore.isConnected">
      <h2>{{ t('home.systemStatus.title') }}</h2>
      <el-descriptions border>
        <el-descriptions-item :label="t('home.systemStatus.connectionStatus')">
          <el-tag type="success">{{ t('common.connected') }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="t('home.systemStatus.serverAddress')">
          {{ mcpHost }}:{{ mcpPort }}
        </el-descriptions-item>
        <el-descriptions-item :label="t('home.systemStatus.protocol')">
          {{ mcpProtocol.toUpperCase() }}
        </el-descriptions-item>
        <el-descriptions-item :label="t('home.systemStatus.llmProviderCount')">
          {{ llmProviders.length }}
        </el-descriptions-item>
      </el-descriptions>
    </div>
    
    <div class="connection-error" v-else>
      <el-alert
        :title="t('home.connectionError.title')"
        type="warning"
        :closable="false"
        show-icon
      >
        <p>{{ t('home.connectionError.description') }}</p>
        <el-button type="primary" :loading="mcpStore.isConnecting" @click="connect">
          {{ mcpStore.isConnecting ? t('home.connectionError.connecting') : t('home.connectionError.button') }}
        </el-button>
      </el-alert>
    </div>
    
    <div class="server-debug" v-if="!mcpStore.isConnected">
      <el-divider content-position="center">{{ t('home.serverDebug.title') }}</el-divider>
      
      <div class="debug-actions">
        <el-button type="primary" @click="checkServer" :loading="isChecking">
          {{ t('home.serverDebug.checkButton') }}
        </el-button>
        <el-button type="success" @click="connect" :loading="mcpStore.isConnecting">
          {{ t('home.serverDebug.connectButton') }}
        </el-button>
      </div>
      
      <div class="debug-results" v-if="checkResults.length > 0">
        <h3>{{ t('home.serverDebug.results') }}</h3>
        <el-table :data="checkResults" border stripe>
          <el-table-column prop="endpoint" label="端点" />
          <el-table-column label="状态">
            <template #default="scope">
              <span v-if="scope.row.ok" class="text-success">{{ t('home.serverDebug.success') }} ({{ scope.row.status }})</span>
              <span v-else-if="scope.row.status" class="text-danger">{{ t('home.serverDebug.failed') }} ({{ scope.row.status }}: {{ scope.row.statusText }})</span>
              <span v-else class="text-warning">{{ t('home.serverDebug.error') }} {{ scope.row.error }}</span>
            </template>
          </el-table-column>
        </el-table>
        
        <div class="debug-tips">
          <p>{{ t('home.serverDebug.tips.title') }}</p>
          <ol>
            <li>{{ t('home.serverDebug.tips.checkServer') }} (<code>python src/main.py</code>)</li>
            <li>{{ t('home.serverDebug.tips.confirmPort') }} <b>{{ mcpPort }}</b></li>
            <li>{{ t('home.serverDebug.tips.checkEnv') }}</li>
            <li>{{ t('home.serverDebug.tips.checkNetwork') }}</li>
            <li>{{ t('home.serverDebug.tips.checkLogs') }}</li>
          </ol>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { useMCPStore } from '../stores/mcp';
import { ElMessage } from 'element-plus';
import { ref, watch } from 'vue';
// 导入Element Plus图标
import { ChatDotRound, MagicStick, Setting } from '@element-plus/icons-vue';
// 导入国际化函数
import { useI18n } from '../i18n';

const mcpStore = useMCPStore();
const { t, currentLang } = useI18n();

// 获取MCP服务器配置
const mcpHost = import.meta.env.VITE_MCP_HOST || 'localhost';
const mcpPort = import.meta.env.VITE_MCP_PORT || '3000';
const mcpProtocol = import.meta.env.VITE_MCP_PROTOCOL || 'ws';

// LLM提供商列表 
const llmProviders = ref([]);

// 在成功连接后获取LLM提供商列表
async function fetchLLMProviders() {
  if (mcpStore.isConnected) {
    try {
      const result = await mcpStore.listLLMProviders();
      if (result && result.providers) {
        llmProviders.value = result.providers;
      }
    } catch (error) {
      console.warn('获取LLM提供商列表失败:', error);
      llmProviders.value = []; // 确保始终有一个空数组
    }
  }
}

// 监听连接状态变化，连接成功后获取LLM提供商
watch(() => mcpStore.isConnected, async (newValue) => {
  if (newValue) {
    await fetchLLMProviders();
  }
}, { immediate: true });

// 手动连接方法
async function connect() {
  // 如果已经在连接中，则防止重复点击
  if (isConnecting.value) {
    ElMessage.info(currentLang.value === 'en' ? 'Connecting, please wait...' : '正在连接中，请稍候...');
    return;
  }
  
  // 如果已经连接成功，直接返回
  if (mcpStore.isConnected) {
    ElMessage.success(currentLang.value === 'en' ? 'Already connected' : '已经连接成功');
    return;
  }
  
  // 设置本地连接状态
  isConnecting.value = true;
  
  try {
    console.log('开始尝试连接MCP服务器');
    
    // 使用简单的连接调用，并设置最长10秒超时
    setTimeout(() => {
      if (isConnecting.value) {
        isConnecting.value = false;
        ElMessage.error(currentLang.value === 'en' ? 'Connection operation timed out, please check the server or try again' : '连接操作超时，请检查服务器或重试');
      }
    }, 10000);
    
    const success = await mcpStore.connect();
    
    if (success) {
      ElMessage.success(currentLang.value === 'en' ? 'Connection successful' : '连接成功');
      try {
        // 尝试检查服务器状态，但不要等待太久
        const checkPromise = checkServer();
        const checkTimeout = new Promise(resolve => setTimeout(resolve, 3000));
        await Promise.race([checkPromise, checkTimeout]);
      } catch (e) {
        console.warn('自动检查服务器状态失败', e);
      }
    } else {
      ElMessage.error(mcpStore.connectionError || (currentLang.value === 'en' ? 'Connection failed, please check the server status' : '连接失败，请检查服务器状态'));
    }
  } catch (error) {
    console.error('连接处理出错:', error);
    ElMessage.error(currentLang.value === 'en' 
      ? `Connection processing error: ${error.message || 'Unknown error'}` 
      : `连接处理出错: ${error.message || '未知错误'}`);
  } finally {
    // 确保无论如何都会重置连接状态
    isConnecting.value = false;
  }
}

// 服务器检查方法
async function checkServer() {
  if (isChecking.value) {
    ElMessage.info(currentLang.value === 'en' ? 'Check already in progress...' : '检查已在进行中...');
    return;
  }
  
  isChecking.value = true;
  checkResults.value = [];
  
  try {
    // 添加总体检查超时
    const checkTimeout = setTimeout(() => {
      if (isChecking.value) {
        isChecking.value = false;
        checkResults.value.push({
          endpoint: currentLang.value === 'en' ? 'Overall Check' : '总体检查',
          status: 'error',
          message: currentLang.value === 'en' ? 'Check timed out, please try again' : '检查超时，请重试'
        });
        ElMessage.warning(currentLang.value === 'en' ? 'Server check timed out' : '服务器检查超时');
      }
    }, 20000);
    
    // 尝试连接（如果未连接）
    if (!mcpStore.isConnected) {
      try {
        // 设置短超时，避免长时间阻塞
        const connectPromise = mcpStore.connect();
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error(currentLang.value === 'en' ? 'Connection timed out' : '连接超时')), 5000);
        });
        
        await Promise.race([connectPromise, timeoutPromise]);
        
        checkResults.value.push({
          endpoint: 'MCP连接',
          status: 'success',
          message: currentLang.value === 'en' ? 'Connection successful' : '连接成功'
        });
      } catch (error) {
        checkResults.value.push({
          endpoint: 'MCP连接',
          status: 'error',
          message: `${currentLang.value === 'en' ? 'Connection failed' : '连接失败'}: ${error.message}`
        });
        clearTimeout(checkTimeout);
        isChecking.value = false;
        return;
      }
    }
    
    // 并行执行所有检查以提高效率
    await Promise.all([
      // 检查健康状态
      (async () => {
        try {
          const health = await mcpStore.checkHealth();
          checkResults.value.push({
            endpoint: 'health',
            status: 'success',
            message: `${currentLang.value === 'en' ? 'Status' : '状态'}: ${health.status}, ${currentLang.value === 'en' ? 'Version' : '版本'}: ${health.version}`
          });
        } catch (error) {
          checkResults.value.push({
            endpoint: 'health',
            status: 'error',
            message: `${currentLang.value === 'en' ? 'Call failed' : '调用失败'}: ${error.message}`
          });
        }
      })(),
      
      // 检查详细状态
      (async () => {
        try {
          const status = await mcpStore.getStatus();
          checkResults.value.push({
            endpoint: 'status',
            status: 'success',
            message: `${currentLang.value === 'en' ? 'Service status' : '服务状态'}: ${status.service?.status}, ${currentLang.value === 'en' ? 'Version' : '版本'}: ${status.service?.version}`
          });
          
          // 显示LLM提供商信息
          if (status.llm && status.llm.providers) {
            checkResults.value.push({
              endpoint: currentLang.value === 'en' ? 'LLM Providers' : 'LLM提供商',
              status: 'success',
              message: `${currentLang.value === 'en' ? 'Available providers' : '可用提供商'}: ${status.llm.providers.join(', ')}`
            });
          }
        } catch (error) {
          checkResults.value.push({
            endpoint: 'status',
            status: 'error',
            message: `${currentLang.value === 'en' ? 'Call failed' : '调用失败'}: ${error.message}`
          });
        }
      })(),
      
      // 获取工具列表
      (async () => {
        try {
          const tools = await mcpStore.fetchTools();
          checkResults.value.push({
            endpoint: 'Tools',
            status: 'success',
            message: `${currentLang.value === 'en' ? 'Available tools' : '可用工具'}: ${tools.length}${currentLang.value === 'en' ? ' items' : '个'}`
          });
        } catch (error) {
          checkResults.value.push({
            endpoint: 'Tools',
            status: 'error',
            message: `${currentLang.value === 'en' ? 'Acquisition failed' : '获取失败'}: ${error.message}`
          });
        }
      })()
    ]);
    
    clearTimeout(checkTimeout);
  } catch (error) {
    console.error('服务器检查出错:', error);
    checkResults.value.push({
      endpoint: currentLang.value === 'en' ? 'Overall Check' : '总体检查',
      status: 'error',
      message: `${currentLang.value === 'en' ? 'Error in inspection process' : '检查过程出错'}: ${error.message}`
    });
  } finally {
    isChecking.value = false;
  }
}

// 添加监控状态的响应式变量
const isChecking = ref(false);
const isConnecting = ref(false);  // 添加本地连接状态
const checkResults = ref([]);
</script>

<style lang="scss" scoped>
.home-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
  
  .welcome-section {
    text-align: center;
    margin-bottom: 40px;
    
    h1 {
      font-size: 2.5rem;
      margin-bottom: 10px;
      color: #409EFF;
    }
    
    .subtitle {
      font-size: 1.2rem;
      color: #606266;
      margin-bottom: 30px;
    }
  }
  
  .features {
    margin-top: 40px;
    
    .feature-card {
      height: 100%;
      transition: transform 0.3s;
      
      &:hover {
        transform: translateY(-5px);
      }
      
      .card-header {
        h3 {
          margin: 0;
          display: flex;
          align-items: center;
          
          .el-icon {
            margin-right: 8px;
          }
        }
      }
      
      .card-content {
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        height: 150px;
        
        p {
          margin-bottom: 20px;
        }
      }
    }
  }
  
  .system-status,
  .connection-error {
    margin-top: 40px;
  }
  
  .server-debug {
    margin-top: 30px;
    padding: 20px;
    background-color: #f8f9fa;
    border-radius: 8px;
    
    .debug-actions {
      display: flex;
      gap: 10px;
      margin-bottom: 20px;
    }
    
    .debug-results {
      margin-top: 20px;
    }
    
    .debug-tips {
      margin-top: 20px;
      background-color: #fff;
      padding: 15px;
      border-left: 4px solid #f39c12;
      
      p {
        font-weight: bold;
        margin-top: 0;
      }
      
      ol {
        padding-left: 20px;
      }
      
      code {
        background-color: #f1f1f1;
        padding: 2px 5px;
        border-radius: 3px;
      }
    }
  }
}
</style> 