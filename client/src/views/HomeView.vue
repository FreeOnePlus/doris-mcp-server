<template>
  <div class="home-container">
    <div class="welcome-section">
      <h1>Apache Doris MCP 客户端</h1>
      <p class="subtitle">基于MCP协议的智能分析工具</p>
      
      <div class="features">
        <el-row :gutter="20">
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><ChatDotRound /></el-icon>
                    NL2SQL 自然语言查询
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>使用自然语言描述您的数据需求，系统将自动转换为SQL查询并执行，同时提供业务分析和可视化建议。</p>
                <el-button type="primary" @click="$router.push('/nl2sql')">开始使用</el-button>
              </div>
            </el-card>
          </el-col>
          
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><MagicStick /></el-icon>
                    SQL 智能优化分析
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>提交您的SQL查询，系统将自动分析性能瓶颈，提供优化建议，并生成改进后的SQL语句。</p>
                <el-button type="primary" @click="$router.push('/sql-optimize')">开始使用</el-button>
              </div>
            </el-card>
          </el-col>
          
          <el-col :span="8">
            <el-card class="feature-card">
              <template #header>
                <div class="card-header">
                  <h3>
                    <el-icon><Setting /></el-icon>
                    LLM 配置管理
                  </h3>
                </div>
              </template>
              <div class="card-content">
                <p>自定义各处理阶段使用的大语言模型(LLM)，为不同的任务选择合适的模型和参数配置。</p>
                <el-button type="primary" @click="$router.push('/config')">管理配置</el-button>
              </div>
            </el-card>
          </el-col>
        </el-row>
      </div>
    </div>
    
    <div class="system-status" v-if="mcpStore.isConnected">
      <h2>系统状态</h2>
      <el-descriptions border>
        <el-descriptions-item label="连接状态">
          <el-tag type="success">已连接</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="服务器地址">
          {{ mcpHost }}:{{ mcpPort }}
        </el-descriptions-item>
        <el-descriptions-item label="协议">
          {{ mcpProtocol.toUpperCase() }}
        </el-descriptions-item>
        <el-descriptions-item label="LLM提供商数量">
          {{ llmProviders.length }}
        </el-descriptions-item>
      </el-descriptions>
    </div>
    
    <div class="connection-error" v-else>
      <el-alert
        title="未连接到MCP服务器"
        type="warning"
        :closable="false"
        show-icon
      >
        <p>请检查服务器是否已启动，然后点击下方按钮尝试连接。</p>
        <el-button type="primary" :loading="mcpStore.isConnecting" @click="connect">
          {{ mcpStore.isConnecting ? '连接中...' : '连接服务器' }}
        </el-button>
      </el-alert>
    </div>
    
    <div class="server-debug" v-if="!mcpStore.isConnected">
      <el-divider content-position="center">服务器连接调试</el-divider>
      
      <div class="debug-actions">
        <el-button type="primary" @click="checkServer" :loading="isChecking">
          检查服务器状态
        </el-button>
        <el-button type="success" @click="connect" :loading="mcpStore.isConnecting">
          手动连接
        </el-button>
      </div>
      
      <div class="debug-results" v-if="checkResults.length > 0">
        <h3>服务器检查结果:</h3>
        <el-table :data="checkResults" border stripe>
          <el-table-column prop="endpoint" label="端点" />
          <el-table-column label="状态">
            <template #default="scope">
              <span v-if="scope.row.ok" class="text-success">✓ 成功 ({{ scope.row.status }})</span>
              <span v-else-if="scope.row.status" class="text-danger">✗ 失败 ({{ scope.row.status }}: {{ scope.row.statusText }})</span>
              <span v-else class="text-warning">✗ 错误: {{ scope.row.error }}</span>
            </template>
          </el-table-column>
        </el-table>
        
        <div class="debug-tips">
          <p>调试提示:</p>
          <ol>
            <li>检查MCP服务器是否已启动 (<code>python src/main.py</code>)</li>
            <li>确认服务器运行在端口 <b>{{ mcpPort }}</b> 上</li>
            <li>检查环境变量配置是否正确 (.env 文件)</li>
            <li>检查网络连接和防火墙设置</li>
            <li>尝试在MCP服务器控制台查看日志输出</li>
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

const mcpStore = useMCPStore();

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
    ElMessage.info('正在连接中，请稍候...');
    return;
  }
  
  // 如果已经连接成功，直接返回
  if (mcpStore.isConnected) {
    ElMessage.success('已经连接成功');
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
        ElMessage.error('连接操作超时，请检查服务器或重试');
      }
    }, 10000);
    
    const success = await mcpStore.connect();
    
    if (success) {
      ElMessage.success('连接成功');
      try {
        // 尝试检查服务器状态，但不要等待太久
        const checkPromise = checkServer();
        const checkTimeout = new Promise(resolve => setTimeout(resolve, 3000));
        await Promise.race([checkPromise, checkTimeout]);
      } catch (e) {
        console.warn('自动检查服务器状态失败', e);
      }
    } else {
      ElMessage.error(mcpStore.connectionError || '连接失败，请检查服务器状态');
    }
  } catch (error) {
    console.error('连接处理出错:', error);
    ElMessage.error(`连接处理出错: ${error.message || '未知错误'}`);
  } finally {
    // 确保无论如何都会重置连接状态
    isConnecting.value = false;
  }
}

// 服务器检查方法
async function checkServer() {
  if (isChecking.value) {
    ElMessage.info('检查已在进行中...');
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
          endpoint: '总体检查',
          status: 'error',
          message: '检查超时，请重试'
        });
        ElMessage.warning('服务器检查超时');
      }
    }, 20000);
    
    // 尝试连接（如果未连接）
    if (!mcpStore.isConnected) {
      try {
        // 设置短超时，避免长时间阻塞
        const connectPromise = mcpStore.connect();
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('连接超时')), 5000);
        });
        
        await Promise.race([connectPromise, timeoutPromise]);
        
        checkResults.value.push({
          endpoint: 'MCP连接',
          status: 'success',
          message: '连接成功'
        });
      } catch (error) {
        checkResults.value.push({
          endpoint: 'MCP连接',
          status: 'error',
          message: `连接失败: ${error.message}`
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
            message: `状态: ${health.status}, 版本: ${health.version}`
          });
        } catch (error) {
          checkResults.value.push({
            endpoint: 'health',
            status: 'error',
            message: `调用失败: ${error.message}`
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
            message: `服务状态: ${status.service?.status}, 版本: ${status.service?.version}`
          });
          
          // 显示LLM提供商信息
          if (status.llm && status.llm.providers) {
            checkResults.value.push({
              endpoint: 'LLM提供商',
              status: 'success',
              message: `可用提供商: ${status.llm.providers.join(', ')}`
            });
          }
        } catch (error) {
          checkResults.value.push({
            endpoint: 'status',
            status: 'error',
            message: `调用失败: ${error.message}`
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
            message: `可用工具: ${tools.length}个`
          });
        } catch (error) {
          checkResults.value.push({
            endpoint: 'Tools',
            status: 'error',
            message: `获取失败: ${error.message}`
          });
        }
      })()
    ]);
    
    clearTimeout(checkTimeout);
  } catch (error) {
    console.error('服务器检查出错:', error);
    checkResults.value.push({
      endpoint: '总体检查',
      status: 'error',
      message: `检查过程出错: ${error.message}`
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