<template>
  <div class="nl2sql-view">
    <el-card class="chat-container">
      <template #header>
        <div class="header">
          <h2>NL2SQL 自然语言查询</h2>
          <div class="header-actions">
            <el-switch
              v-model="debugMode"
              class="debug-switch"
              active-text="调试模式"
              inactive-text=""
              size="small"
            />
            <el-tag 
              :type="mcpStore.isConnected ? 'success' : 'danger'" 
              size="small" 
              class="connection-status"
            >
              {{ mcpStore.isConnected ? '已连接' : '未连接' }}
              <el-button 
                v-if="!mcpStore.isConnected && !mcpStore.isConnecting" 
                type="primary" 
                size="small" 
                circle 
                @click="reconnect" 
                title="重新连接"
                class="reconnect-btn"
              >
                <el-icon><Refresh /></el-icon>
              </el-button>
              <el-icon v-else-if="mcpStore.isConnecting" class="is-loading"><Loading /></el-icon>
            </el-tag>
            <el-button type="primary" plain @click="clearChat" :disabled="chatMessages.length === 0">
              清空对话
            </el-button>
          </div>
        </div>
      </template>
      
      <!-- 调试信息显示 -->
      <el-collapse v-if="debugMode">
        <el-collapse-item title="调试信息">
          <div class="debug-info">
            <p><strong>当前阶段:</strong> {{ mcpStore.currentStage }} ({{ mcpStore.getStageName(mcpStore.currentStage) }})</p>
            <p><strong>进度:</strong> {{ mcpStore.queryProgress }}%</p>
            <p><strong>思考状态:</strong> {{ mcpStore.isThinking ? '思考中' : '空闲' }}</p>
            <p><strong>阶段历史:</strong></p>
            <ul>
              <li v-for="(stage, index) in mcpStore.stageHistory" :key="index">
                {{ stage }} ({{ mcpStore.getStageName(stage) }})
              </li>
            </ul>
          </div>
        </el-collapse-item>
      </el-collapse>
      
      <!-- 连接错误提示 -->
      <el-alert
        v-if="!mcpStore.isConnected && !mcpStore.isConnecting && mcpStore.connectionError"
        type="error"
        :title="mcpStore.connectionError"
        :closable="true"
        show-icon
        style="margin-bottom: 15px;"
      >
        <template #default>
          <p>建议检查：</p>
          <ol>
            <li>服务器是否已启动</li>
            <li>WebSocket URL配置是否正确</li>
            <li>网络连接是否正常</li>
          </ol>
          <el-button type="primary" size="small" @click="reconnect">重新连接</el-button>
        </template>
      </el-alert>
      
      <div class="chat-messages" ref="messagesContainer">
        <div v-if="chatMessages.length === 0" class="empty-chat">
          <el-empty description="开始对话，输入您的自然语言查询">
            <el-button type="primary" @click="focusInput">开始查询</el-button>
          </el-empty>
        </div>
        
        <template v-for="(message, index) in chatMessages" :key="index">
          <!-- 用户消息 -->
          <div v-if="message.role === 'user'" class="message user-message">
            <div class="avatar">
              <el-avatar :icon="UserFilled" />
            </div>
            <div class="content">
              {{ message.content }}
            </div>
          </div>
          
          <!-- 系统消息 -->
          <div v-else class="message system-message">
            <div class="avatar">
              <el-avatar :src="logoUrl" />
            </div>
            <div class="content">
              <!-- 思考过程 -->
              <div v-if="message.thinking_process" class="thinking-section">
                <div class="section-header">
                  <span>思考过程</span>
                  <el-button type="text" @click="message.thinkingCollapsed = !message.thinkingCollapsed">
                    {{ message.thinkingCollapsed ? '展开' : '折叠' }}
                  </el-button>
                </div>
                <div v-show="!message.thinkingCollapsed" class="thinking-content">
                  <pre>{{ message.thinking_process }}</pre>
                </div>
              </div>
              
              <!-- SQL查询 -->
              <div v-if="message.sql" class="sql-section">
                <div class="section-header">
                  <span>SQL查询</span>
                  <el-button size="small" @click="copySql(message.sql)">复制</el-button>
                </div>
                <pre class="sql-code">{{ message.sql }}</pre>
              </div>
              
              <!-- 查询结果 -->
              <div v-if="message.result && message.result.length > 0" class="result-section">
                <div class="section-header">
                  <span>查询结果 ({{ message.result.length }}条记录)</span>
                </div>
                <el-table :data="message.result.slice(0, 5)" style="width: 100%" border size="small">
                  <el-table-column
                    v-for="column in message.column_names"
                    :key="column"
                    :prop="column"
                    :label="column"
                  />
                </el-table>
                <div v-if="message.result.length > 5" class="more-results">
                  <el-button type="text" @click="showFullResults(message.result, message.column_names)">
                    查看全部 {{ message.result.length }} 条记录
                  </el-button>
                </div>
              </div>
              
              <!-- 业务分析 -->
              <div v-if="message.business_analysis" class="analysis-section">
                <div class="section-header">
                  <span>业务分析</span>
                </div>
                <div v-html="formatText(message.business_analysis.business_analysis)" class="analysis-text"></div>
                
                <!-- 可视化图表 -->
                <div v-if="message.business_analysis.visualization" class="visualization">
                  <h4>{{ message.business_analysis.visualization.title }}</h4>
                  <div :id="`chart-${index}`" class="chart-container"></div>
                  <p class="chart-desc">{{ message.business_analysis.visualization.description }}</p>
                </div>
                
                <!-- 趋势和建议 -->
                <div v-if="message.business_analysis.trends && message.business_analysis.trends.length" class="trends">
                  <h4>主要趋势</h4>
                  <ul>
                    <li v-for="(trend, i) in message.business_analysis.trends" :key="i">{{ trend }}</li>
                  </ul>
                </div>
                
                <div v-if="message.business_analysis.recommendations && message.business_analysis.recommendations.length" class="recommendations">
                  <h4>业务建议</h4>
                  <ul>
                    <li v-for="(rec, i) in message.business_analysis.recommendations" :key="i">{{ rec }}</li>
                  </ul>
                </div>
              </div>
              
              <!-- 错误信息 -->
              <div v-if="message.error" class="error-section">
                <el-alert
                  type="error"
                  :title="message.error.message || '查询执行出错'"
                  :closable="false"
                  show-icon
                />
                <div v-if="message.error.details" class="error-details">
                  <p>详细信息：{{ message.error.details }}</p>
                </div>
                
                <!-- 服务器实现问题诊断 -->
                <div v-if="message.error.message && message.error.message.includes('服务器未正确处理')" class="server-diagnosis">
                  <h4>服务器问题诊断</h4>
                  <p>服务器返回了无效的响应格式，问题可能是：</p>
                  <ul>
                    <li>服务器端 <code>nl2sql_query</code> 工具没有完全实现</li>
                    <li>NL2SQL处理服务没有正确启动或配置</li>
                    <li>服务器端的数据库连接问题</li>
                  </ul>
                  <p>解决建议：</p>
                  <ul>
                    <li>检查服务器日志中的错误信息</li>
                    <li>确认服务器的 <code>nl2sql_processor.py</code> 模块正确实现并返回标准格式</li>
                    <li>检查 <code>nl2sql_service.py</code> 中的 <code>process_query</code> 方法是否正确处理并返回结果</li>
                  </ul>
                </div>
              </div>
              
              <!-- 普通消息 -->
              <div v-if="!message.sql && !message.result && !message.error && !message.business_analysis">
                {{ message.content }}
              </div>
            </div>
          </div>
        </template>
        
        <!-- 流式思考过程显示 -->
        <div v-if="isLoading" class="message system-message">
          <div class="avatar">
            <el-avatar :src="logoUrl" />
          </div>
          <div class="content">
            <div class="thinking-section" v-if="mcpStore.currentStage && mcpStore.currentStage !== 'waiting'">
              <div class="thinking-header">
                <span class="thinking-title">思考过程</span>
                <span class="thinking-status">
                  - {{ getStageName(mcpStore.currentStage) }} ({{ mcpStore.queryProgress }}%)
                  <span v-if="mcpStore.is_processing" class="pulse-dot"></span>
                </span>
              </div>
              
              <div class="thinking-process">
                <div class="progress-container">
                  <div class="progress-markers">
                    <div v-for="(marker, index) in dynamicStageMarkers" :key="`marker-${index}`"
                         :class="['stage-marker', { 
                           'completed': isStageCompleted(marker.value), 
                           'current': mapStageToDisplay(mcpStore.currentStage) === marker.value 
                         }]"
                         :title="marker.label">
                      <span class="marker-dot"></span>
                      <span class="marker-label">{{ marker.label }}</span>
                    </div>
                  </div>
                  <div class="progress-bar">
                    <div class="progress-filled" :style="{ width: `${mcpStore.queryProgress}%` }" :key="`progress-${mcpStore.queryProgress}`"></div>
                  </div>
                </div>
                
                <div class="thinking-text">
                  <div v-if="!mcpStore.currentStage || mcpStore.currentStage === 'waiting'" class="no-thinking-yet">
                    正在分析您的查询...
                  </div>
                  <div v-else>
                    <p>当前阶段: {{ getStageName(mcpStore.currentStage) || '处理中' }}</p>
                    <p>处理状态: {{ mcpStore.is_processing ? '处理中' : '空闲' }}</p>
                    <p>进度: {{ mcpStore.queryProgress }}%</p>
                    <p v-if="mcpStore.current_query">查询: {{ mcpStore.current_query }}</p>
                    <p>历史阶段: {{ mcpStore.stageHistory.map(stage => getStageName(stage)).join(' → ') }}</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      <div class="chat-input">
        <el-input
          v-model="inputMessage"
          type="textarea"
          :rows="3"
          placeholder="输入您的自然语言查询，例如：'2023年第一季度销售额最高的三个产品是什么？'"
          :disabled="isLoading || !mcpStore.available"
          @keydown.ctrl.enter="sendMessage"
          ref="inputRef"
        />
        <el-button 
          type="primary"
          :disabled="!inputMessage.trim() || isLoading || !mcpStore.available"
          @click="sendMessage"
        >
          发送
        </el-button>
      </div>
    </el-card>
    
    <!-- 全屏结果对话框 -->
    <el-dialog v-model="fullResultsVisible" title="完整查询结果" width="80%">
      <el-table :data="fullResultsData" style="width: 100%" border height="500px">
        <el-table-column
          v-for="column in fullResultsColumns"
          :key="column"
          :prop="column"
          :label="column"
        />
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick, watch, computed, onUnmounted } from 'vue';
import { useMCPStore } from '../stores/mcp';
import { ElMessage } from 'element-plus';
import * as echarts from 'echarts';
import { UserFilled, Refresh, Loading } from '@element-plus/icons-vue';
import logoUrl from '../assets/logo.svg';

const mcpStore = useMCPStore();
const chatMessages = ref([]);
const inputMessage = ref('');
const isLoading = ref(false);
const messagesContainer = ref(null);
const inputRef = ref(null);
const thinkingContentRef = ref(null);
const debugMode = ref(true);
const showDebug = ref(false);
const lastStatusUpdateTime = ref('无');
let typingTimer = null;

// 阶段标记定义 - 移除不需要的阶段并修复重复
const dynamicStageMarkers = computed(() => [
  // 移除等待输入和开始阶段
  { value: 'analyzing', label: '分析' },
  { value: 'similar_example', label: '示例分析' },
  { value: 'business_metadata', label: '业务分析' },
  // 移除模式推理阶段
  // 保留一个SQL生成阶段，将两个变体都映射到同一个标记
  { value: 'generating', label: 'SQL生成' },
  { value: 'executing', label: 'SQL执行' }
]);

// 添加stage映射函数，处理可能的变体
function mapStageToDisplay(stage) {
  // 将生成SQL的不同变体映射到"generating"
  if (stage === 'sql_generation' || stage === 'generate_sql') {
    return 'generating';
  }
  // 将执行SQL的变体映射到"executing"
  if (stage === 'execute_sql') {
    return 'executing';
  }
  return stage;
}

// 修改isStageCompleted函数以处理阶段变体
function isStageCompleted(stage) {
  // 标准变体检查
  if (mcpStore.stageHistory.includes(stage)) {
    return true;
  }
  
  // 检查映射变体
  if (stage === 'generating') {
    return mcpStore.stageHistory.some(s => 
      s === 'generating' || s === 'sql_generation' || s === 'generate_sql'
    );
  }
  
  if (stage === 'executing') {
    return mcpStore.stageHistory.some(s => 
      s === 'executing' || s === 'execute_sql'
    );
  }
  
  return false;
}

// 添加状态更新计数器，用于强制重新渲染
const statusUpdateCount = ref(0);

// 添加强制刷新状态的方法
async function forceRefreshStatus() {
  try {
    await mcpStore.getNl2sqlStatus();
    lastStatusUpdateTime.value = new Date().toLocaleTimeString();
  } catch (error) {
    console.error('状态刷新失败:', error);
  }
}

// 根据预定义顺序获取阶段索引
function getStageIndex(stageId) {
  // 定义阶段的顺序
  const stageOrder = [
    'start', 
    'analyzing', 
    'similar_example', 
    'business_metadata', 
    'generating', 
    'executing', 
    'complete'
  ];
  
  // 找到阶段索引，如果不存在则返回-1
  const index = stageOrder.indexOf(stageId);
  console.log(`阶段[${stageId}]的索引为:`, index);
  return index;
}

// 简化监听，只在状态变化时更新计数器
watch([
  () => mcpStore.currentStage, 
  () => mcpStore.queryProgress
], () => {
  console.log(`状态已更新: 阶段=${mcpStore.currentStage}, 进度=${mcpStore.queryProgress}%`);
  // 增加计数器触发重新渲染
  statusUpdateCount.value++;
  // 滚动到底部
  nextTick(() => {
    scrollToBottom();
  });
}, { deep: true });

// 全屏结果对话框数据
const fullResultsVisible = ref(false);
const fullResultsData = ref([]);
const fullResultsColumns = ref([]);

// 监听消息变化，自动滚动到底部并渲染图表
watch(chatMessages, () => {
  nextTick(() => {
    scrollToBottom();
    renderCharts();
  });
}, { deep: true });

// 监听思考过程更新，自动滚动到底部并应用动态打字效果
let lastThinkingContent = '';
watch(() => mcpStore.thinkingProcess, (newValue) => {
  if (!newValue || lastThinkingContent === newValue) return;
  
  // 如果是新的内容，截取只显示新增的部分
  const newContent = newValue.substring(lastThinkingContent.length);
  if (newContent.trim()) {
    typeThinkingText(newContent);
  }
  
  lastThinkingContent = newValue;
  
  nextTick(() => {
    scrollToBottom();
  });
});

// 监听状态更新，确保UI刷新
watch(() => [mcpStore.currentStage, mcpStore.queryProgress, mcpStore.isThinking], ([newStage, newProgress, newThinking]) => {
  console.log(`状态更新: 阶段=${newStage}, 进度=${newProgress}%, 思考=${newThinking}`);
  console.log(`可用阶段标记:`, dynamicStageMarkers.value.map(m => m.id));
  
  // 强制刷新DOM
  nextTick(() => {
    // 触发DOM重新计算
    scrollToBottom();
    
    // 确保进度条和标记更新
    const progressContainer = document.querySelector('.progress-container');
    if (progressContainer) {
      // 轻微触发回流以强制更新渲染
      progressContainer.style.display = 'none';
      setTimeout(() => {
        progressContainer.style.display = '';
      }, 0);
    }
    
    // 强制检查所有标记状态
    dynamicStageMarkers.value.forEach(marker => {
      console.log(`检查标记[${marker.id}]: 当前阶段=${mcpStore.currentStage}, 完成状态=${isStageCompleted(marker.id)}`);
    });
  });
}, { deep: true });

// 监听加载状态变化，确保在开始加载时获取最新状态
watch(isLoading, (newVal) => {
  if (newVal) {
    // 如果开始加载，立即获取一次状态
    mcpStore.getNl2sqlStatus().catch(err => {
      console.warn('加载状态变化时获取状态出错，但不影响流程:', err);
    });
  }
});

// 监听阶段历史更新
watch(() => mcpStore.stageHistory, (newHistory) => {
  console.log('阶段历史已更新:', newHistory);
  // 强制重新计算DOM
  nextTick(() => {
    scrollToBottom();
  });
}, { deep: true });

// 监听阶段变化，强制UI更新
watch(() => mcpStore.currentStage, (newStage, oldStage) => {
  console.log(`当前阶段变化: ${oldStage} -> ${newStage}`);
  // 强制更新一些DOM元素以确保渲染正确
  nextTick(() => {
    const stageMarkers = document.querySelectorAll('.stage-marker');
    stageMarkers.forEach(el => {
      // 获取标记ID
      const markerId = el.getAttribute('data-id');
      console.log(`更新标记[${markerId}]的显示状态: 当前阶段=${newStage}, 完成=${isStageCompleted(markerId)}`);
      
      // 更新类以反映当前状态
      el.classList.toggle('active', markerId === newStage);
      el.classList.toggle('completed', isStageCompleted(markerId));
    });
  });
}, { immediate: true });

// 发送消息
async function sendMessage() {
  if (!inputMessage.value.trim() || isLoading.value || !mcpStore.available) return;
  
  // 添加用户消息
  const query = inputMessage.value.trim();
  chatMessages.value.push({
    role: 'user',
    content: query
  });
  
  // 清空输入框
  inputMessage.value = '';
  
  // 显示加载状态
  isLoading.value = true;
  // 设置初始状态
  mcpStore.setThinkingState(true);
  mcpStore.setCurrentStage('start');
  mcpStore.setProgress(5);
  statusUpdateCount.value++; // 增加计数器触发重新渲染
  scrollToBottom();
  
  try {
    // 确保连接状态
    if (!mcpStore.isConnected) {
      await mcpStore.connect();
    }
    
    // 立即获取一次状态
    await mcpStore.getNl2sqlStatus();
    statusUpdateCount.value++; // 再次增加计数器
    
    // 开始状态轮询
    mcpStore.startStatusPolling();
    
    // 使用流式查询
    await new Promise((resolve, reject) => {
      mcpStore.streamNl2sqlQuery(query, {
        onThinking: (data) => {
          console.log('思考中:', data);
          // 立即获取最新状态
          mcpStore.getNl2sqlStatus().then(() => {
            statusUpdateCount.value++; // 增加计数器
            scrollToBottom();
          });
        },
        
        onProgress: (data) => {
          console.log('进度更新:', data);
          // 立即获取最新状态
          mcpStore.getNl2sqlStatus().then(() => {
            statusUpdateCount.value++; // 增加计数器
            scrollToBottom();
          });
        },
        
        onFinal: (result) => {
          // 处理最终结果
          console.log('最终结果:', result);
          
          // 停止状态轮询
          mcpStore.stopStatusPolling();
          
          // 添加思考过程折叠状态
          const thinkingLength = result.thinking_process ? result.thinking_process.length : 0;
          const shouldCollapse = thinkingLength > 500;
          
          // 添加系统回复
          chatMessages.value.push({
            role: 'system',
            content: result.message || '查询处理完成',
            sql: result.sql,
            result: result.result || [],
            column_names: result.column_names || [],
            business_analysis: result.business_analysis,
            error: result.error,
            thinking_process: result.thinking_process,
            thinkingCollapsed: shouldCollapse
          });
          
          resolve();
        },
        
        onError: (error) => {
          console.error('查询出错:', error);
          
          // 停止状态轮询
          mcpStore.stopStatusPolling();
          
          // 添加思考过程折叠状态
          const thinkingLength = error.thinking_process ? error.thinking_process.length : 0;
          const shouldCollapse = thinkingLength > 500;
          
          // 添加系统错误回复
          chatMessages.value.push({
            role: 'system',
            content: '处理查询时出错',
            error: error.error,
            thinking_process: error.thinking_process,
            thinkingCollapsed: shouldCollapse
          });
          
          resolve(); // 解析Promise，不会中断UI操作
        }
      });
    });
  } catch (error) {
    console.error('查询处理出错:', error);
    
    // 停止状态轮询
    mcpStore.stopStatusPolling();
    
    // 处理错误
    chatMessages.value.push({
      role: 'system',
      content: '处理查询时出错',
      error: { message: error.message || '未知错误，请检查网络连接' }
    });
    
    ElMessage.error(`查询处理失败: ${error.message || '未知错误'}`);
  } finally {
    isLoading.value = false;
    scrollToBottom();
  }
}

// 滚动到底部
function scrollToBottom() {
  nextTick(() => {
    if (messagesContainer.value) {
      messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight;
    }
  });
}

// 复制SQL
function copySql(sql) {
  navigator.clipboard.writeText(sql)
    .then(() => ElMessage.success('SQL已复制到剪贴板'))
    .catch(() => ElMessage.error('复制失败'));
}

// 显示完整结果
function showFullResults(data, columns) {
  fullResultsData.value = data;
  fullResultsColumns.value = columns;
  fullResultsVisible.value = true;
}

// 清空对话
function clearChat() {
  chatMessages.value = [];
}

// 聚焦输入框
function focusInput() {
  nextTick(() => {
    inputRef.value?.focus();
  });
}

// 格式化文本（处理换行等）
function formatText(text) {
  if (!text) return '';
  return text.replace(/\n/g, '<br>');
}

// 渲染图表
function renderCharts() {
  nextTick(() => {
    chatMessages.value.forEach((message, index) => {
      if (message.role === 'system' && 
          message.business_analysis && 
          message.business_analysis.visualization &&
          message.result) {
        
        const chartContainer = document.getElementById(`chart-${index}`);
        if (!chartContainer) return;
        
        const viz = message.business_analysis.visualization;
        const chart = echarts.init(chartContainer);
        
        // 根据可视化类型构建配置
        let option;
        switch (viz.type.toLowerCase()) {
          case 'bar':
            option = createBarChart(message.result, viz);
            break;
          case 'line':
            option = createLineChart(message.result, viz);
            break;
          case 'pie':
            option = createPieChart(message.result, viz);
            break;
          default:
            option = createBarChart(message.result, viz);
        }
        
        chart.setOption(option);
        
        // 监听窗口大小变化，调整图表大小
        window.addEventListener('resize', () => {
          chart.resize();
        });
      }
    });
  });
}

// 创建柱状图配置
function createBarChart(data, viz) {
  const xAxis = viz.x_axis;
  const yAxis = viz.y_axis;
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'axis'
    },
    xAxis: {
      type: 'category',
      data: data.map(item => item[xAxis])
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      type: 'bar',
      data: data.map(item => item[yAxis]),
      itemStyle: {
        color: '#409EFF'
      }
    }]
  };
}

// 创建折线图配置
function createLineChart(data, viz) {
  const xAxis = viz.x_axis;
  const yAxis = viz.y_axis;
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'axis'
    },
    xAxis: {
      type: 'category',
      data: data.map(item => item[xAxis])
    },
    yAxis: {
      type: 'value'
    },
    series: [{
      type: 'line',
      data: data.map(item => item[yAxis]),
      smooth: true,
      itemStyle: {
        color: '#67C23A'
      }
    }]
  };
}

// 创建饼图配置
function createPieChart(data, viz) {
  const xAxis = viz.x_axis;
  const yAxis = viz.y_axis;
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b}: {c} ({d}%)'
    },
    series: [{
      name: viz.title,
      type: 'pie',
      radius: '60%',
      data: data.map(item => ({
        name: item[xAxis],
        value: item[yAxis]
      })),
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowOffsetX: 0,
          shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
      }
    }]
  };
}

// 重新连接
function reconnect() {
  mcpStore.connect();
}

// 获取阶段名称
function getStageName(stage) {
  // 先映射阶段，处理可能的变体
  const mappedStage = mapStageToDisplay(stage);
  const marker = dynamicStageMarkers.value.find(m => m.value === mappedStage);
  return marker ? marker.label : mcpStore.getStageName(stage) || `阶段 ${stage}`;
}

// 重置思考状态
function resetThinkingState() {
  mcpStore.isThinking.value = false;
  mcpStore.currentStage.value = 'waiting';
  mcpStore.queryProgress.value = 0;
  mcpStore.is_processing.value = false;
  lastStatusUpdateTime.value = new Date().toLocaleTimeString() + ' (手动重置)';
}

// 修改组件挂载代码，设置定时器
onMounted(async () => {
  // 连接后端
  if (!mcpStore.isConnected) {
    try {
      await mcpStore.connect();
    } catch (error) {
      console.error('连接后端失败:', error);
    }
  }
  
  // 设置状态更新定时器
  statusTimer = setInterval(async () => {
    if (mcpStore.isConnected) {
      try {
        await mcpStore.getNl2sqlStatus();
        statusUpdateCount.value++; // 更新计数器触发UI刷新
      } catch (error) {
        console.warn('状态更新失败，但继续尝试:', error);
      }
    }
  }, 2000); // 每2秒检查一次状态
  
  // 初始获取一次状态
  if (mcpStore.isConnected) {
    try {
      await mcpStore.getNl2sqlStatus();
    } catch (error) {
      console.warn('初始状态获取失败:', error);
    }
  }
  
  // 初始获取一次状态
  forceRefreshStatus();
  
  // 默认启用调试模式
  showDebug.value = true;
  
  // 设置定时检查
  setInterval(() => {
    if (mcpStore.isThinking || mcpStore.currentStage !== 'waiting') {
      forceRefreshStatus();
    }
  }, 500);
});

// 组件卸载时清除定时器
onUnmounted(() => {
  if (statusTimer) {
    clearInterval(statusTimer);
  }
});
</script>

<style lang="scss" scoped>
.nl2sql-view {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
  height: 100%;
  
  .chat-container {
    height: calc(100vh - 120px);
    display: flex;
    flex-direction: column;
    
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    
    .chat-messages {
      flex: 1;
      overflow-y: auto;
      padding: 20px 0;
      
      .empty-chat {
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
      }
      
      .message {
        display: flex;
        margin-bottom: 20px;
        gap: 16px;
      }
      
      .user-message {
        flex-direction: row-reverse;
      }
      
      .user-message .content {
        background-color: #ecf5ff;
        border-radius: 8px 2px 8px 8px;
        padding: 12px;
        max-width: 80%;
      }
      
      .system-message .content {
        background-color: #f5f7fa;
        border-radius: 2px 8px 8px 8px;
        padding: 12px;
        max-width: 80%;
      }
      
      .avatar {
        flex-shrink: 0;
      }
      
      .thinking-section, .sql-section, .result-section, .analysis-section, .error-section {
        margin-bottom: 16px;
        border: 1px solid #ebeef5;
        border-radius: 8px;
        overflow: hidden;
      }
      
      .section-header {
        background: #f5f7fa;
        padding: 8px 12px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-weight: bold;
        color: #409eff;
      }
      
      .thinking-content {
        padding: 12px;
      }
      
      .sql-code {
        background: #f8f8f8;
        padding: 10px;
        border-radius: 4px;
        font-family: 'Courier New', monospace;
        overflow-x: auto;
        margin: 10px 0;
      }
      
      .more-results {
        text-align: center;
        padding: 10px;
        background: #f5f7fa;
      }
      
      .loading-message {
        display: flex;
        margin-bottom: 20px;
        gap: 16px;
      }
      
      .loading-message .content {
        background-color: #f5f7fa;
        border-radius: 2px 8px 8px 8px;
        padding: 12px;
        flex: 1;
        max-width: 80%;
      }
      
      .chart-container {
        height: 300px;
        margin: 10px 0;
      }
      
      .chart-desc {
        color: #606266;
        text-align: center;
        margin-top: 5px;
      }
    }
    
    .chat-input {
      display: flex;
      gap: 10px;
      margin-top: 20px;
    }
  }
}

/* 新增样式 */
.progress-container {
  margin: 10px 0;
  position: relative;
  
  /* 添加动画效果 */
  transition: all 0.3s ease;
  
  /* 突出显示当前处理状态 */
  &:has(.stage-marker.active) {
    box-shadow: 0 0 8px rgba(64, 158, 255, 0.2);
  }
}

.stages-container {
  display: flex;
  justify-content: space-between;
  margin-top: 5px;
  padding: 0 5px;
  font-size: 12px;
  color: #666;
}

.stage-marker {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 60px;
  font-size: 12px;
  color: #909399;
  transition: all 0.5s ease; /* 增加过渡时间 */
}

.stage-marker::before {
  content: '';
  width: 12px;
  height: 12px;
  background-color: #dcdfe6;
  border-radius: 50%;
  margin-bottom: 5px;
  transition: all 0.5s ease;
}

.stage-marker.active {
  color: #409eff;
  font-weight: bold;
  transform: scale(1.1); /* 轻微放大 */
  z-index: 1; /* 确保显示在上层 */
}

.stage-marker.active::before {
  background-color: #409eff;
  box-shadow: 0 0 0 4px rgba(64, 158, 255, 0.3);
  transform: scale(1.2);
  animation: pulse-blue 2s infinite; /* 添加脉动动画 */
}

.stage-marker.completed {
  color: #67c23a;
}

.stage-marker.completed::before {
  background-color: #67c23a;
  box-shadow: 0 0 0 2px rgba(103, 194, 58, 0.2);
}

/* 添加一个脉动效果的动画 */
@keyframes pulse-blue {
  0% {
    box-shadow: 0 0 0 0 rgba(64, 158, 255, 0.5);
  }
  70% {
    box-shadow: 0 0 0 6px rgba(64, 158, 255, 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(64, 158, 255, 0);
  }
}

.current-stage-badge {
  background-color: #E6F7FF;
  color: #1890ff;
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 12px;
  margin-left: 12px;
  display: inline-flex;
  align-items: center;
  transition: all 0.3s ease; /* 平滑过渡效果 */
}

.current-stage-badge.processing {
  background-color: #f0f9eb;
  color: #67c23a;
  animation: slight-pulse 2s infinite; /* 添加轻微的呼吸效果 */
}

.badge-icon {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #1890ff;
  margin-right: 6px;
}

.current-stage-badge.processing .badge-icon {
  background-color: #67c23a;
  animation: pulse 1.5s infinite;
}

/* 轻微的脉动效果 */
@keyframes slight-pulse {
  0% {
    opacity: 0.9;
  }
  50% {
    opacity: 1;
  }
  100% {
    opacity: 0.9;
  }
}

/* 确保进度条更新时有动画效果 */
:deep(.el-progress-bar__inner) {
  transition: width 0.5s ease-out !important;
}

/* 连接状态样式 */
.header-actions {
  display: flex;
  gap: 10px;
  align-items: center;
}

.connection-status {
  display: flex;
  align-items: center;
  gap: 5px;
}

.reconnect-btn {
  padding: 2px;
  margin-left: 5px;
}

:deep(.el-tag.el-tag--success) {
  border-color: #67c23a;
}

:deep(.el-tag.el-tag--danger) {
  border-color: #f56c6c;
}

/* 调试模式样式 */
.debug-switch {
  margin-right: 10px;
}

.debug-info {
  font-family: monospace;
  background-color: #f8f8f8;
  padding: 10px;
  border-radius: 4px;
  font-size: 12px;
}

.debug-info p {
  margin: 5px 0;
}

.debug-info ul {
  margin: 5px 0;
  padding-left: 20px;
}

.debug-info li {
  margin: 2px 0;
}

/* 增强显示效果 */
.no-thinking-yet {
  padding: 10px;
  background-color: #f9f9f9;
  border-radius: 4px;
  
  .stage-note {
    color: #67c23a;
    font-weight: bold;
    margin: 5px 0;
  }
}

/* 思考过程区域样式 */
.thinking-section {
  margin-bottom: 20px;
  border: 1px solid #eaeaea;
  border-radius: 8px;
  padding: 15px;
  background: #fcfcfc;
}

.thinking-header {
  display: flex;
  align-items: center;
  margin-bottom: 15px;
}

.thinking-title {
  font-size: 16px;
  font-weight: bold;
}

.thinking-status {
  color: #409eff;
  margin-left: 5px;
}

.progress-container {
  margin-bottom: 15px;
}

.progress-markers {
  display: flex;
  justify-content: space-between;
  margin-bottom: 5px;
}

.stage-marker {
  display: flex;
  flex-direction: column;
  align-items: center;
  position: relative;
  flex: 1;
  opacity: 0.6;
  transition: all 0.3s ease;
}

.stage-marker.current {
  opacity: 1;
  color: #409eff;
  font-weight: bold;
}

.stage-marker.completed {
  opacity: 1;
  color: #67c23a;
}

.marker-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background-color: #dcdfe6;
  margin-bottom: 5px;
}

.stage-marker.current .marker-dot {
  background-color: #409eff;
  box-shadow: 0 0 0 3px rgba(64, 158, 255, 0.2);
  animation: pulse 1.5s infinite;
}

.stage-marker.completed .marker-dot {
  background-color: #67c23a;
}

.marker-label {
  font-size: 12px;
  text-align: center;
}

.progress-bar {
  height: 8px;
  background-color: #f0f0f0;
  border-radius: 4px;
  overflow: hidden;
  position: relative;
}

.progress-filled {
  height: 100%;
  background-color: #409eff;
  border-radius: 4px;
  transition: width 0.3s ease;
}

.thinking-text {
  padding: 10px;
  min-height: 60px;
  border: 1px solid #eaeaea;
  border-radius: 4px;
  background: white;
}

.no-thinking-yet {
  color: #909399;
  text-align: center;
  padding: 20px 0;
}

/* 调试工具栏样式 */
.debug-toolbar {
  margin-top: 15px;
  padding: 10px;
  border: 1px dashed #e6a23c;
  border-radius: 4px;
  background: #fdf6ec;
}

.debug-actions {
  display: flex;
  margin-bottom: 10px;
}

.debug-button {
  margin-right: 10px;
  padding: 5px 10px;
  background: #f56c6c;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

.raw-status {
  font-family: monospace;
  font-size: 12px;
  background: #303133;
  color: #eee;
  padding: 10px;
  border-radius: 4px;
  overflow-x: auto;
}

@keyframes pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(64, 158, 255, 0.4);
  }
  70% {
    box-shadow: 0 0 0 5px rgba(64, 158, 255, 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(64, 158, 255, 0);
  }
}

.pulse-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #67c23a;
  margin-left: 6px;
  animation: pulse 1.5s infinite;
}

@keyframes pulse {
  0% {
    box-shadow: 0 0 0 0 rgba(103, 194, 58, 0.7);
  }
  70% {
    box-shadow: 0 0 0 6px rgba(103, 194, 58, 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(103, 194, 58, 0);
  }
}
</style> 