<template>
  <div class="nl2sql-view">
    <!-- 将卡片头部移出卡片，固定在顶部 -->
    <div class="fixed-card-header">
      <div class="header">
        <h2><span class="title-decoration"></span>NL2SQL 自然语言查询</h2>
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
    </div>
    
    <!-- 用普通div替换el-card，避免其默认样式 -->
    <div class="chat-container">
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
      
      <div class="chat-messages" ref="messagesContainer" @scroll="handleScrollEvent">
        <div v-if="chatMessages.length === 0" class="empty-chat">
          <el-empty description="开始对话，输入您的自然语言查询">
          </el-empty>
        </div>
        
        <div v-else class="messages-wrapper">
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
                  <div class="markdown-code-block">
                    <pre class="sql-code"><code>{{ message.sql }}</code></pre>
                  </div>
                </div>
                
                <!-- 查询结果 - 增强样式 -->
                <div v-if="message.result && message.result.length > 0" class="result-section">
                  <div class="section-header">
                    <span>查询结果 ({{ message.result.length }}条记录)</span>
                  </div>
                  <el-table 
                    :data="message.result" 
                    style="width: 100%" 
                    border 
                    :max-height="400"
                    :header-cell-style="{background:'#f5f7fa', color:'#606266'}"
                  >
                    <el-table-column
                      v-for="column in message.column_names"
                      :key="column"
                      :prop="column"
                      :label="column"
                      show-overflow-tooltip
                    />
                  </el-table>
                </div>
                
                <!-- 业务分析 -->
                <div v-if="message.business_analysis" class="analysis-section">
                  <div class="section-header">
                    <span>业务分析</span>
                  </div>
                  <div v-html="formatText(message.business_analysis.business_analysis)" class="analysis-text formatted-content"></div>
                  
                  <!-- 可视化图表 -->
                  <div v-if="message.business_analysis.visualization" class="visualization">
                    <h4>{{ message.business_analysis.visualization.title }}</h4>
                    <div :id="`chart-${index}`" class="chart-container"></div>
                    <p class="chart-desc">{{ message.business_analysis.visualization.description }}</p>
                  </div>
                  
                  <!-- 趋势和建议 -->
                  <div v-if="message.business_analysis.trends && message.business_analysis.trends.length" class="trends formatted-content">
                    <h4>主要趋势</h4>
                    <ul class="styled-list">
                      <li v-for="(trend, i) in message.business_analysis.trends" :key="i">{{ trend }}</li>
                    </ul>
                  </div>
                  
                  <div v-if="message.business_analysis.recommendations && message.business_analysis.recommendations.length" class="recommendations formatted-content">
                    <h4>业务建议</h4>
                    <ul class="styled-list">
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
                <div v-if="!message.sql && !message.result && !message.error && !message.business_analysis" class="normal-message">
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
      </div>
    </div>
    
    <!-- 聊天输入框容器保持不变 -->
    <div class="chat-input-container">
      <div class="chat-input">
        <el-input
          v-model="inputMessage"
          type="textarea"
          :rows="3"
          placeholder="输入您的自然语言查询，例如：'2023年第一季度销售额最高的三个产品是什么？'"
          :disabled="isLoading || !mcpStore.available"
          @keydown.enter.prevent="sendMessage"
          ref="inputRef"
        />
        <el-button 
          type="primary"
          :disabled="!inputMessage.trim() || isLoading || !mcpStore.available"
          @click="sendMessage"
          class="center-button"
        >
          发送
        </el-button>
      </div>
    </div>
    
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
    
    <!-- 修改滚动到底部按钮，使其更明显 -->
    <div class="scroll-to-bottom-btn" @click="scrollToBottom">
      <el-icon><ArrowDown /></el-icon> 
      <span>滚动到底部</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick, watch, computed, onUnmounted } from 'vue';
import { useMCPStore } from '../stores/mcp';
import { ElMessage } from 'element-plus';
import * as echarts from 'echarts';
import { UserFilled, Refresh, Loading, ArrowDown } from '@element-plus/icons-vue';
import logoUrl from '../assets/logo.svg';

const mcpStore = useMCPStore();
const chatMessages = ref([]);
const inputMessage = ref('');
const isLoading = ref(false);
const messagesContainer = ref(null);
const inputRef = ref(null);
const thinkingContentRef = ref(null);
const debugMode = ref(false);
const showDebug = ref(false);
const lastStatusUpdateTime = ref('无');
let typingTimer = null;
let statusTimer = null;

// 完全重写滚动控制变量和逻辑
const userHasScrolled = ref(false);
const lastScrollPosition = ref(0);
const autoScrollEnabled = ref(false);
const isManualScrolling = ref(false);
const scrollLocked = ref(false); // 添加新变量用于锁定滚动
let scrollTimer = null;
let scrollLockTimer = null;
let lastScrollTime = Date.now();

// 添加全屏结果对话框数据（恢复误删的变量）
const fullResultsVisible = ref(false);
const fullResultsData = ref([]);
const fullResultsColumns = ref([]);

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

// 修改监听函数，避免不必要的滚动
watch(chatMessages, (newVal, oldVal) => {
  // 只渲染图表，不干预滚动
  nextTick(() => {
    renderCharts();
  });
}, { deep: true });

// 监听状态更新，确保UI刷新
watch(() => [mcpStore.currentStage, mcpStore.queryProgress, mcpStore.isThinking], () => {
  // 仅更新计数器，不触发滚动
  statusUpdateCount.value++;
}, { deep: true });

// 监听思考过程更新，移除自动滚动逻辑
let lastThinkingUpdateTime = 0;
let lastThinkingContent = '';
watch(() => mcpStore.thinkingProcess, (newValue) => {
  if (!newValue || lastThinkingContent === newValue) return;
  
  // 处理新增内容
  const newContent = newValue.substring(lastThinkingContent.length);
  if (newContent.trim()) {
    typeThinkingText(newContent);
  }
  
  lastThinkingContent = newValue;
});

// 监听阶段历史更新，移除自动滚动逻辑
watch(() => mcpStore.stageHistory, (newHistory) => {
  console.log('阶段历史已更新:', newHistory);
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

// 修改发送消息函数，完全移除所有自动滚动
async function sendMessage() {
  if (!inputMessage.value.trim() || isLoading.value || !mcpStore.available) return;
  
  // 添加用户消息
  const query = inputMessage.value.trim();
  chatMessages.value.push({
    role: 'user',
    content: query
  });
  
  // 清空输入框，不再自动滚动
  inputMessage.value = '';
  
  // 显示加载状态
  isLoading.value = true;
  // 设置初始状态
  mcpStore.setThinkingState(true);
  mcpStore.setCurrentStage('start');
  mcpStore.setProgress(5);
  statusUpdateCount.value++; // 增加计数器触发重新渲染
  
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
          });
        },
        
        onProgress: (data) => {
          console.log('进度更新:', data);
          // 立即获取最新状态
          mcpStore.getNl2sqlStatus().then(() => {
            statusUpdateCount.value++; // 增加计数器
          });
        },
        
        onFinal: (result) => {
          // 处理最终结果
          console.log('最终结果:', result);
          
          // 停止状态轮询
          mcpStore.stopStatusPolling();
          
          // 确保结果数据格式正确
          let formattedResult = result.result || [];
          if (formattedResult && !Array.isArray(formattedResult)) {
            console.warn('结果不是数组格式，尝试转换', formattedResult);
            // 如果结果不是数组，尝试处理
            if (typeof formattedResult === 'object') {
              formattedResult = [formattedResult];
            } else {
              formattedResult = [];
            }
          }
          
          // 确保列名存在
          let columnNames = result.column_names || [];
          if (formattedResult.length > 0 && (!columnNames || columnNames.length === 0)) {
            console.warn('未提供列名，从结果中提取');
            columnNames = Object.keys(formattedResult[0]);
          }
          
          // 添加思考过程折叠状态
          const thinkingLength = result.thinking_process ? result.thinking_process.length : 0;
          const shouldCollapse = thinkingLength > 500;
          
          // 添加系统回复
          chatMessages.value.push({
            role: 'system',
            content: result.message || '查询处理完成',
            sql: result.sql,
            result: formattedResult, // 使用格式化后的结果
            column_names: columnNames, // 使用格式化后的列名
            business_analysis: result.business_analysis,
            error: result.error,
            thinking_process: result.thinking_process,
            thinkingCollapsed: shouldCollapse
          });
          
          // 完成后渲染图表，但不自动滚动
          nextTick(() => {
            renderCharts();
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
  }
}

// 完全重写滚动到底部函数，解决不能向上滚动的问题
function scrollToBottom() {
  if (!messagesContainer.value) return;
  
  console.log('手动滚动到底部', new Date().toISOString());
  
  try {
    // 获取滚动区域高度
    const container = messagesContainer.value;
    const scrollHeight = container.scrollHeight;
    
    // 确保滚动到最底部
    container.scrollTo({
      top: scrollHeight + 1000, // 加大数值确保滚动到底
      behavior: 'smooth'
    });
    
    // 额外保证滚动到底部 (有时候第一次滚动可能不完全)
    setTimeout(() => {
      const newScrollHeight = container.scrollHeight;
      if (newScrollHeight > scrollHeight) {
        container.scrollTo({
          top: newScrollHeight + 1000,
          behavior: 'smooth'
        });
      }
    }, 100);
  } catch (e) {
    console.error('滚动执行失败:', e);
  }
}

// 重写滚动事件处理函数 - 不做任何处理，让用户完全控制
function handleScrollEvent(event) {
  // 不做任何处理，让用户完全控制滚动
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
  console.log('开始渲染图表...');
  
  // 延迟执行以确保DOM已更新
  setTimeout(() => {
    chatMessages.value.forEach((message, index) => {
      if (message.role === 'system' && 
          message.business_analysis && 
          message.business_analysis.visualization &&
          message.result) {
        
        try {
          const chartId = `chart-${index}`;
          console.log(`尝试渲染图表 ${chartId}`);
          
          const chartContainer = document.getElementById(chartId);
          if (!chartContainer) {
            console.error(`图表容器 ${chartId} 不存在`);
            return;
          }
          
          // 强制设置容器尺寸
          chartContainer.style.height = '400px';
          chartContainer.style.width = '100%';
          
          const viz = message.business_analysis.visualization;
          console.log(`图表配置:`, viz);
          
          // 确保必要的属性存在
          if (!viz.type || !viz.x_axis) {
            console.error('缺少必要的可视化属性');
            chartContainer.innerHTML = `<div class="chart-error">图表配置缺少必要属性</div>`;
            return;
          }
          
          // 创建图表实例
          try {
            // 先清除现有图表
            try {
              const existingChart = echarts.getInstanceByDom(chartContainer);
              if (existingChart) {
                existingChart.dispose();
              }
            } catch (e) {
              console.warn('清除旧图表失败', e);
            }
            
            // 清空容器
            chartContainer.innerHTML = '';
            
            // 创建新图表
            const chart = echarts.init(chartContainer);
            
            // 根据类型创建配置
            let option;
            if (viz.type.toLowerCase() === 'bar') {
              option = createBarChart(message.result, viz, Array.isArray(viz.y_axis) ? viz.y_axis : [viz.y_axis]);
            } else if (viz.type.toLowerCase() === 'line') {
              option = createLineChart(message.result, viz, Array.isArray(viz.y_axis) ? viz.y_axis : [viz.y_axis]);
            } else if (viz.type.toLowerCase() === 'pie') {
              option = createPieChart(message.result, viz, Array.isArray(viz.y_axis) ? viz.y_axis : [viz.y_axis]);
            } else {
              option = createBarChart(message.result, viz, Array.isArray(viz.y_axis) ? viz.y_axis : [viz.y_axis]);
            }
            
            // 设置配置
            chart.setOption(option);
            
            // 尝试调整大小
            setTimeout(() => {
              if (chart && !chart.isDisposed()) {
                chart.resize();
              }
            }, 200);
            
          } catch (chartError) {
            console.error('创建图表失败:', chartError);
            chartContainer.innerHTML = `<div class="chart-error">图表创建失败: ${chartError.message}</div>`;
          }
        } catch (error) {
          console.error('图表处理错误:', error);
        }
      }
    });
  }, 300);
}

// 创建柱状图配置
function createBarChart(data, viz, yAxisFields) {
  const xAxis = viz.x_axis;
  
  // 预处理数据，处理可能的大数值
  const processedData = data.map(item => {
    const processedItem = {};
    // 复制原始数据
    for (const key in item) {
      // 检查是否是数值型且值非常大
      if (!isNaN(item[key]) && typeof item[key] === 'number' && item[key] > 1000000000000) {
        // 转换为万亿单位
        processedItem[key] = Math.round(item[key] / 1000000000000) / 100;
      } else {
        processedItem[key] = item[key];
      }
    }
    return processedItem;
  });
  
  // 确定是否需要显示数值单位
  const needsLargeNumberUnit = yAxisFields.some(field => {
    return data.some(item => !isNaN(item[field]) && item[field] > 1000000000000);
  });
  
  // 柱状图系列配置
  const series = yAxisFields.map((field, index) => {
    return {
      name: field + (needsLargeNumberUnit && data.some(item => !isNaN(item[field]) && item[field] > 1000000000000) ? ' (万亿)' : ''),
      type: 'bar',
      data: processedData.map(item => item[field]),
      itemStyle: {
        // 使用不同颜色区分不同系列
        color: index === 0 ? '#409EFF' : 
               index === 1 ? '#67C23A' : 
               index === 2 ? '#E6A23C' : 
               index === 3 ? '#F56C6C' : 
               '#909399'
      }
    };
  });
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'axis',
      formatter: function(params) {
        let result = params[0].name + '<br/>';
        params.forEach(param => {
          // 检查原始数据是否需要特殊显示
          const originalValue = data[param.dataIndex][param.seriesName.split(' ')[0]];
          const valueDisplay = !isNaN(originalValue) && originalValue > 1000000000000 ? 
            (originalValue / 1000000000000).toFixed(2) + ' 万亿' : originalValue;
          
          result += `${param.marker} ${param.seriesName}: ${valueDisplay}<br/>`;
        });
        return result;
      }
    },
    legend: {
      data: yAxisFields.map((field, index) => 
        field + (needsLargeNumberUnit && data.some(item => !isNaN(item[field]) && item[field] > 1000000000000) ? ' (万亿)' : '')
      ),
      orient: 'horizontal',
      bottom: 10
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      top: '10%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: data.map(item => item[xAxis]),
      axisLabel: {
        rotate: 45
      }
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        formatter: function(value) {
          if (needsLargeNumberUnit) {
            return value.toFixed(2);
          }
          return value;
        }
      }
    },
    series: series
  };
}

// 创建折线图配置
function createLineChart(data, viz, yAxisFields) {
  const xAxis = viz.x_axis;
  
  // 预处理数据，处理可能的大数值
  const processedData = data.map(item => {
    const processedItem = {};
    // 复制原始数据
    for (const key in item) {
      // 检查是否是数值型且值非常大
      if (!isNaN(item[key]) && typeof item[key] === 'number' && item[key] > 1000000000000) {
        // 转换为万亿单位
        processedItem[key] = Math.round(item[key] / 1000000000000) / 100;
      } else {
        processedItem[key] = item[key];
      }
    }
    return processedItem;
  });
  
  // 确定是否需要显示数值单位
  const needsLargeNumberUnit = yAxisFields.some(field => {
    return data.some(item => !isNaN(item[field]) && item[field] > 1000000000000);
  });
  
  // 折线图系列配置
  const series = yAxisFields.map((field, index) => {
    return {
      name: field + (needsLargeNumberUnit && data.some(item => !isNaN(item[field]) && item[field] > 1000000000000) ? ' (万亿)' : ''),
      type: 'line',
      data: processedData.map(item => item[field]),
      smooth: true,
      itemStyle: {
        // 使用不同颜色区分不同系列
        color: index === 0 ? '#409EFF' : 
               index === 1 ? '#67C23A' : 
               index === 2 ? '#E6A23C' : 
               index === 3 ? '#F56C6C' : 
               '#909399'
      }
    };
  });
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'axis',
      formatter: function(params) {
        let result = params[0].name + '<br/>';
        params.forEach(param => {
          // 检查原始数据是否需要特殊显示
          const originalValue = data[param.dataIndex][param.seriesName.split(' ')[0]];
          const valueDisplay = !isNaN(originalValue) && originalValue > 1000000000000 ? 
            (originalValue / 1000000000000).toFixed(2) + ' 万亿' : originalValue;
          
          result += `${param.marker} ${param.seriesName}: ${valueDisplay}<br/>`;
        });
        return result;
      }
    },
    legend: {
      data: yAxisFields.map((field, index) => 
        field + (needsLargeNumberUnit && data.some(item => !isNaN(item[field]) && item[field] > 1000000000000) ? ' (万亿)' : '')
      ),
      orient: 'horizontal',
      bottom: 10
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '15%',
      top: '10%',
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: data.map(item => item[xAxis]),
      axisLabel: {
        rotate: 45
      }
    },
    yAxis: {
      type: 'value',
      axisLabel: {
        formatter: function(value) {
          if (needsLargeNumberUnit) {
            return value.toFixed(2);
          }
          return value;
        }
      }
    },
    series: series
  };
}

// 创建饼图配置
function createPieChart(data, viz, yAxisFields) {
  const xAxis = viz.x_axis;
  // 饼图通常只使用第一个y轴值
  const yAxis = yAxisFields[0];
  
  // 处理大数值
  const needsLargeNumberUnit = data.some(item => !isNaN(item[yAxis]) && item[yAxis] > 1000000000000);
  
  // 准备饼图数据
  const pieData = data.map(item => {
    let value = item[yAxis];
    if (!isNaN(value) && value > 1000000000000) {
      value = Math.round(value / 1000000000000) / 100;
    }
    return {
      name: item[xAxis],
      value: value
    };
  });
  
  return {
    title: {
      text: viz.title
    },
    tooltip: {
      trigger: 'item',
      formatter: function(param) {
        // 获取原始值
        const origItem = data.find(item => item[xAxis] === param.name);
        const originalValue = origItem ? origItem[yAxis] : param.value;
        const valueDisplay = !isNaN(originalValue) && originalValue > 1000000000000 ? 
          (originalValue / 1000000000000).toFixed(2) + ' 万亿' : originalValue;
        
        return `${param.seriesName}<br/>${param.name}: ${valueDisplay} (${param.percent}%)`;
      }
    },
    legend: {
      orient: 'horizontal',
      bottom: 10,
      type: 'scroll',
      pageButtonPosition: 'end'
    },
    series: [{
      name: yAxis + (needsLargeNumberUnit ? ' (万亿)' : ''),
      type: 'pie',
      radius: '60%',
      data: pieData,
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowOffsetX: 0,
          shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
      },
      label: {
        formatter: '{b}: {d}%'
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

// 修改组件挂载代码
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
  
  // 正确添加滚动事件监听
  nextTick(() => {
    if (messagesContainer.value) {
      console.log('添加滚动事件监听器');
      messagesContainer.value.addEventListener('scroll', handleScrollEvent, { passive: true });
    }
  });
  
  // 监听窗口大小变化，重新渲染图表
  window.addEventListener('resize', () => {
    // 延迟执行以避免频繁触发
    clearTimeout(window.resizeTimer);
    window.resizeTimer = setTimeout(() => {
      console.log('窗口大小变化，重新渲染图表');
      renderCharts();
    }, 200);
  });
});

// 组件卸载清理
onUnmounted(() => {
  // 清理所有定时器
  if (statusTimer) clearInterval(statusTimer);
  if (scrollTimer) clearTimeout(scrollTimer);
  if (scrollLockTimer) clearTimeout(scrollLockTimer);
  if (window.resizeTimer) clearTimeout(window.resizeTimer);
  
  // 移除事件监听器
  if (messagesContainer.value) {
    console.log('移除滚动事件监听器');
    messagesContainer.value.removeEventListener('scroll', handleScrollEvent);
  }
  
  // 清理所有图表实例
  document.querySelectorAll('[id^="chart-"]').forEach(container => {
    try {
      const chart = echarts.getInstanceByDom(container);
      if (chart) {
        chart.dispose();
      }
    } catch (e) {
      console.warn('清理图表失败:', e);
    }
  });
  
  // 移除窗口大小变化监听
  window.removeEventListener('resize', () => {});
});

// 添加打字效果函数的空实现
function typeThinkingText(newContent) {
  // 空实现，不做任何操作
  console.log('收到新思考内容，但不应用打字效果:', newContent.length, '字符');
}
</script>

<style lang="scss" scoped>
.nl2sql-view {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 20px 80px;
  height: 100%;
  position: relative;
  flex-direction: column;
  box-sizing: border-box;
  
  // 固定的卡片头部样式
  .fixed-card-header {
    position: fixed;
    top: 60px;
    left: 0;
    right: 0;
    background-color: #fff;
    z-index: 99;
    padding: 8px 20px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
    border-bottom: 1px solid #ebeef5;
    
    // 居中内容，与容器保持一致
    .header {
      max-width: 1160px;
      margin: 0 auto;
      display: flex;
      justify-content: space-between;
      align-items: center;
      
      h2 {
        margin: 0;
        font-size: 18px;
        color: #303133;
        display: flex;
        align-items: center;
        position: relative;
        padding-left: 15px;
        
        .title-decoration {
          position: absolute;
          left: 0;
          top: 50%;
          transform: translateY(-50%);
          width: 4px;
          height: 18px;
          background-color: #409eff;
          border-radius: 2px;
        }
      }
      
      .header-actions {
        display: flex;
        align-items: center;
        gap: 12px;
        
        .debug-switch {
          margin-right: 10px;
        }
        
        .connection-status {
          padding: 0 10px;
          height: 28px;
          line-height: 26px;
          display: flex;
          align-items: center;
          
          .reconnect-btn {
            padding: 2px;
            margin-left: 5px;
          }
        }
      }
    }
  }
  
  // 聊天输入框容器样式
  .chat-input-container {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    background-color: #fff;
    box-shadow: 0 -2px 4px rgba(0, 0, 0, 0.08);
    padding: 8px 15px;
    z-index: 10;
    box-sizing: border-box;
    transition: transform 0.3s ease;
    border-top: 1px solid #ebeef5;
    
    &.hidden {
      transform: translateY(100%);
    }
    
    .chat-input {
      max-width: 1160px;
      margin: 0 auto;
      display: flex;
      gap: 10px;
      
      .el-input {
        flex: 1;
      }
      
      .el-button {
        flex-shrink: 0;
        align-self: flex-end;
        height: 40px;
      }
      
      @media (max-width: 768px) {
        flex-direction: column;
        
        .el-button {
          margin-top: 10px;
          align-self: stretch;
          width: 100%;
        }
      }
      
      :deep(.el-textarea__inner) {
        border-radius: 8px;
        border-color: #dcdfe6;
        padding: 12px;
        transition: all 0.3s ease;
        resize: none;
        
        &:focus {
          border-color: #409eff;
          box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.2);
        }
      }
      
      .el-button {
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s ease;
        
        &:hover:not(:disabled) {
          transform: translateY(-2px);
          box-shadow: 0 4px 12px rgba(64, 158, 255, 0.2);
        }
        
        &:active:not(:disabled) {
          transform: translateY(0);
        }
      }
    }
  }
  
  // 响应式调整整体布局
  @media (max-width: 768px) {
    padding: 10px 10px 130px;
    
    .chat-container {
      height: calc(100vh - 300px);
      margin-top: 80px;
    }
    
    .fixed-card-header {
      padding: 10px;
    }
  }
  
  // 为所有卡片和按钮添加过渡效果
  * {
    transition: background-color 0.3s, border-color 0.3s, box-shadow 0.3s;
  }
  
  // 添加滚动到底部的平滑过渡
  .chat-messages {
    scroll-behavior: smooth;
  }
  
  // 消息出现动画
  .message {
    animation: fadeIn 0.3s ease-in-out;
  }
  
  @keyframes fadeIn {
    from {
      opacity: 0;
      transform: translateY(10px);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }
}

// 卡片容器样式 - 现在是普通div，不再是el-card
.chat-container {
  height: calc(100vh - 180px);
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  margin-bottom: 0;
  margin-top: 55px;
  border-radius: 4px;
  border: 1px solid #e4e7ed;
  background-color: #fff;
  
  // 调整折叠面板样式
  :deep(.el-collapse) {
    border: none;
    
    .el-collapse-item__header {
      font-size: 14px;
      color: #606266;
      font-weight: 500;
      padding: 8px 15px;
      border-bottom: 1px solid #f0f2f5;
      background-color: #f8f9fb;
    }
    
    .el-collapse-item__content {
      padding: 8px 15px;
      border-bottom: 1px solid #f0f2f5;
    }
    
    // 调整图标样式
    .el-collapse-item__arrow {
      margin-right: 0;
      font-size: 14px;
    }
  }
  
  // 调整报错提示
  :deep(.el-alert) {
    margin: 0 15px 10px;
  }
}

// 聊天消息容器
.chat-messages {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  height: 100%;
  background-color: #fff;
}

// 消息包装容器
.messages-wrapper {
  padding: 10px 15px;
  flex: 1;
  display: flex;
  flex-direction: column;
  background-color: #fff;
}

// 消息样式
.message {
  margin-bottom: 12px;
  display: flex;
  gap: 10px;
  
  &.user-message {
    flex-direction: row-reverse;
    justify-content: flex-start;
    
    .content {
      background-color: #ecf5ff;
      color: #303133;
      border-radius: 8px 0 8px 8px;
    }
  }
  
  &.system-message {
    .content {
      background-color: #f5f7fa;
      border-radius: 0 8px 8px 8px;
    }
  }
  
  .avatar {
    flex-shrink: 0;
  }
  
  .content {
    padding: 10px 15px;
    max-width: 85%;
    word-break: break-word;
  }
}

// 空状态样式
.empty-chat {
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  flex: 1;
  padding: 0;
  
  .el-empty {
    margin: 0;
    padding: 10px 0;
    
    :deep(.el-empty__image) {
      width: 100px;
      height: 100px;
    }
    
    :deep(.el-empty__description) {
      margin-top: 15px;
      color: #909399;
      font-size: 14px;
    }
  }
  
  :deep(.el-button) {
    padding: 8px 20px;
    font-size: 14px;
    margin-top: 15px;
    transition: all 0.3s ease;
    
    &:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(64, 158, 255, 0.2);
    }
  }
}

/* 新增样式 */
.progress-container {
  margin: 10px 0;
  position: relative;
  transition: all 0.3s ease;
  
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
  transition: all 0.5s ease;
  
  .marker-dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background-color: #dcdfe6;
    margin-bottom: 5px;
  }
  
  .marker-label {
    font-size: 12px;
    text-align: center;
  }
  
  &.active {
    color: #409eff;
    font-weight: bold;
    transform: scale(1.1);
    z-index: 1;
    
    .marker-dot {
      background-color: #409eff;
      box-shadow: 0 0 0 4px rgba(64, 158, 255, 0.3);
      transform: scale(1.2);
      animation: pulse-blue 2s infinite;
    }
  }
  
  &.completed {
    color: #67c23a;
    
    .marker-dot {
      background-color: #67c23a;
      box-shadow: 0 0 0 2px rgba(103, 194, 58, 0.2);
    }
  }
  
  &.current .marker-dot {
    background-color: #409eff;
    box-shadow: 0 0 0 3px rgba(64, 158, 255, 0.2);
    animation: pulse 1.5s infinite;
  }
}

.progress-bar {
  height: 8px;
  background-color: #f0f0f0;
  border-radius: 4px;
  overflow: hidden;
  position: relative;
  margin-top: 5px;
}

.progress-filled {
  height: 100%;
  background-color: #409eff;
  border-radius: 4px;
  transition: width 0.3s ease;
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
  
  .stage-note {
    color: #67c23a;
    font-weight: bold;
    margin: 5px 0;
  }
}

.progress-markers {
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  margin-bottom: 15px;
  width: 100%;
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
  transition: all 0.3s ease;
  
  &.processing {
    background-color: #f0f9eb;
    color: #67c23a;
    animation: slight-pulse 2s infinite;
  }
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

@keyframes slight-pulse {
  0% { opacity: 0.9; }
  50% { opacity: 1; }
  100% { opacity: 0.9; }
}

@keyframes pulse {
  0% { box-shadow: 0 0 0 0 rgba(64, 158, 255, 0.4); }
  70% { box-shadow: 0 0 0 5px rgba(64, 158, 255, 0); }
  100% { box-shadow: 0 0 0 0 rgba(64, 158, 255, 0); }
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

/* 连接状态和调试样式 */
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

.debug-switch {
  margin-right: 10px;
}

.debug-info {
  font-family: monospace;
  background-color: #f8f8f8;
  padding: 8px;
  border-radius: 4px;
  font-size: 12px;
  
  p {
    margin: 3px 0;
  }
  
  ul {
    margin: 3px 0;
    padding-left: 20px;
  }
  
  li {
    margin: 1px 0;
  }
}

// 图表错误样式
.chart-error {
  padding: 15px;
  background-color: #fef0f0;
  border: 1px solid #fde2e2;
  border-radius: 4px;
  margin: 10px 0;
  
  h4 {
    color: #f56c6c;
    margin-top: 0;
    margin-bottom: 10px;
  }
  
  p, ul {
    margin: 5px 0;
    color: #666;
  }
}

// 图表相关样式
.visualization {
  margin: 15px 0;
  
  h4 {
    margin-bottom: 10px;
    color: #303133;
  }
  
  .chart-container {
    width: 100%;
    height: 400px; // 明确设置高度
    border: 1px solid #ebeef5;
    border-radius: 4px;
    margin: 10px 0;
    background-color: #fcfcfc;
  }
  
  .chart-desc {
    margin-top: 10px;
    font-size: 13px;
    color: #606266;
  }
}

.result-section {
  margin: 15px 0;
  padding: 10px;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  background-color: #fff;
  
  .section-header {
    margin-bottom: 10px;
    font-weight: bold;
    color: #303133;
  }
  
  :deep(.el-table) {
    margin-bottom: 10px;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.1);
    
    .el-table__header-wrapper th {
      background-color: #f5f7fa;
      color: #606266;
      font-weight: 600;
    }
    
    .el-table__body-wrapper td {
      padding: 8px;
    }
    
    .el-table__empty-block {
      min-height: 100px;
    }
  }
}

// 修改滚动到底部按钮的逻辑和显示条件
.scroll-to-bottom-btn {
  position: fixed;
  right: 30px;
  bottom: 100px;
  background-color: #409eff;
  color: white;
  padding: 10px 20px;
  border-radius: 24px;
  box-shadow: 0 4px 16px 0 rgba(0, 0, 0, 0.15);
  cursor: pointer;
  z-index: 1000;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 16px;
  transition: all 0.3s;
  animation: pulse-btn 2s infinite;
  
  &:hover {
    background-color: #66b1ff;
    transform: translateY(-3px);
    box-shadow: 0 6px 20px 0 rgba(0, 0, 0, 0.2);
  }
  
  .el-icon {
    font-size: 18px;
  }
}

@keyframes pulse-btn {
  0% { transform: translateY(0); }
  50% { transform: translateY(-5px); }
  100% { transform: translateY(0); }
}

// 淡入淡出动画
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.3s, transform 0.3s;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
  transform: translateY(20px);
}

// 在样式部分添加center-button类
.center-button {
  margin: auto;
  padding: 12px 40px;
  font-size: 16px;
  height: auto;
  width: 120px;
}

// 恢复脉冲动画关键帧
@keyframes pulse-blue {
  0% { box-shadow: 0 0 0 0 rgba(64, 158, 255, 0.5); }
  70% { box-shadow: 0 0 0 6px rgba(64, 158, 255, 0); }
  100% { box-shadow: 0 0 0 0 rgba(64, 158, 255, 0); }
}

// 恢复进度条内部过渡效果
:deep(.el-progress-bar__inner) {
  transition: width 0.5s ease-out !important;
}

/* 添加新的内容样式 */
/* SQL代码块样式 */
.markdown-code-block {
  background-color: #f8f8f8;
  border-radius: 6px;
  border: 1px solid #e0e0e0;
  margin: 10px 0;
  position: relative;
  overflow: hidden;
  
  &::before {
    content: "SQL";
    position: absolute;
    top: 0;
    right: 0;
    background-color: #e0e0e0;
    color: #606266;
    padding: 2px 8px;
    font-size: 12px;
    border-bottom-left-radius: 6px;
  }
  
  .sql-code {
    margin: 0;
    padding: 15px;
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace;
    font-size: 14px;
    line-height: 1.6;
    color: #333;
    white-space: pre-wrap;
    word-break: break-word;
    overflow-x: auto;
    
    code {
      color: #476582;
      padding: 0;
      background-color: transparent;
    }
  }
}

/* 普通消息样式 */
.normal-message {
  line-height: 1.6;
  color: #333;
  padding: 5px 0;
  font-size: 15px;
}

/* 格式化内容通用样式 */
.formatted-content {
  line-height: 1.6;
  color: #333;
  font-size: 15px;
  
  p {
    margin: 8px 0;
  }
  
  h4 {
    margin: 16px 0 8px;
    font-size: 16px;
    color: #303133;
    font-weight: 600;
  }
  
  .styled-list {
    margin: 8px 0;
    padding-left: 20px;
    
    li {
      margin: 6px 0;
      position: relative;
      padding-left: 4px;
      
      &::before {
        content: "•";
        position: absolute;
        left: -15px;
        color: #409eff;
      }
    }
  }
}

/* 分析文本样式 */
.analysis-text {
  background-color: #f9f9f9;
  border-left: 3px solid #409eff;
  padding: 10px 15px;
  border-radius: 0 6px 6px 0;
  margin: 10px 0;
}

/* 趋势和建议样式 */
.trends, .recommendations {
  margin: 15px 0;
  
  ul {
    margin-left: 0;
  }
}
</style> 