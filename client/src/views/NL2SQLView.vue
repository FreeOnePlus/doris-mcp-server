<template>
  <div class="nl2sql-view">
    <!-- 顶部标题栏 -->
    <div class="fixed-card-header">
      <div class="header">
        <h2>自然语言查询SQL</h2>
        <div class="header-actions">
          <el-button v-if="!mcpStore.isConnected" 
            type="primary" 
            size="small" 
            @click="mcpStore.connect()" 
          >
            连接服务器
          </el-button>
          <el-tag 
            :type="mcpStore.isConnected ? 'success' : 'danger'" 
            size="small"
          >
            {{ mcpStore.isConnected ? '已连接' : '未连接' }}
          </el-tag>
        </div>
      </div>
    </div>
    
    <!-- 中间聊天内容区域 - 只有这部分滚动 -->
    <div class="chat-container" ref="chatContainer">
      <div class="chat-content-wrapper">
        <div v-if="messages.length === 0" class="empty-chat">
          <el-empty description="暂无对话内容">
            <template #image>
              <img src="../assets/empty-chat.svg" alt="空对话" class="empty-image" v-if="false" />
            </template>
          </el-empty>
          <div class="empty-tip">
            <p>在输入框中输入您的自然语言查询，例如：</p>
            <p>"历史销量趋势怎么样？"</p>
            <p>"过去30天的每日销售额是多少？"</p>
          </div>
        </div>
        
        <transition-group name="fade" v-else>
          <div 
            v-for="(message, index) in messages" 
            :key="index" 
            :class="['message-container', message.role]"
          >
            <!-- 用户消息 -->
            <template v-if="message.role === 'user'">
              <div class="message user-message">
                <div class="message-content">
                  {{ message.content }}
                </div>
                <div class="message-timestamp" v-if="message.timestamp">
                  {{ formatTimestamp(message.timestamp) }}
                </div>
              </div>
            </template>
            
            <!-- 助手回复 -->
            <template v-else-if="message.role === 'assistant'">
              <div class="message assistant-message">
                <div class="message-content" v-if="!message.thinking">
                  <div v-if="message.error" class="error-message">
                    <el-icon><Warning /></el-icon> {{ message.error }}
                  </div>
                  <div v-else-if="message.result">
                    <div v-if="message.sql" class="sql-container">
                      <div class="sql-header">
                        <span>生成的SQL查询:</span>
                        <div class="sql-actions">
                          <el-button size="small" @click="copyToClipboard(message.sql)">
                            复制
                          </el-button>
                        </div>
                      </div>
                      <pre class="sql-code">{{ message.sql }}</pre>
                    </div>
                    
                    <!-- SQL查询结果显示 - 优先使用直接的result数组 -->
                    <div v-if="message.result && Array.isArray(message.result) && message.result.length > 0" class="data-result-container">
                      <div class="data-result-header">
                        <span>查询结果数据:</span>
                      </div>
                      <el-table
                        :data="message.result" 
                        style="width: 100%" 
                        border 
                        stripe
                        :max-height="300"
                      >
                        <el-table-column
                          v-for="(header, index) in Object.keys(message.result[0] || {})"
                          :key="index"
                          :prop="header"
                          :label="header"
                          :formatter="(row, column, cellValue, index) => formatCellValue(cellValue, header)"
                          show-overflow-tooltip
                        />
                      </el-table>
                    </div>
                    
                    <!-- 检查JSON字符串中是否包含result数组 -->
                    <div v-else-if="message.content && checkForResultArray(message.content)" class="data-result-container">
                      <div class="data-result-header">
                        <span>查询结果数据:</span>
                      </div>
                      <el-table
                        :data="extractResultArray(message.content)" 
                        style="width: 100%" 
                        border 
                        stripe
                        :max-height="300"
                      >
                        <el-table-column
                          v-for="(header, index) in getResultArrayHeaders(message.content)"
                          :key="index"
                          :prop="header"
                          :label="header"
                          :formatter="(row, column, cellValue, index) => formatCellValue(cellValue, header)"
                          show-overflow-tooltip
                        />
                      </el-table>
                    </div>
                    
                    <!-- 通用结果数组提取 -->
                    <div v-else-if="getMessageResultArray(message).length > 0" class="data-result-container">
                      <div class="data-result-header">
                        <span>查询结果数据:</span>
                      </div>
                      <el-table
                        :data="getMessageResultArray(message)" 
                        style="width: 100%" 
                        border 
                        stripe
                        :max-height="300"
                      >
                        <el-table-column
                          v-for="(header, index) in getMessageResultHeaders(message)"
                          :key="index"
                          :prop="header"
                          :label="header"
                          :formatter="(row, column, cellValue, index) => formatCellValue(cellValue, header)"
                          show-overflow-tooltip
                        />
                      </el-table>
                    </div>
                    
                    <div v-if="hasAnalysis(message)" class="analysis-container">
                      <div class="analysis-header">
                        <span>业务分析报告:</span>
                      </div>
                      <div class="analysis-content">
                        <p>{{ getAnalysisContent(message) }}</p>
                      </div>
                    </div>
                    
                    <!-- 直接从原始内容中提取的业务分析，用于处理JSON解析失败的情况 -->
                    <div v-else-if="extractRawBusinessAnalysis(message)" class="analysis-container partial-data">
                      <div class="analysis-header">
                        <span>业务分析报告 (部分内容):</span>
                        <el-tooltip content="原始数据格式有误，显示部分内容" placement="top">
                          <el-icon><Warning /></el-icon>
                        </el-tooltip>
                      </div>
                      <div class="analysis-content">
                        <p>{{ extractRawBusinessAnalysis(message) }}</p>
                      </div>
                    </div>
                    
                    <div v-if="message.trends && message.trends.length > 0" class="trends-container">
                      <div class="trends-header">
                        <span>数据趋势:</span>
                      </div>
                      <div class="trends-content">
                        <ul>
                          <li v-for="(trend, index) in message.trends" :key="index">
                            {{ trend }}
                          </li>
                        </ul>
                      </div>
                    </div>
                    
                    <div v-if="message.recommendations && message.recommendations.length > 0" class="recommendations-container">
                      <div class="recommendations-header">
                        <span>分析建议:</span>
                      </div>
                      <div class="recommendations-content">
                        <ul>
                          <li v-for="(recommendation, index) in message.recommendations" :key="index">
                            {{ recommendation }}
                          </li>
                        </ul>
                      </div>
                    </div>
                    
                    <div v-if="showDataTable(message)" class="data-table-container">
                      <div class="data-table-header">
                        <span>数据明细:</span>
                      </div>
                      <el-table
                        :data="getDataTableRows(message)" 
                        style="width: 100%" 
                        border 
                        stripe
                        :max-height="300"
                      >
                        <el-table-column
                          v-for="(header, index) in getDataTableHeaders(message)"
                          :key="index"
                          :prop="header"
                          :label="header"
                          :formatter="(row, column, cellValue, index) => formatCellValue(cellValue, header)"
                          show-overflow-tooltip
                        />
                      </el-table>
                    </div>
                    
                    <div v-if="hasChartOptions(message)" class="chart-container">
                      <div class="chart-header">
                        <span>数据可视化:</span>
                      </div>
                      <div 
                        :id="`chart-${message.id}`" 
                        class="chart-content" 
                        style="width: 100%; height: 400px;"
                        ref="chartRef"
                        v-once="initChart(message)"
                      ></div>
                    </div>
                  </div>
                </div>
                
                <!-- 思考过程展示 -->
                <div v-if="message.thinking" class="thinking-message">
                  <div class="deepersearch-thinking-process">
                    <!-- 左侧思考阶段列表 -->
                    <div class="left-stages">
                      <template v-for="(stage, index) in stageList" :key="index">
                        <div class="stage-item" :class="{
                          'active': isStageActive(stage.id),
                          'completed': isStageCompleted(stage.id)
                        }">
                          <div class="stage-number">{{ index + 1 }}</div>
                          <div class="stage-name">{{ stage.name }}</div>
                          <div v-if="mcpStore.stageTiming[stage.id]" class="stage-time">
                            {{ mcpStore.stageTiming[stage.id] }}s
                          </div>
                        </div>
                      </template>
                    </div>
                    
                    <!-- 右侧思考内容流 -->
                    <div class="thinking-content-stream" ref="thinkingStreamContent">
                      <div v-for="(line, lineIdx) in mcpStore.thinkingLines" :key="lineIdx" class="thinking-line" :class="{'stage-marker': line.startsWith('阶段:')}">
                        <pre v-if="line.includes('```') || line.includes('===') || line.includes('---')" 
                           class="formatted-code">{{ line }}</pre>
                        <span v-else v-html="formatThinkingLine(line)"></span>
                      </div>
                    </div>
                  </div>
                  
                  <div class="progress-container">
                    <div class="progress-bar">
                      <div 
                        class="progress-fill" 
                        :style="{ width: `${mcpStore.queryProgress}%` }"
                      ></div>
                    </div>
                    <div class="progress-label">
                      {{ mcpStore.currentStage ? mcpStore.getStageName(mcpStore.currentStage) : '处理中...' }}
                      <span v-if="mcpStore.queryProgress > 0">
                        ({{ Math.floor(mcpStore.queryProgress) }}%)
                      </span>
                    </div>
                  </div>
                </div>
                
                <div class="message-timestamp" v-if="message.timestamp">
                  {{ formatTimestamp(message.timestamp) }}
                </div>
              </div>
            </template>
          </div>
        </transition-group>
      </div>
    </div>
    
    <!-- 底部输入栏 -->
    <div class="chat-input-container">
      <div class="input-wrapper">
        <el-input
          v-model="userInput"
          :placeholder="inputPlaceholder"
          :disabled="isProcessing"
          @keyup.enter="submitQuery"
          class="query-input"
          clearable
          autofocus
        >
          <template #suffix>
            <el-icon v-if="isProcessing"><Loading /></el-icon>
          </template>
        </el-input>
        <el-button 
          type="primary" 
          :loading="isProcessing"
          @click="submitQuery"
          class="send-button"
        >
          <el-icon><Promotion /></el-icon>
        </el-button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue';
import { useRouter } from 'vue-router';
import { ElMessage } from 'element-plus';
import { useMCPStore } from '../stores/mcp';
import { useI18n } from '../i18n';
import { Check, Warning, Loading, Promotion, DataAnalysis } from '@element-plus/icons-vue';
import * as echarts from 'echarts';

const router = useRouter();
const mcpStore = useMCPStore();
const { t } = useI18n();
const chatContainer = ref(null);
const thinkingStreamContent = ref(null);
const userInput = ref('');
const isProcessing = ref(false);
const messages = ref([]);
const showSettings = ref(false);

// 定义查询处理阶段
const stageList = ref([
  { id: 'start', name: '开始处理查询' },
  { id: 'business_keyword_matching', name: '分析查询类型' },
  { id: 'builtin_keyword_matching', name: '关键词匹配' },
  { id: 'similar_query_search', name: '查找相似的查询示例' },
  { id: 'business_metadata', name: '获取业务元数据' },
  { id: 'sql_generation', name: '生成SQL查询' },
  { id: 'sql_fix', name: '修复SQL错误' },
  { id: 'result_analysis', name: '分析查询结果' },
  { id: 'sql_execution_complete', name: 'SQL执行完成' },
  { id: 'business_analysis', name: '业务分析与可视化' },
  { id: 'complete', name: '查询处理完成' }
]);

// 计算属性
const inputPlaceholder = computed(() => {
  if (isProcessing.value) return '处理中，请稍候...';
  if (!mcpStore.isConnected) return '连接中断，请重新连接...';
  return '输入您的自然语言查询...';
});

// 格式化思考行，处理Markdown格式
function formatThinkingLine(line) {
  // 使用简单的正则替换处理基本Markdown语法
  let formatted = line;
  
  // 处理粗体
  formatted = formatted.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  // 处理斜体
  formatted = formatted.replace(/\*(.*?)\*/g, '<em>$1</em>');
  // 处理代码片段
  formatted = formatted.replace(/`(.*?)`/g, '<code>$1</code>');
  // 处理SQL关键词高亮
  formatted = formatted.replace(/\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|HAVING|LIMIT|OFFSET|AND|OR|NOT|IN|BETWEEN|LIKE|AS|ON|WITH|UNION|ALL|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\b/gi, 
    (match) => `<span class="sql-keyword">${match}</span>`);
  
  // 返回格式化后的行
  return formatted;
}

// 处理NL2SQL事件
const handleNL2SQLEvent = (event) => {
  const { type, data } = event.detail;
  console.log(`处理NL2SQL事件: ${type}`, data);
  
  // 根据事件类型处理
  switch (type) {
    case 'thinking': 
      // 处理思考过程事件
      if (data.stage) {
        // 如果阶段变化，记录上一阶段的耗时
        if (mcpStore.currentStage && mcpStore.currentStage !== data.stage) {
          const endTime = Date.now();
          const startTime = mcpStore.stageStartTime[mcpStore.currentStage] || endTime;
          const duration = ((endTime - startTime) / 1000).toFixed(2); // 转换为秒，保留两位小数
          
          // 保存阶段耗时
          mcpStore.stageTiming[mcpStore.currentStage] = duration;
          console.log(`阶段 ${mcpStore.currentStage} 耗时: ${duration}秒`);
          
          // 添加阶段结束标记到思考内容
          mcpStore.thinkingLines.push(`阶段: ${mcpStore.getStageName(mcpStore.currentStage)} 完成 (${duration}秒)`);
        }
        
        // 记录新阶段的开始时间
        if (!mcpStore.stageStartTime[data.stage]) {
          mcpStore.stageStartTime[data.stage] = Date.now();
        }
        
        mcpStore.currentStage = data.stage;
        console.log(`[Stream] 设置当前阶段: ${data.stage}`);
        
        // 添加到阶段历史
        if (!mcpStore.stageHistory.includes(data.stage)) {
          mcpStore.stageHistory.push(data.stage);
          console.log(`[Stream] 添加到阶段历史: ${data.stage}`);
          
          // 添加阶段开始标记到思考内容
          mcpStore.thinkingLines.push(`阶段: ${mcpStore.getStageName(data.stage)} 开始处理`);
        }
      }
      
      if (data.progress !== undefined) {
        mcpStore.queryProgress = data.progress;
        console.log(`[Stream] 设置进度: ${data.progress}%`);
      }
      
      // 添加思考内容
      if (data.content) {
        mcpStore.thinkingProcess += data.content + '\n';
        mcpStore.thinkingLines.push(data.content);
        console.log(`[Stream] 添加思考内容: ${data.content}`);
        scrollToBottom();
      }
      break;
      
    case 'progress':
      // 处理进度事件
      console.log('处理进度事件:', data);
      if (data.stage) {
        mcpStore.currentStage = data.stage;
      }
      if (data.progress !== undefined) {
        mcpStore.queryProgress = data.progress;
      }
      break;
      
    case 'partial':
      // 处理部分结果
      console.log('处理部分结果:', data);
      // 可以在这里处理部分结果，例如预览
      break;
      
    case 'final': 
      // 处理最终结果
      console.log('处理最终结果事件:', data);
      
      // 查找最后一条消息
      let lastMessage = messages.value[messages.value.length - 1];
      
      // 如果最后一条消息不是助手消息，则不处理
      if (!lastMessage || lastMessage.role !== 'assistant') {
        console.log('没有找到可更新的助手消息');
        return;
      }
      
      // 如果最后一条消息仍在thinking状态，则更新它
      if (lastMessage.thinking) {
        console.log('更新思考中的消息为最终结果');
        
        // 获取结果数据 - 处理不同的数据结构
        let resultData = null;
        
        if (data.content) {
          // 格式1: {content: ...}
          resultData = data.content;
        } else if (data.result) {
          // 格式2: {result: ...}
          resultData = data.result;
        } else if (data.data) {
          // 格式3: {data: ...}
          resultData = data.data;
        } else {
          // 格式4: 直接是结果对象
          resultData = data;
        }
        
        console.log('解析的结果数据:', resultData);
        
        // 更新消息内容
        lastMessage.thinking = false;
        lastMessage.result = true;
        
        // 设置SQL (处理不同的数据结构)
        lastMessage.sql = extractField(resultData, data, 'sql');
        console.log('提取的SQL:', lastMessage.sql);
        
        // 设置查询结果
        if (resultData && (resultData.rows || (resultData.result && resultData.result.rows))) {
          // 从嵌套结构中提取headers和rows
          lastMessage.headers = resultData.headers || (resultData.result && resultData.result.headers) || [];
          lastMessage.rows = resultData.rows || (resultData.result && resultData.result.rows) || [];
          console.log('提取的查询结果:', lastMessage.headers, lastMessage.rows);
        } else if (data.headers && data.rows) {
          // 直接从data中获取
          lastMessage.headers = data.headers || [];
          lastMessage.rows = data.rows || [];
          console.log('从data中提取的查询结果:', lastMessage.headers, lastMessage.rows);
        }
        
        // 设置业务分析 (处理不同的数据结构)
        lastMessage.analysis = extractField(resultData, data, 'analysis');
        console.log('提取的业务分析:', lastMessage.analysis);
        
        // 从business_analysis中提取更详细的内容
        const businessAnalysis = extractField(resultData, data, 'business_analysis');
        if (businessAnalysis && typeof businessAnalysis === 'object') {
          console.log('提取business_analysis对象:', businessAnalysis);
          
          // 使用business_analysis中的详细分析替换简单分析
          if (businessAnalysis.business_analysis) {
            lastMessage.analysis = businessAnalysis.business_analysis;
            console.log('从business_analysis中更新分析:', lastMessage.analysis);
          }
          
          // 提取趋势数据
          if (businessAnalysis.trends) {
            lastMessage.trends = businessAnalysis.trends;
            console.log('从business_analysis中提取趋势:', lastMessage.trends);
          }
          
          // 提取建议
          if (businessAnalysis.recommendations) {
            lastMessage.recommendations = businessAnalysis.recommendations;
            console.log('从business_analysis中提取建议:', lastMessage.recommendations);
          }
          
          // 提取ECharts选项
          if (businessAnalysis.echarts_option) {
            lastMessage.echarts_option = businessAnalysis.echarts_option;
            console.log('从business_analysis中提取图表选项:', lastMessage.echarts_option);
          }
        }
        
        // 设置图表选项 (处理不同的数据结构)
        if (!lastMessage.echarts_option) {
          lastMessage.echarts_option = extractField(resultData, data, 'echarts_option');
          console.log('提取的图表选项:', lastMessage.echarts_option);
        }
        
        // 记录原始内容以备参考
        lastMessage.content = typeof data === 'object' ? JSON.stringify(data, null, 2) : data;
        
        // 记录查询类型
        if (resultData && resultData.result_type) {
          lastMessage.result_type = resultData.result_type;
        } else if (data && data.result_type) {
          lastMessage.result_type = data.result_type;
        } else if (lastMessage.analysis) {
          lastMessage.result_type = 'analysis';
        }
        
        // 设置查询
        if (resultData && resultData.query) {
          lastMessage.query = resultData.query;
        } else if (data && data.query) {
          lastMessage.query = data.query;
        }
        
        // 设置时间戳
        lastMessage.timestamp = new Date();
        console.log('更新的消息:', lastMessage);
        
        // 确保处理完成后重置isProcessing状态
        isProcessing.value = false;
        console.log('重置isProcessing状态为false');
      }
      break;
      
    case 'error':
      // 处理错误
      console.error('处理错误事件:', data);
      
      // 添加错误到思考记录
      mcpStore.thinkingLines.push(`处理出错: ${data.message || '未知错误'}`);
      
      // 结束思考状态
      isProcessing.value = false;
      
      // 如果有消息列表中最后一条是assistant且thinking为true，更新它为错误
      if (messages.value.length > 0) {
        const lastMessage = messages.value[messages.value.length - 1];
        if (lastMessage.role === 'assistant' && lastMessage.thinking) {
          lastMessage.thinking = false;
          lastMessage.error = data.message || '查询处理出错';
        }
      }
      break;
  }
};

// 添加和移除事件监听器
onMounted(() => {
  // 初始化思考行数组
  if (!mcpStore.thinkingLines) {
    mcpStore.thinkingLines = [];
  }
  
  // 初始化阶段计时数据
  if (!mcpStore.stageTiming) {
    mcpStore.stageTiming = {};
  }
  
  // 初始化阶段开始时间
  if (!mcpStore.stageStartTime) {
    mcpStore.stageStartTime = {};
  }
  
  // 连接到服务器
  mcpStore.connect().catch(error => {
    console.error('连接失败:', error);
    ElMessage.error('连接到服务器失败');
  });
  
  // 添加NL2SQL事件监听器
  console.log('添加NL2SQL事件监听器');
  window.addEventListener('nl2sql:event', handleNL2SQLEvent);
});

onBeforeUnmount(() => {
  // 移除NL2SQL事件监听器
  console.log('移除NL2SQL事件监听器');
  window.removeEventListener('nl2sql:event', handleNL2SQLEvent);
});

// 监听思考内容更新，自动滚动到底部
watch(() => mcpStore.thinkingProcess, (newContent, oldContent) => {
  if (newContent && newContent !== oldContent) {
    nextTick(() => {
      if (thinkingStreamContent.value) {
        thinkingStreamContent.value.scrollTop = thinkingStreamContent.value.scrollHeight;
      }
    });
  }
});

// 监听当前阶段变化
watch(() => mcpStore.currentStage, (newStage, oldStage) => {
  if (newStage !== oldStage) {
    console.log(`阶段变化: ${oldStage || 'none'} -> ${newStage}, 进度: ${mcpStore.queryProgress}%`);
    console.log(`阶段历史: ${mcpStore.stageHistory.join(', ')}`);
  }
});

// 监听消息变化，初始化图表
watch(() => messages.value, (newMessages) => {
  // 等待DOM更新后初始化图表
  nextTick(() => {
    newMessages.forEach(message => {
      if (hasChartOptions(message)) {
        const chartContainer = document.getElementById(`chart-${message.id}`);
        if (chartContainer) {
          console.log('初始化图表:', message.id);
          try {
            // 如果已经存在图表实例，先销毁
            if (chartInstances.value[message.id]) {
              chartInstances.value[message.id].dispose();
            }
            
            // 获取图表选项
            const options = getChartOptions(message);
            if (options) {
              console.log('图表选项:', options);
              // 创建新的图表实例
              const chart = echarts.init(chartContainer);
              chart.setOption(options);
              // 保存实例引用
              chartInstances.value[message.id] = chart;
              
              // 添加窗口大小变化监听
              window.addEventListener('resize', () => {
                chart.resize();
              });
              
              console.log('图表初始化成功:', message.id);
            } else {
              console.warn('无法获取图表选项:', message.id);
            }
          } catch (error) {
            console.error('初始化图表时出错:', error);
          }
        } else {
          console.warn(`未找到图表容器: chart-${message.id}`);
        }
      }
    });
  });
}, { deep: true });

// 判断某个阶段是否已完成
function isStageCompleted(stageId) {
  // 记录一下调试信息
  console.log(`检查阶段 ${stageId} 是否已完成, 阶段历史: ${mcpStore.stageHistory}`);
  
  // 阶段顺序
  const stageOrder = [
    'start',
    'business_keyword_matching',
    'builtin_keyword_matching',
    'similar_query_search',
    'business_metadata',
    'sql_generation',
    'sql_fix',
    'result_analysis',
    'sql_execution_complete',
    'business_analysis',
    'complete'
  ];
  
  // 查找当前阶段索引
  const currentStageIndex = stageOrder.indexOf(isStageActive(stageId) ? stageId : mcpStore.currentStage);
  const targetStageIndex = stageOrder.indexOf(stageId);
  
  // 如果目标阶段的索引小于当前阶段的索引，则表示该阶段已完成
  return targetStageIndex < currentStageIndex;
}

// 关键修复：正确判断阶段是否激活
function isStageActive(stageId) {
  // 记录一下调试信息
  console.log(`检查阶段 ${stageId} 是否激活, 当前阶段: ${mcpStore.currentStage}`);
  
  const currentStage = mcpStore.currentStage;
  
  // 阶段匹配映射关系
  const stageMapping = {
    'start': 'start',
    'analyzing': 'business_keyword_matching',
    'check_business_query': 'business_keyword_matching',
    'business_keyword_matching': 'business_keyword_matching',
    'builtin_keyword_matching': 'builtin_keyword_matching',
    'pattern_matching': 'builtin_keyword_matching',
    'similar_example': 'similar_query_search', 
    'similar_query_search': 'similar_query_search',
    'find_similar_example': 'similar_query_search',
    'business_metadata': 'business_metadata',
    'get_business_metadata': 'business_metadata',
    'sql_generation': 'sql_generation',
    'generate_sql': 'sql_generation',
    'generating': 'sql_generation',
    'thinking': 'sql_generation',
    'sql_fix': 'sql_fix',
    'executing': 'result_analysis',
    'result_analysis': 'result_analysis',
    'sql_execution_complete': 'sql_execution_complete',
    'business_analysis': 'business_analysis',
    'complete': 'complete',
    'error': 'error'
  };
  
  // 根据映射关系匹配当前阶段
  const mappedCurrentStage = stageMapping[currentStage] || currentStage;
  
  // 检查是否匹配目标阶段ID
  return mappedCurrentStage === stageId;
}

// 提交查询
const submitQuery = async () => {
  if (!userInput.value.trim() || isProcessing.value) return;
  
  try {
    // 添加用户消息
    const userMessage = {
      role: 'user',
      content: userInput.value.trim(),
      timestamp: new Date()
    };
    messages.value.push(userMessage);
    
    // 添加助手思考消息
    const assistantMessage = {
      role: 'assistant',
      thinking: true,
      timestamp: new Date()
    };
    messages.value.push(assistantMessage);
    
    // 开始处理
    isProcessing.value = true;
    
    // 清除之前的思考行
    mcpStore.clearThinkingState();
    
    console.log("使用流式查询API...");
    
    // 保存查询内容和清空输入框
    const queryText = userInput.value.trim();
    userInput.value = '';
    
    // 尝试使用直接流式API
    try {
      await mcpStore.nl2sqlQueryDirectStream(queryText);
      console.log("直接流式API调用成功");
    } catch (directError) {
      console.error("直接流式API调用失败:", directError);
      
      // 回退到工具流式API
      try {
        console.log("回退到工具流式API");
        await mcpStore.nl2sqlQueryStream(queryText);
        console.log("工具流式API调用成功");
      } catch (toolError) {
        console.error("工具流式API也失败:", toolError);
        
        // 如果两种流式API都失败，尝试使用普通API
        console.warn("回退到普通查询API");
        const result = await mcpStore.nl2sqlQuery(queryText);
        
        // 手动更新UI
        assistantMessage.thinking = false;
        assistantMessage.content = result.message || "查询处理完成";
        assistantMessage.result = result;
        assistantMessage.sql = result.sql || "";
        
        // 处理完成
        isProcessing.value = false;
      }
    }
    
    // 滚动到底部
    scrollToBottom();
  } catch (error) {
    console.error('处理查询时出错:', error);
    ElMessage.error('处理查询时发生错误');
    isProcessing.value = false;
  }
};

// 格式化时间戳
function formatTimestamp(timestamp) {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

// 复制到剪贴板
function copyToClipboard(text) {
  navigator.clipboard.writeText(text)
    .then(() => {
      ElMessage.success('已复制到剪贴板');
    })
    .catch(err => {
      console.error('复制失败:', err);
      ElMessage.error('复制失败');
    });
}

// 返回上一页
function goBack() {
  router.back();
}

// 处理流式思考和阶段进度
const handleStreamResponse = (data) => {
  console.log('收到流式响应: ', data);
  
  // 如果是思考事件
  if (data.type === 'thinking') {
    console.log('处理thinking事件:', data);
    
    // 更新思考内容
    if (data.content) {
      mcpStore.thinkingLines.push(data.content);
      console.log('添加思考行:', data.content);
      // 自动滚动到底部
      nextTick(() => {
        if (thinkingStreamContent.value) {
          thinkingStreamContent.value.scrollTop = thinkingStreamContent.value.scrollHeight;
          console.log('滚动到底部, 高度:', thinkingStreamContent.value.scrollHeight);
        }
      });
    }
    
    // 更新阶段信息
    if (data.stage) {
      mcpStore.currentStage = data.stage;
      
      // 如果不在历史中，则添加
      if (!mcpStore.stageHistory.includes(data.stage)) {
        mcpStore.stageHistory.push(data.stage);
        console.log('添加阶段到历史:', data.stage, '当前历史:', mcpStore.stageHistory);
      }
      
      // 更新进度
      if (data.progress !== undefined) {
        mcpStore.queryProgress = data.progress;
        console.log('更新进度:', data.progress);
      }
    }
  }
  
  // 处理最终结果
  if (data.type === 'final' || data.type === 'result') {
    console.log('处理最终结果:', data);
    isProcessing.value = false;
    mcpStore.isThinking = false;
    
    // 提取查询结果
    let result;
    if (data.content && typeof data.content === 'string') {
      try {
        result = JSON.parse(data.content);
        console.log('解析结果内容:', result);
      } catch (e) {
        console.error('解析结果JSON失败:', e);
        result = { text: data.content };
      }
    } else {
      console.log('使用原始结果数据');
      result = data;
    }
    
    // 创建新消息
    const newMessage = {
      id: Date.now(),
      role: 'assistant',
      content: result.message || '查询完成',
      data: result,
      timestamp: new Date()
    };
    
    messages.value.push(newMessage);
    console.log('添加结果消息:', newMessage);
  }
  
  // 处理错误
  if (data.type === 'error') {
    console.error('处理错误事件:', data);
    isProcessing.value = false;
    mcpStore.isThinking = false;
    
    // 创建错误消息
    const errorMessage = {
      id: Date.now(),
      role: 'assistant',
      content: `查询出错: ${data.content || '未知错误'}`,
      error: true,
      timestamp: new Date()
    };
    
    messages.value.push(errorMessage);
    console.log('添加错误消息:', errorMessage);
  }
};

// 滚动到底部
const scrollToBottom = () => {
  nextTick(() => {
    if (chatContainer.value) {
      chatContainer.value.scrollTop = chatContainer.value.scrollHeight;
    }
    
    if (thinkingStreamContent.value) {
      thinkingStreamContent.value.scrollTop = thinkingStreamContent.value.scrollHeight;
    }
  });
};

// 判断是否有业务分析
function hasAnalysis(message) {
  // 检查多种可能的路径
  const hasDirectAnalysis = Boolean(
    message.analysis ||
    (message.result && message.result.analysis) ||
    (message.result && message.result.result && message.result.result.analysis) ||
    (typeof message.result === 'object' && 'analysis' in message.result)
  );
  
  // 如果直接路径没有找到分析内容，尝试从content内容中解析
  if (!hasDirectAnalysis && message.content && typeof message.content === 'string') {
    const jsonObj = safeParseJSON(message.content);
    return Boolean(jsonObj && (jsonObj.analysis || jsonObj.business_analysis));
  }
  
  return hasDirectAnalysis;
}

// 从原始内容中提取业务分析文本(用于处理JSON解析失败的情况)
function extractRawBusinessAnalysis(message) {
  if (!message.content || typeof message.content !== 'string') return '';
  
  // 如果内容中包含business_analysis字段，尝试通过正则表达式提取内容
  if (message.content.includes('business_analysis')) {
    // 匹配 "business_analysis": "内容" 格式
    const match = message.content.match(/"business_analysis"\s*:\s*"([^"]*)/);
    if (match && match[1]) {
      return match[1] + '... (内容可能已截断)';
    }
    
    // 匹配 "business_analysis":{ 格式（嵌套对象）
    const nestedMatch = message.content.match(/"business_analysis"\s*:\s*\{([^}]*)/);
    if (nestedMatch && nestedMatch[1]) {
      // 提取嵌套对象中的文本
      const textMatch = nestedMatch[1].match(/"([^"]*)"/);
      if (textMatch && textMatch[1]) {
        return textMatch[1] + '... (内容可能已截断)';
      }
    }
  }
  
  return '';
}

// 获取业务分析内容
function getAnalysisContent(message) {
  // 先尝试直接获取
  if (message.analysis) {
    return message.analysis;
  } 
  
  // 从结果对象中获取
  if (message.result) {
    if (typeof message.result === 'object' && message.result.analysis) {
      return message.result.analysis;
    }
    
    if (message.result.result && message.result.result.analysis) {
      return message.result.result.analysis;
    }
  }
  
  // 尝试从content字符串中解析
  if (message.content && typeof message.content === 'string') {
    const jsonObj = safeParseJSON(message.content);
    if (jsonObj) {
      if (jsonObj.business_analysis) {
        return typeof jsonObj.business_analysis === 'string' 
          ? jsonObj.business_analysis 
          : jsonObj.business_analysis.business_analysis || '';
      }
      
      if (jsonObj.analysis) {
        return jsonObj.analysis;
      }
    }
    
    // 如果JSON解析失败，尝试直接提取business_analysis文本
    if (message.content.includes('"business_analysis"')) {
      const match = message.content.match(/"business_analysis"\s*:\s*"([^"]+)"/);
      if (match && match[1]) {
        return match[1] + '...'; // 添加省略号表示可能被截断
      }
    }
  }
  
  return '';
}

// 判断是否有可视化建议
function hasVisualization(message) {
  // 检查多种可能的路径
  return Boolean(
    message.visualization ||
    (message.result && message.result.visualization) ||
    (message.result && message.result.result && message.result.result.visualization) ||
    (typeof message.result === 'object' && 'visualization' in message.result)
  );
}

// 获取可视化建议内容
function getVisualizationContent(message) {
  if (message.visualization) {
    return message.visualization;
  } else if (message.result && message.result.visualization) {
    return message.result.visualization;
  } else if (message.result && message.result.result && message.result.result.visualization) {
    return message.result.result.visualization;
  } else if (typeof message.result === 'object' && 'visualization' in message.result) {
    return message.result.visualization;
  }
  return '';
}

// 添加辅助函数用于从多层嵌套对象中提取字段
function extractField(resultData, data, fieldName) {
  // 按优先级检查各种可能的路径
  if (resultData && resultData[fieldName]) {
    return resultData[fieldName];
  } else if (data && data[fieldName]) {
    return data[fieldName];
  } else if (resultData && resultData.result && resultData.result[fieldName]) {
    return resultData.result[fieldName];
  } else if (data && data.result && data.result[fieldName]) {
    return data.result[fieldName];
  } else if (data && data.data && data.data[fieldName]) {
    return data.data[fieldName];
  } else if (data && data.data && data.data.result && data.data.result[fieldName]) {
    return data.data.result[fieldName];
  }
  
  return null;
}

// 添加辅助函数检查是否有查询结果
function hasQueryResult(message) {
  // 检查各种可能包含查询结果的位置
  return (message.result && message.result.headers && message.result.rows) ||
         (message.result && message.result.result && message.result.result.headers && message.result.result.rows) ||
         (message.data && message.data.headers && message.data.rows) ||
         (message.result && message.result.data && message.result.data.headers && message.result.data.rows);
}

// 获取查询结果的表头
function getQueryHeaders(message) {
  if (message.result && message.result.headers) {
    return message.result.headers;
  }
  if (message.result && message.result.result && message.result.result.headers) {
    return message.result.result.headers;
  }
  if (message.data && message.data.headers) {
    return message.data.headers;
  }
  if (message.result && message.result.data && message.result.data.headers) {
    return message.result.data.headers;
  }
  return [];
}

// 获取查询结果的数据行
function getQueryRows(message) {
  if (message.result && message.result.rows) {
    return message.result.rows;
  }
  if (message.result && message.result.result && message.result.result.rows) {
    return message.result.result.rows;
  }
  if (message.data && message.data.rows) {
    return message.data.rows;
  }
  if (message.result && message.result.data && message.result.data.rows) {
    return message.result.data.rows;
  }
  return [];
}

// 判断是否有图表选项
function hasChartOptions(message) {
  return Boolean(
    message.echarts_option ||
    (message.result && message.result.echarts_option) ||
    (message.result && message.result.result && message.result.result.echarts_option) ||
    (typeof message.result === 'object' && 'echarts_option' in message.result)
  );
}

// 获取图表选项
function getChartOptions(message) {
  if (message.echarts_option) {
    return message.echarts_option;
  } else if (message.result && message.result.echarts_option) {
    return message.result.echarts_option;
  } else if (message.result && message.result.result && message.result.result.echarts_option) {
    return message.result.result.echarts_option;
  } else if (typeof message.result === 'object' && 'echarts_option' in message.result) {
    return message.result.echarts_option;
  }
  return null;
}

// 渲染图表的函数
function renderChart(element, options) {
  if (!element || !options) return;
  
  const chart = echarts.init(element);
  chart.setOption(options);
  
  // 自适应窗口大小变化
  window.addEventListener('resize', () => {
    chart.resize();
  });
  
  return chart;
}

// 用于存储图表实例的引用
const chartInstances = ref({});

// 在组件销毁时清理图表实例
onBeforeUnmount(() => {
  Object.values(chartInstances.value).forEach(chart => {
    if (chart) {
      chart.dispose();
    }
  });
});

// 显示明细数据表格，当result对象中包含数据时
function showDataTable(message) {
  console.log('检查是否显示数据表格，message:', message);
  
  // 直接检查message.result字段是否为数组
  if (message.result && Array.isArray(message.result) && message.result.length > 0) {
    console.log('检测到直接的result数组数据:', message.result.length);
    return true;
  }
  
  // 首先检查是否有数据行
  const rows = getDataTableRows(message);
  if (rows.length > 0) {
    console.log('发现数据行:', rows.length);
    // 检查第一行是否是对象且有值
    if (typeof rows[0] === 'object' && rows[0] !== null && Object.keys(rows[0]).length > 0) {
      return true;
    }
  }
  
  // 检查是否是年报告类查询，这类查询通常有意义的数据表
  if (message.query && /年|销售额|趋势|对比|报表|表格/.test(message.query)) {
    const result = message.result;
    if (result && result.result && Array.isArray(result.result) && result.result.length > 0) {
      console.log('检测到结果数组数据:', result.result.length);
      return true;
    }
  }
  
  return false;
}

// 获取数据明细表格的表头
function getDataTableHeaders(message) {
  console.log('获取数据表格表头，message:', message);
  
  // 直接检查message.result字段是否为数组
  if (message.result && Array.isArray(message.result) && message.result.length > 0) {
    console.log('从直接的result数组中提取表头');
    const firstRow = message.result[0];
    if (typeof firstRow === 'object' && firstRow !== null) {
      const headers = Object.keys(firstRow);
      console.log('从result数组中提取的表头:', headers);
      return headers;
    }
  }
  
  // 先尝试从message.result.result中提取数据
  const result = message.result;
  if (result && result.result && Array.isArray(result.result) && result.result.length > 0) {
    console.log('从result.result中提取表头');
    const firstRow = result.result[0];
    if (typeof firstRow === 'object') {
      return Object.keys(firstRow);
    }
  }
  
  // 再尝试从数据行中提取表头
  const firstRow = getDataTableRows(message)[0] || {};
  if (typeof firstRow === 'object' && firstRow !== null) {
    console.log('从首行数据提取表头:', Object.keys(firstRow));
    return Object.keys(firstRow);
  }
  
  return [];
}

// 获取数据明细表格的数据行
function getDataTableRows(message) {
  console.log('获取数据表格行数据，message:', message);
  
  // 直接检查message.result字段，如果它是数组，直接使用
  if (message.result && Array.isArray(message.result)) {
    console.log('直接从message.result获取数组数据:', message.result);
    return message.result;
  }
  
  // 检查是否有结果字段中的数组数据
  const result = message.result;
  
  if (result) {
    // 检查result是否直接是数组
    if (Array.isArray(result)) {
      return result;
    }
    
    // 检查result.result数组
    if (result.result && Array.isArray(result.result)) {
      return result.result;
    }
    
    // 检查业务分析结果中的图表数据
    if (result.business_analysis && result.business_analysis.echarts_option) {
      const chartOption = result.business_analysis.echarts_option;
      // 尝试从图表数据中提取
      if (chartOption && chartOption.series && chartOption.series[0] && chartOption.series[0].data) {
        const seriesData = chartOption.series[0].data;
        // 如果数据是对象数组，可以直接使用
        if (Array.isArray(seriesData) && typeof seriesData[0] === 'object') {
          return seriesData;
        }
        
        // 如果是简单数组，尝试和xAxis结合生成数据
        if (Array.isArray(seriesData) && chartOption.xAxis && chartOption.xAxis.data) {
          const xAxisData = chartOption.xAxis.data;
          return xAxisData.map((x, index) => {
            return {
              '分类': x,
              '数值': seriesData[index]
            };
          });
        }
      }
    }
  }
  
  // 检查直接包含result字段的数组数据
  if (message.data && Array.isArray(message.data)) {
    return message.data;
  }
  
  // 如果上述都不满足，最后尝试从查询结果中构造
  const rows = getQueryRows(message);
  const headers = getQueryHeaders(message);
  
  if (rows.length > 0 && headers.length > 0) {
    return rows.map(row => {
      const obj = {};
      headers.forEach((header, index) => {
        obj[header] = Array.isArray(row) ? row[index] : row[header];
      });
      return obj;
    });
  }
  
  return [];
}

// 检测是否应该显示图表占位符
function shouldShowChartPlaceholder(message) {
  // 满足以下条件时显示图表占位符：
  // 1. 有查询结果
  // 2. 没有图表选项
  // 3. 查询涉及数据分析或包含一定数量的数据行(可能适合可视化)
  
  const hasData = hasQueryResult(message) && getQueryRows(message).length > 0;
  const isAnalysisQuery = hasAnalysis(message) || 
                         (message.result_type === 'analysis') ||
                         (message.query && /分析|趋势|比较|占比|排名|TOP|统计|可视化|图表/.test(message.query));
  
  return hasData && isAnalysisQuery && !hasChartOptions(message);
}

// 初始化图表
function initChart(message) {
  if (!hasChartOptions(message)) return;
  
  // 在nextTick中执行，确保DOM已更新
  nextTick(() => {
    const chartId = `chart-${message.id}`;
    const chartContainer = document.getElementById(chartId);
    if (!chartContainer) {
      console.warn(`未找到图表容器: ${chartId}`);
      return;
    }
    
    try {
      // 如果已经存在图表实例，先销毁
      if (chartInstances.value[message.id]) {
        chartInstances.value[message.id].dispose();
      }
      
      // 获取图表选项
      const options = getChartOptions(message);
      if (!options) {
        console.warn('图表选项为空:', message.id);
        return;
      }
      
      console.log('初始化图表:', message.id, options);
      
      // 创建新的图表实例
      const chart = echarts.init(chartContainer);
      chart.setOption(options);
      
      // 保存实例引用
      chartInstances.value[message.id] = chart;
      
      // 添加窗口大小变化监听
      window.addEventListener('resize', () => {
        if (chart) {
          chart.resize();
        }
      });
      
      console.log('图表初始化成功:', message.id);
    } catch (error) {
      console.error('初始化图表时出错:', error);
    }
  });
}

// 添加辅助函数，处理可能存在的格式错误JSON
function safeParseJSON(str) {
  if (typeof str !== 'string') return null;
  
  try {
    // 尝试直接解析
    return JSON.parse(str);
  } catch (e) {
    console.log('JSON直接解析失败:', e);
    
    try {
      // 检查是否有 ```json 标记
      if (str.includes('```json')) {
        const jsonStart = str.indexOf('```json') + 7;
        const jsonEnd = str.indexOf('```', jsonStart);
        let jsonStr = jsonEnd > jsonStart ? str.substring(jsonStart, jsonEnd).trim() : str.substring(jsonStart).trim();
        
        // 尝试修复截断的JSON
        if (!jsonStr.endsWith('}')) {
          jsonStr = fixTruncatedJSON(jsonStr);
        }
        
        return JSON.parse(jsonStr);
      }
      
      // 检查是否是普通JSON但被截断
      if (str.startsWith('{') && !str.endsWith('}')) {
        const fixedStr = fixTruncatedJSON(str);
        return JSON.parse(fixedStr);
      }
    } catch (innerError) {
      console.log('修复JSON后解析仍然失败:', innerError);
    }
  }
  
  return null;
}

// 尝试修复被截断的JSON
function fixTruncatedJSON(str) {
  if (!str) return '{}';
  
  let openBraces = (str.match(/{/g) || []).length;
  let closeBraces = (str.match(/}/g) || []).length;
  
  // 计算需要添加的闭合大括号数量
  let diff = openBraces - closeBraces;
  
  if (diff > 0) {
    // 添加缺失的闭合大括号
    return str + '}'.repeat(diff);
  }
  
  return str;
}

// 修改checkForResultArray函数，使用safeParseJSON
function checkForResultArray(content) {
  if (typeof content !== 'string') return false;
  
  const jsonObj = safeParseJSON(content);
  if (!jsonObj) return false;
  
  // 检查直接的result字段
  if (jsonObj.result && Array.isArray(jsonObj.result) && jsonObj.result.length > 0) {
    return true;
  }
  
  // 检查嵌套的result.result字段
  if (jsonObj.result && jsonObj.result.result && Array.isArray(jsonObj.result.result) && jsonObj.result.result.length > 0) {
    return true;
  }
  
  // 检查data.result字段
  if (jsonObj.data && jsonObj.data.result && Array.isArray(jsonObj.data.result) && jsonObj.data.result.length > 0) {
    return true;
  }
  
  return false;
}

// 修改extractResultArray函数，使用safeParseJSON
function extractResultArray(content) {
  if (typeof content !== 'string') return [];
  
  const jsonObj = safeParseJSON(content);
  if (!jsonObj) return [];
  
  // 检查直接的result字段
  if (jsonObj.result && Array.isArray(jsonObj.result) && jsonObj.result.length > 0) {
    console.log('从content的result字段提取到数组:', jsonObj.result);
    return jsonObj.result;
  }
  
  // 检查嵌套的result.result字段
  if (jsonObj.result && jsonObj.result.result && Array.isArray(jsonObj.result.result) && jsonObj.result.result.length > 0) {
    console.log('从content的result.result字段提取到数组:', jsonObj.result.result);
    return jsonObj.result.result;
  }
  
  // data.result字段
  if (jsonObj.data && jsonObj.data.result && Array.isArray(jsonObj.data.result) && jsonObj.data.result.length > 0) {
    console.log('从content的data.result字段提取到数组:', jsonObj.data.result);
    return jsonObj.data.result;
  }
  
  return [];
}

// 获取result数组的表头
function getResultArrayHeaders(content) {
  if (typeof content !== 'string') return [];
  
  const jsonObj = safeParseJSON(content);
  if (!jsonObj) return [];
  
  let resultArray = null;
  
  // 检查直接的result字段
  if (jsonObj.result && Array.isArray(jsonObj.result) && jsonObj.result.length > 0) {
    resultArray = jsonObj.result;
  }
  // 检查嵌套的result.result字段
  else if (jsonObj.result && jsonObj.result.result && Array.isArray(jsonObj.result.result) && jsonObj.result.result.length > 0) {
    resultArray = jsonObj.result.result;
  }
  // 检查data.result字段
  else if (jsonObj.data && jsonObj.data.result && Array.isArray(jsonObj.data.result) && jsonObj.data.result.length > 0) {
    resultArray = jsonObj.data.result;
  }
  
  if (resultArray && resultArray.length > 0) {
    const firstItem = resultArray[0];
    if (typeof firstItem === 'object' && firstItem !== null) {
      const headers = Object.keys(firstItem);
      console.log('提取到表头:', headers);
      return headers;
    }
  }
  
  return [];
}

// 格式化单元格值
function formatCellValue(value, columnName) {
  if (value === undefined || value === null) return '-';
  
  // 检查列名是否包含特定关键词，如total_sales, revenue, amount等
  const isCurrencyColumn = /sales|revenue|amount|金额|销售额|价格/.test(columnName.toLowerCase());
  
  // 如果是销售额列且是数字，使用货币格式
  if (isCurrencyColumn && typeof value === 'number') {
    return new Intl.NumberFormat('zh-CN', { 
      style: 'currency',
      currency: 'CNY',
      notation: value > 1000000 ? 'compact' : 'standard',
      maximumFractionDigits: 2
    }).format(value);
  }
  
  // 如果是年份列，确保显示完整年份
  if (columnName.toLowerCase() === 'year' || columnName === '年份') {
    return value.toString();
  }
  
  // 如果是大数字但不是货币，使用数字格式化
  if (typeof value === 'number' && value > 10000) {
    return new Intl.NumberFormat('zh-CN', {
      notation: 'compact',
      maximumFractionDigits: 2
    }).format(value);
  }
  
  return value;
}

// 从message对象中获取result数组
function getMessageResultArray(message) {
  console.log('尝试从message中提取result数组');
  
  // 检查各种可能的路径
  
  // 直接的result数组
  if (message.result && Array.isArray(message.result) && message.result.length > 0) {
    console.log('从message.result提取到数组，长度:', message.result.length);
    return message.result;
  }
  
  // 嵌套的result.result数组
  if (message.result && message.result.result && Array.isArray(message.result.result) && message.result.result.length > 0) {
    console.log('从message.result.result提取到数组，长度:', message.result.result.length);
    return message.result.result;
  }
  
  // data字段中的result数组
  if (message.data && message.data.result && Array.isArray(message.data.result) && message.data.result.length > 0) {
    console.log('从message.data.result提取到数组，长度:', message.data.result.length);
    return message.data.result;
  }
  
  // 从content字符串中解析
  if (message.content && typeof message.content === 'string') {
    const jsonObj = safeParseJSON(message.content);
    if (jsonObj) {
      // 直接的result字段
      if (jsonObj.result && Array.isArray(jsonObj.result) && jsonObj.result.length > 0) {
        console.log('从content的result字段解析出数组，长度:', jsonObj.result.length);
        return jsonObj.result;
      }
      
      // 嵌套的result.result字段
      if (jsonObj.result && jsonObj.result.result && Array.isArray(jsonObj.result.result) && jsonObj.result.result.length > 0) {
        console.log('从content的result.result字段解析出数组，长度:', jsonObj.result.result.length);
        return jsonObj.result.result;
      }
      
      // data.result字段
      if (jsonObj.data && jsonObj.data.result && Array.isArray(jsonObj.data.result) && jsonObj.data.result.length > 0) {
        console.log('从content的data.result字段解析出数组，长度:', jsonObj.data.result.length);
        return jsonObj.data.result;
      }
    }
  }
  
  return [];
}

// 从message对象中获取result数组的表头
function getMessageResultHeaders(message) {
  const resultArray = getMessageResultArray(message);
  if (resultArray.length > 0) {
    const firstItem = resultArray[0];
    if (typeof firstItem === 'object' && firstItem !== null) {
      const headers = Object.keys(firstItem);
      console.log('从result数组中提取到表头:', headers);
      return headers;
    }
  }
  return [];
}
</script>

<style lang="scss" scoped>
/* 主容器 */
.nl2sql-view {
  position: fixed; /* 整个容器固定到视口 */
  top: 56px; /* 给上方的全局导航留空间 */
  bottom: 0;
  left: 0;
  right: 0;
  background-color: #f5f7fa;
  overflow: hidden; /* 禁止整体滚动 */
  display: flex;
  flex-direction: column;
}

/* 顶部标题栏 */
.fixed-card-header {
  flex-shrink: 0;
  background-color: #fff;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  border-bottom: 1px solid #ebeef5;
  z-index: 100;
  
  .header {
    width: 60%;
    margin: 0 auto;
    padding: 15px 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
    
    h2 {
      margin: 0;
      font-size: 18px;
      color: #303133;
    }
    
    .header-actions {
      display: flex;
      gap: 10px;
      align-items: center;
    }
  }
}

/* 中间聊天内容区域 - 仅内部可滚动 */
.chat-container {
  flex: 1; /* 占据剩余空间 */
  position: relative;
  overflow: hidden; /* 确保容器本身不滚动 */
  
  .chat-content-wrapper {
    width: 60%;
    margin: 0 auto;
    height: 100%; /* 高度100%填充父容器 */
    background-color: #fff;
    overflow-y: auto; /* 只有内容区可滚动 */
    padding: 20px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    border-left: 1px solid #e4e7ed;
    border-right: 1px solid #e4e7ed;
  }
}

/* 底部输入区域 */
.chat-input-container {
  flex-shrink: 0;
  background-color: #fff;
  border-top: 1px solid #e4e7ed;
  padding: 15px 0;
  z-index: 100;
  
  .input-wrapper {
    width: 60%;
    margin: 0 auto;
    display: flex;
    align-items: center;
    
    .query-input {
      flex: 1;
    }
    
    .send-button {
      margin-left: 10px;
    }
  }
}

/* 淡入动画 */
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.3s;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
}

/* 加载动画 */
@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* 响应式调整 */
@media (max-width: 768px) {
  .nl2sql-view {
    .deepersearch-thinking-process {
      flex-direction: column;
      
      .thinking-stages {
        width: 100% !important;
        border-right: none !important;
        border-bottom: 1px solid #e4e7ed;
      }
    }
  }
}

.empty-chat {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 400px;
  color: #909399;
  
  .el-empty {
    padding: 40px 0;
    
    .el-empty__image {
      width: 160px;
      height: 160px;
    }
    
    .el-empty__description {
      margin-top: 20px;
      font-size: 16px;
      color: #909399;
    }
  }
  
  .empty-tip {
    margin-top: 20px;
    text-align: center;
    
    p {
      margin: 8px 0;
      color: #909399;
      font-size: 14px;
    }
  }
}

.message-container {
  margin-bottom: 20px;
  
  &.user {
    display: flex;
    justify-content: flex-end;
  }
  
  .message {
    max-width: 80%;
    padding: 12px 16px;
    border-radius: 8px;
    
    &.user-message {
      background-color: #409eff;
      color: white;
      align-self: flex-end;
      border-bottom-right-radius: 0;
    }
    
    &.assistant-message {
      background-color: #fff;
      border: 1px solid #e4e7ed;
      box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.05);
      border-bottom-left-radius: 0;
      
      .message-content {
        margin-bottom: 8px;
        
        .result-text {
          margin-bottom: 16px;
          padding: 8px 0;
          color: #303133;
          line-height: 1.5;
          font-size: 15px;
        }
        
        .error-message {
          color: #f56c6c;
          padding: 8px 12px;
          background-color: #fef0f0;
          border-radius: 4px;
          display: flex;
          align-items: center;
          
          .el-icon {
            margin-right: 8px;
          }
        }
      }
      
      .sql-container {
        margin-top: 12px;
        margin-bottom: 16px;
        background-color: #f9f9f9;
        border-radius: 6px;
        padding: 2px;
        
        .sql-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 8px 12px;
          background-color: #f0f2f5;
          border-top-left-radius: 6px;
          border-top-right-radius: 6px;
          border-bottom: 1px solid #e4e7ed;
          font-weight: 500;
          color: #606266;
        }
        
        .sql-code {
          background-color: #f5f7fa;
          padding: 12px;
          border-radius: 4px;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: 'Courier New', monospace;
          font-size: 14px;
          line-height: 1.4;
          max-height: 300px;
          overflow-y: auto;
          margin: 0;
        }
      }
      
      .result-table {
        margin-top: 16px;
        border-radius: 6px;
        overflow: hidden;
        box-shadow: 0 1px 4px rgba(0, 0, 0, 0.1);
        
        .table-header {
          padding: 10px 12px;
          background-color: #f0f2f5;
          border-bottom: 1px solid #e4e7ed;
          font-weight: 500;
          color: #606266;
        }
      }
      
      .text-result {
        margin-top: 12px;
        padding: 12px;
        background-color: #f5f7fa;
        border-radius: 4px;
        border: 1px solid #e4e7ed;
        white-space: pre-wrap;
        word-break: break-word;
        font-size: 14px;
        line-height: 1.5;
      }
    }
    
    .message-timestamp {
      font-size: 12px;
      color: #909399;
      margin-top: 4px;
      text-align: right;
    }
  }
  
  .thinking-message {
    width: 100%;
    
    .deepersearch-thinking-process {
      display: flex;
      background-color: #f9fafc;
      border-radius: 8px;
      margin-bottom: 16px;
      border: 1px solid #e4e7ed;
      overflow: hidden;
      
      .left-stages {
        width: 240px;
        padding: 16px 12px;
        background-color: #f0f2f5;
        border-right: 1px solid #e4e7ed;
        
        .stage-item {
          display: flex;
          align-items: center;
          margin-bottom: 16px;
          padding: 8px 10px;
          border-radius: 4px;
          transition: all 0.3s;
          position: relative;
          
          &:last-child {
            margin-bottom: 0;
          }
          
          &.active {
            color: #409eff;
            background-color: rgba(64, 158, 255, 0.1);
            font-weight: 500;
            
            .stage-number {
              border-color: #409eff;
              background-color: #409eff;
              color: white;
              position: relative;
              overflow: hidden;
            }
            
            .stage-time {
              color: #409eff;
            }
            
            /* 添加脉冲动画效果 */
            &::after {
              content: '';
              position: absolute;
              top: 0;
              left: 0;
              width: 100%;
              height: 100%;
              background-color: rgba(64, 158, 255, 0.2);
              border-radius: 4px;
              animation: pulse 1.5s infinite;
            }
          }
          
          &.completed {
            color: #67c23a;
            
            .stage-number {
              background-color: #67c23a;
              border-color: #67c23a;
              color: white;
              position: relative;
            }
            
            .stage-time {
              color: #67c23a;
            }
            
            /* 添加完成图标 */
            .stage-number::after {
              content: '✓';
              position: absolute;
              top: 50%;
              left: 50%;
              transform: translate(-50%, -50%);
              font-size: 14px;
            }
          }
          
          .stage-number {
            width: 24px;
            height: 24px;
            border-radius: 50%;
            border: 1px solid #dcdfe6;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-right: 10px;
            background-color: white;
            font-size: 12px;
            transition: all 0.3s;
          }
          
          .stage-name {
            font-size: 14px;
            flex: 1;
          }
          
          .stage-time {
            font-size: 12px;
            color: #909399;
            margin-left: 5px;
            min-width: 40px;
            text-align: right;
          }
        }
      }
      
      .thinking-content-stream {
        flex: 1;
        padding: 16px;
        max-height: 300px;
        overflow-y: auto;
        font-family: system-ui, -apple-system, sans-serif;
        
        .thinking-line {
          margin-bottom: 8px;
          line-height: 1.5;
          color: #606266;
          font-size: 14px;
          
          &.stage-marker {
            color: #409eff;
            font-weight: 500;
            padding: 5px 0;
            border-bottom: 1px dashed #dcdfe6;
            margin-top: 10px;
            margin-bottom: 10px;
          }
          
          pre.formatted-code {
            background-color: #f5f7fa;
            border-radius: 4px;
            border: 1px solid #e4e7ed;
            padding: 10px;
            margin: 8px 0;
            white-space: pre-wrap;
            word-break: break-word;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            line-height: 1.4;
            overflow-x: auto;
          }
          
          code {
            background-color: #f0f2f5;
            border-radius: 3px;
            padding: 2px 4px;
            font-family: 'Courier New', monospace;
            font-size: 90%;
          }
          
          .sql-keyword {
            color: #409eff;
            font-weight: 500;
          }
        }
      }
    }
    
    .progress-container {
      margin-top: 8px;
      
      .progress-bar {
        height: 6px;
        background-color: #ebeef5;
        border-radius: 3px;
        overflow: hidden;
        
        .progress-fill {
          height: 100%;
          background-color: #409eff;
          transition: width 0.3s ease;
        }
      }
      
      .progress-label {
        text-align: center;
        font-size: 12px;
        color: #909399;
        margin-top: 4px;
      }
    }
  }
}

/* 添加脉冲动画 */
@keyframes pulse {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.5;
    transform: scale(1.02);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}

/* 业务分析容器 */
.analysis-container, .visualization-container, .trends-container, .recommendations-container {
  margin-top: 15px;
  padding: 15px;
  background-color: #f5f7fa;
  border-radius: 5px;
  border-left: 4px solid #409EFF;
}

.analysis-header, .visualization-header, .trends-header, .recommendations-header {
  font-weight: bold;
  margin-bottom: 10px;
  color: #303133;
  font-size: 0.95rem;
}

.analysis-content, .visualization-content {
  color: #606266;
  line-height: 1.6;
  white-space: pre-line;
}

.trends-content, .recommendations-content {
  color: #606266;
  line-height: 1.6;
  
  ul {
    padding-left: 20px;
    margin: 10px 0;
    
    li {
      margin-bottom: 8px;
    }
  }
}

.chart-container {
  margin-top: 16px;
  border-radius: 6px;
  overflow: hidden;
  background-color: #fff;
  border: 1px solid #e4e7ed;
  
  .chart-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 12px;
    background-color: #ecf5ff;
    border-bottom: 1px solid #e4e7ed;
    font-weight: 500;
    color: #409eff;
  }
  
  .chart-content {
    height: 400px;
    width: 100%;
    padding: 12px;
  }
}

.empty-chart-container {
  margin-top: 16px;
  padding: 30px 20px;
  background-color: #f9fafc;
  border: 1px solid #e4e7ed;
  border-radius: 6px;
  text-align: center;
  
  .empty-chart-message {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: #909399;
    
    .el-icon {
      font-size: 48px;
      margin-bottom: 16px;
      color: #c0c4cc;
    }
    
    span {
      font-size: 14px;
      line-height: 1.5;
      max-width: 80%;
    }
  }
}

.query-result {
  margin-top: 16px;
  padding: 15px;
  background-color: #f5f7fa;
  border-radius: 5px;
  border: 1px solid #e4e7ed;
}

.data-table-container {
  margin-top: 16px;
  padding: 15px;
  background-color: #f5f7fa;
  border-radius: 5px;
  border: 1px solid #e4e7ed;
}

.data-table-header {
  font-weight: bold;
  margin-bottom: 10px;
  color: #303133;
  font-size: 0.95rem;
}

.data-result-container {
  margin-top: 16px;
  padding: 15px;
  background-color: #f5f7fa;
  border-radius: 5px;
  border: 1px solid #e4e7ed;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.05);
}

.data-result-header {
  font-weight: bold;
  margin-bottom: 10px;
  color: #303133;
  font-size: 0.95rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 8px;
  border-bottom: 1px solid #ebeef5;
}

.partial-data {
  position: relative;
  border-left: 4px solid #E6A23C;
  
  .analysis-header {
    color: #E6A23C;
    
    .el-icon {
      color: #E6A23C;
      margin-left: 5px;
      cursor: help;
    }
  }
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: repeating-linear-gradient(
      45deg,
      rgba(230, 162, 60, 0.05),
      rgba(230, 162, 60, 0.05) 10px,
      rgba(230, 162, 60, 0.1) 10px,
      rgba(230, 162, 60, 0.1) 20px
    );
    opacity: 0.5;
    pointer-events: none;
    border-radius: 5px;
  }
}
</style> 