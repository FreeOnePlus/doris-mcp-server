import { defineStore } from 'pinia';
import { ref, computed } from 'vue';
import { MCPClient } from '../utils/mcp-client';
import { ElMessage } from 'element-plus';

export const useMCPStore = defineStore('mcp', () => {
  // 状态
  const client = ref(null);
  const isConnected = ref(false);
  const isConnecting = ref(false);
  const tools = ref([]);
  const resources = ref([]);
  const prompts = ref([]);
  const connectionError = ref(null);
  
  // 流式响应状态
  const thinkingProcess = ref('');
  const thinkingLines = ref([]); // 添加思考内容分行存储数组
  const queryProgress = ref(0);
  const isThinking = ref(false);
  const currentStage = ref('waiting');
  const stageHistory = ref([]);  // 阶段历史记录
  const statusPollingEnabled = ref(false); // 是否启用状态轮询
  const statusPollingInterval = ref(null); // 状态轮询定时器
  const pollingFrequency = 1000; // 轮询频率（毫秒）
  const is_processing = ref(false); // 是否正在处理查询
  const current_query = ref(''); // 当前正在处理的查询
  const stageTiming = ref({}); // 存储各阶段耗时
  const stageStartTime = ref({}); // 存储各阶段开始时间
  const stageLabels = {
    'start': '初始化',
    'analyzing': '业务查询判断阶段',
    'metadata': '元数据处理阶段',
    'similar_example': '相似示例查找阶段',
    'business_metadata': '业务元数据分析阶段',
    'thinking_start': 'SQL生成准备阶段',
    'thinking': 'SQL生成阶段',
    'generating': 'SQL生成阶段',
    'executing': 'SQL执行阶段',
    'sql_generated': 'SQL已生成',
    'execute_sql': 'SQL执行阶段',
    'generate_sql': 'SQL生成阶段',
    'check_business_query': '业务查询判断阶段',
    'find_similar_example': '相似示例查找阶段',
    'get_business_metadata': '业务元数据分析阶段',
    'analyze_business': '业务分析阶段',
    'fix_sql': 'SQL修复阶段',
    'complete': '完成',
    'error': '错误'
  };
  
  // 获取环境变量配置
  const mcpHost = import.meta.env.VITE_MCP_HOST || 'localhost';
  const mcpPort = import.meta.env.VITE_MCP_PORT || '3000';
  const mcpProtocol = import.meta.env.VITE_MCP_PROTOCOL || 'http';
  
  // 计算属性
  const available = computed(() => isConnected.value && client.value !== null);
  
  // 获取阶段显示名称
  function getStageName(stage) {
    // 简化后的阶段映射，每个显示名称只保留一个代表性的阶段ID
    const stageMap = {
      'waiting': '等待处理',
      'start': '开始处理查询',
      'business_keyword_matching': '分析查询类型',
      'builtin_keyword_matching': '关键词匹配',
      'similar_query_search': '查找相似的查询示例',
      'business_metadata': '获取业务元数据',
      'sql_generation': '生成SQL查询',
      'sql_fix': '修复SQL错误',
      'executing': '执行SQL查询',
      'result_analysis': '分析查询结果',
      'sql_execution_complete': 'SQL执行完成',
      'business_analysis': '业务分析',
      'visualization': '数据可视化',
      'complete': '查询处理完成',
      'error': '处理出错'
    };
    
    // 反向映射表，将多个阶段ID映射到主要阶段ID
    const reverseMap = {
      'analyzing': 'business_keyword_matching',
      'check_business_query': 'business_keyword_matching',
      'pattern_matching': 'builtin_keyword_matching',
      'similar_example': 'similar_query_search', 
      'find_similar_example': 'similar_query_search',
      'get_business_metadata': 'business_metadata',
      'generate_sql': 'sql_generation',
      'generating': 'sql_generation',
      'thinking': 'sql_generation',
      'fix_sql': 'sql_fix',
      'execute_sql': 'executing',
      'analyze_business': 'business_analysis'
    };
    
    // 先尝试从反向映射表中获取主要阶段ID
    const mainStage = reverseMap[stage] || stage;
    
    // 返回该阶段对应的显示名称，如果没有则显示原始阶段ID
    return stageMap[mainStage] || `阶段: ${stage}`;
  }
  
  // 清除思考状态
  function clearThinkingState() {
    thinkingProcess.value = '';
    thinkingLines.value = []; // 清空思考行数组
    queryProgress.value = 0;
    isThinking.value = false;
    currentStage.value = 'waiting';
    stageHistory.value = [];
    stageTiming.value = {}; // 清除阶段耗时
    stageStartTime.value = {}; // 清除阶段开始时间
  }
  
  // 连接到MCP服务器
  async function connect() {
    if (isConnected.value && client.value) return true;
    
    // 如果已经在连接中，等待较短时间或直接返回
    if (isConnecting.value) {
      console.log("已经有一个连接进程在进行中...");
      return false; // 不要等待，直接返回false表示无法立即连接
    }
    
    // 设置连接状态并清除之前的错误
    isConnecting.value = true;
    connectionError.value = null;
    
    // 设置安全超时，确保状态一定会重置
    const safetyTimeout = setTimeout(() => {
      if (isConnecting.value) {
        console.error("连接状态长时间未重置，强制重置");
        isConnecting.value = false;
      }
    }, 20000);
    
    try {
      console.log(`开始连接MCP服务器: ${mcpHost}:${mcpPort} (${mcpProtocol})`);
      
      // 使用MCPClient
      client.value = new MCPClient({
        host: mcpHost,
        port: mcpPort,
        protocol: mcpProtocol,
        maxRetries: 1
      });
      
      // 使用较短的超时时间
      const connectPromise = client.value.connect();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('连接超时')), 8000);
      });
      
      // 使用Promise.race实现超时处理
      await Promise.race([connectPromise, timeoutPromise]);
      
      isConnected.value = true;
      console.log('已连接到MCP服务');
      
      // 初始化后获取可用工具列表（异步，不阻塞）
      setTimeout(() => {
        fetchTools().catch(e => {
          console.warn('获取工具列表失败:', e);
        });
      }, 100);
      
      return true;
    } catch (error) {
      console.error('连接MCP服务失败:', error);
      connectionError.value = error.message;
      isConnected.value = false;
      client.value = null;
      return false;
    } finally {
      clearTimeout(safetyTimeout);
      isConnecting.value = false;
    }
  }
  
  // 断开MCP连接
  function disconnect() {
    if (!isConnected.value || !client.value) return;
    
    try {
      client.value.disconnect();
      isConnected.value = false;
      client.value = null;
      ElMessage.info('已断开MCP连接');
    } catch (error) {
      console.error('断开MCP连接时出错:', error);
    }
  }
  
  // 获取工具列表
  async function fetchTools() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      const response = await client.value.call('list_tools');
      console.log('获取到的原始工具列表:', response);
      
      // 正确处理嵌套的工具列表
      let toolsData = null;
      
      // 处理不同的返回格式
      if (response && response.result && response.result.tools && Array.isArray(response.result.tools)) {
        // 格式1: {id, success, result: {tools: [...]}}
        toolsData = response.result.tools;
        console.log('从result.tools中提取工具列表:', toolsData.length);
      } else if (response && response.tools && Array.isArray(response.tools)) {
        // 格式2: {tools: [...]}
        toolsData = response.tools;
        console.log('从response.tools中提取工具列表:', toolsData.length);
      } else if (response && Array.isArray(response)) {
        // 格式3: 直接是数组
        toolsData = response;
        console.log('从数组中提取工具列表:', toolsData.length);
      } else {
        console.warn('无法识别的工具列表格式:', response);
        // 确保至少有一个默认工具
        toolsData = [
          {name: 'nl2sql_query', description: '将自然语言查询转换为SQL并执行返回结果'},
          {name: 'nl2sql_query_stream', description: '将自然语言查询转换为SQL并使用流式响应返回结果'}
        ];
      }
      
      // 更新工具列表
      tools.value = toolsData;
      console.log('已保存工具列表:', tools.value.length, '个工具');
      
      return tools.value;
    } catch (error) {
      console.error('获取工具列表失败:', error);
      throw error;
    }
  }
  
  // 获取资源列表
  async function fetchResources() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      const result = await client.value.call('resources/templates/list');
      resources.value = result.resourceTemplates || [];
      return resources.value;
    } catch (error) {
      console.error('获取资源列表失败:', error);
      throw error;
    }
  }
  
  // 获取提示模板列表
  async function fetchPrompts() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      const result = await client.value.call('list_prompts');
      prompts.value = result.prompts || [];
      return prompts.value;
    } catch (error) {
      console.error('获取提示模板列表失败:', error);
      throw error;
    }
  }
  
  // 添加思考行内容
  function addThinkingLine(line) {
    if (line && line.trim() && !thinkingLines.value.includes(line.trim())) {
      thinkingLines.value.push(line.trim());
    }
  }

  // 处理思考过程内容更新
  function updateThinkingProcess(content) {
    if (!content) return;
    
    thinkingProcess.value = content;
    
    // 处理新增的行
    const lines = content.split('\n');
    for (const line of lines) {
      addThinkingLine(line);
    }
  }

  // 执行NL2SQL查询 - 流式版本
  async function streamNl2sqlQuery(query, callbacks = {}) {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      console.log(`开始执行流式NL2SQL查询: "${query}"`);
      
      // 重置思考过程和进度
      clearThinkingState();
      isThinking.value = true;
      
      // 启动状态轮询
      startStatusPolling();
      
      // 定义流式回调函数
      const streamCallbacks = {
        onThinking: (data) => {
          console.log('收到思考过程事件:', data);
          
          // 更新当前阶段 - 增强类型判断
          if (data.type) {
            const oldStage = currentStage.value;
            currentStage.value = data.type;
            console.log(`更新思考阶段: ${oldStage} -> ${data.type}`);
            
            // 添加到阶段历史
            if (!stageHistory.value.includes(data.type)) {
              stageHistory.value.push(data.type);
              console.log(`添加阶段到历史: ${data.type}, 当前历史:`, stageHistory.value);
            }
            
            // 更新思考过程
            if (data.content) {
              // 使用新的思考内容处理函数
              updateThinkingProcess(data.content);
            }
          }
          
          // 调用外部回调
          if (callbacks.onThinking) {
            callbacks.onThinking(data);
          }
        },
        
        onProgress: (data) => {
          console.log('收到进度更新事件:', data);
          
          // 更新进度 - 确保有进度值
          if (data.progress !== undefined) {
            const oldProgress = queryProgress.value;
            queryProgress.value = data.progress;
            console.log(`更新进度: ${oldProgress}% -> ${data.progress}%`);
          }
          
          // 调用外部回调
          if (callbacks.onProgress) {
            callbacks.onProgress(data);
          }
        },
        
        onPartial: (data) => {
          console.log('收到部分结果:', data);
          
          // 调用外部回调
          if (callbacks.onPartial) {
            callbacks.onPartial(data);
          }
        },
        
        onFinal: (data) => {
          console.log('收到最终结果:', data);
          isThinking.value = false;
          
          // 停止状态轮询
          stopStatusPolling();
          
          // 添加思考过程到结果
          data.result.thinking_process = thinkingProcess.value;
          
          // 调用外部回调
          if (callbacks.onFinal) {
            callbacks.onFinal(data.result);
          }
        },
        
        onError: (error) => {
          console.error('流式查询出错:', error);
          isThinking.value = false;
          
          // 停止状态轮询
          stopStatusPolling();
          
          // 构造错误响应
          const errorResponse = {
            status: 'error',
            message: '查询处理过程中出错',
            error: error,
            thinking_process: thinkingProcess.value
          };
          
          // 调用外部回调
          if (callbacks.onError) {
            callbacks.onError(errorResponse);
          }
        }
      };
      
      // 使用流式响应调用工具 - 优先使用专用的流式查询工具
      let toolsAvailable = [];
      
      try {
        toolsAvailable = tools.value.map(t => t.name);
      } catch (e) {
        console.warn('解析工具列表出错:', e);
        // 设置默认工具列表
        toolsAvailable = ["nl2sql_query", "nl2sql_query_stream"];
      }
      
      console.log('可用工具列表:', toolsAvailable);
      
      // 确保工具列表获取成功
      let allTools = toolsAvailable;
      if (!allTools || allTools.length === 0) {
        try {
          await fetchTools();
          // 重新尝试获取工具列表
          try {
            allTools = tools.value.map(t => t.name);
          } catch (e) {
            console.warn('重新解析工具列表出错:', e);
            // 使用安全的默认值
            allTools = ["nl2sql_query", "nl2sql_query_stream"];
          }
          console.log('重新获取工具列表:', allTools);
        } catch (error) {
          console.warn('重新获取工具列表失败，使用默认工具列表:', error);
          // 使用固定的工具名列表作为备选
          allTools = ["nl2sql_query", "nl2sql_query_stream"];
        }
      }
      
      // 首选nl2sql_query工具（为了避免流式错误，暂时使用普通工具）
      if (allTools.includes('nl2sql_query')) {
        console.log('使用普通nl2sql_query工具进行查询 (暂时避免流式错误)');
        await client.value.callToolStream('nl2sql_query', { query }, streamCallbacks);
      }
      // 其次尝试nl2sql_query_stream工具
      else if (allTools.includes('nl2sql_query_stream')) {
        console.log('使用nl2sql_query_stream工具进行流式查询');
        await client.value.callToolStream('nl2sql_query_stream', { query }, streamCallbacks);
      }
      // 最后尝试Doris流式查询工具
      else if (allTools.includes('mcp_doris_nl2sql_query_stream')) {
        console.log('使用mcp_doris_nl2sql_query_stream工具进行流式查询');
        await client.value.callToolStream('mcp_doris_nl2sql_query_stream', { query: query }, streamCallbacks);
      }
      // 兜底使用普通查询工具
      else {
        console.log('无已知查询工具可用，尝试使用默认nl2sql_query工具');
        await client.value.callToolStream('nl2sql_query', { query }, streamCallbacks);
      }
      
    } catch (error) {
      console.error('执行流式NL2SQL查询失败:', error);
      isThinking.value = false;
      
      // 尝试重新连接后再次调用
      if (error.message.includes('未连接') || error.message.includes('连接失败')) {
        console.log('连接已断开，尝试重新连接...');
        isConnected.value = false;
        
        try {
          await connect();
          if (isConnected.value) {
            return streamNl2sqlQuery(query, callbacks); // 重试流式查询
          }
        } catch (connError) {
          console.error('重新连接失败:', connError);
        }
      }
      
      // 构造错误响应
      const errorResponse = {
        status: 'error',
        message: '查询处理过程中出错',
        error: { message: error.message || '执行查询时发生未知错误' },
        thinking_process: thinkingProcess.value
      };
      
      // 调用外部错误回调
      if (callbacks.onError) {
        callbacks.onError(errorResponse);
      }
    }
  }
  
  // 执行NL2SQL查询 - 带有流式自动降级功能
  async function nl2sqlQuery(query) {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      console.log(`开始执行NL2SQL查询: "${query}"`);
      
      // 先尝试使用流式响应
      if (client.value.callToolStream) {
        try {
          return new Promise((resolve, reject) => {
            // 重置思考过程
            clearThinkingState();
            isThinking.value = true;
            
            streamNl2sqlQuery(query, {
              onFinal: (result) => {
                isThinking.value = false;
                resolve(result);
              },
              onError: (error) => {
                isThinking.value = false;
                // 如果是连接或格式错误，则降级使用常规调用
                if (error.error && (
                    error.error.message.includes('连接') || 
                    error.error.message.includes('解析') || 
                    error.error.message.includes('流式'))) {
                  console.warn('流式调用失败，降级使用常规调用', error);
                  
                  // 使用常规调用作为降级方案
                  client.value.callTool('nl2sql_query', { query })
                    .then(result => resolve(result))
                    .catch(err => reject(err));
                } else {
                  // 其他错误直接返回
                  reject(error);
                }
              }
            });
          });
        } catch (streamError) {
          console.warn('流式查询失败，降级使用常规调用:', streamError);
          // 降级使用常规调用方法
        }
      }
      
      // 没有流式支持或流式调用失败，使用常规调用
      console.log('使用常规方法调用nl2sql_query');
      
      // 先检查工具是否可用
      if (tools.value.length === 0) {
        try {
          await fetchTools();
        } catch (error) {
          console.warn('获取工具列表失败，但继续尝试调用工具:', error);
        }
      }
      
      // 直接调用工具，即使工具列表为空
      const result = await client.value.callTool('nl2sql_query', { query });
      console.log('NL2SQL查询结果:', result);
      
      // 如果响应为空但没有报错，构造一个有意义的错误响应
      if (!result) {
        return {
          status: 'error',
          message: '服务器返回了空数据，可能是工具未正确配置',
          error: { message: '无法获取查询结果，请检查服务器配置' }
        };
      }
      
      // 检查服务器响应是否只是回显了请求而没有处理
      if (result.result && result.result.message && result.result.message.includes('收到消息')) {
        console.error('服务器没有处理nl2sql_query请求，只是回显了请求内容');
        return {
          status: 'error',
          message: '服务器未正确处理NL2SQL查询',
          error: { 
            message: '服务器端nl2sql_query工具未正确实现，请联系管理员检查服务器实现',
            details: '服务器只返回了收到消息的确认，没有执行实际查询'
          }
        };
      }
      
      // 确保响应包含必要的字段
      if (!result.sql && !result.error) {
        // 添加错误信息
        result.error = { 
          message: '服务器响应格式不正确，请检查nl2sql工具是否正确实现' 
        };
      }
      
      return result;
    } catch (error) {
      console.error('执行NL2SQL查询失败:', error);
      
      // 尝试重新连接后再次调用
      if (error.message.includes('未连接') || error.message.includes('连接失败')) {
        console.log('连接已断开，尝试重新连接...');
        isConnected.value = false;
        
        try {
          await connect();
          if (isConnected.value) {
            return nl2sqlQuery(query); // 重试查询
          }
        } catch (connError) {
          console.error('重新连接失败:', connError);
        }
      }
      
      // 返回友好的错误信息
      return {
        status: 'error',
        message: '查询处理过程中出错',
        error: { message: error.message || '执行查询时发生未知错误' }
      };
    }
  }
  
  // 执行SQL优化
  async function sqlOptimize(sql, requirements = '') {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('sql_optimize', { sql, requirements });
    } catch (error) {
      console.error('执行SQL优化失败:', error);
      throw error;
    }
  }
  
  // 获取处理状态
  async function getNl2sqlStatus() {
    if (!available.value) {
      console.warn('MCP客户端未连接，无法获取处理状态');
      return null;
    }
    
    try {
      // 检查一下工具是否存在
      if (tools.value.length > 0 && !tools.value.some(tool => tool.name === 'get_nl2sql_status')) {
        console.warn('后端未实现get_nl2sql_status工具，使用模拟状态进度');
        return simulateStatusUpdate();
      }
      
      const result = await client.value.callTool('get_nl2sql_status', {});
      console.log('获取NL2SQL处理状态原始数据:', result);
      
      if (result) {
        let status = null;
        
        // 尝试从各种可能的响应结构中提取状态
        if (result.current_status) {
          status = result.current_status;
        } else if (result.result && result.result.current_status) {
          status = result.result.current_status;
        } else if (result.result) {
          status = result.result;
        } else if (result.success && result.success === true) {
          // 可能直接就是一个带有success标记的状态对象
          status = result;
        }
        
        // 只有在获取到有效状态时才处理
        if (status) {
          console.log('解析后的状态数据:', JSON.stringify(status, null, 2));
          
          // 强制触发更新UI
          const updateTime = new Date().toISOString();
          console.log(`状态更新时间: ${updateTime}`);
          
          // 更新当前阶段
          if (status.current_stage !== undefined) {
            currentStage.value = status.current_stage;
            console.log(`设置当前阶段: ${status.current_stage}`);
          }
          
          // 更新进度
          if (status.progress !== undefined) {
            queryProgress.value = status.progress;
            console.log(`设置进度: ${status.progress}%`);
          }
          
          // 更新阶段历史
          if (status.stage_history && status.stage_history.length > 0) {
            stageHistory.value = [...status.stage_history];
            console.log(`设置阶段历史: ${status.stage_history.join(', ')}`);
          }
          
          // 更新处理状态
          if (status.is_processing !== undefined) {
            is_processing.value = status.is_processing;
            console.log(`设置处理状态: ${status.is_processing ? '处理中' : '空闲'}`);
          }
          
          // 更新当前查询
          if (status.current_query !== undefined) {
            current_query.value = status.current_query;
            console.log(`设置当前查询: ${status.current_query}`);
          }
          
          // 关键修复: 正确设置isThinking状态
          // 如果current_stage不是waiting，无论is_processing如何，都应该显示思考状态
          if (status.current_stage && status.current_stage !== 'waiting') {
            isThinking.value = true;
            console.log(`设置思考状态为true，因为当前阶段是 ${status.current_stage}`);
          } 
          // 只有当确定是waiting状态且非处理中时，才重置思考状态
          else if (status.current_stage === 'waiting' && status.is_processing === false) {
            isThinking.value = false;
            console.log('设置思考状态为false，因为当前阶段是waiting且非处理中');
          }
          // 如果is_processing为true，也设置思考状态为true
          else if (status.is_processing === true) {
            isThinking.value = true;
            console.log('设置思考状态为true，因为is_processing为true');
          }
          
          return status;
        } else {
          console.warn('状态响应格式不正确，无法找到status字段:', result);
          return null;
        }
      } else {
        console.warn('获取状态返回空响应');
        return null;
      }
    } catch (error) {
      console.error('获取处理状态失败:', error);
      return null;
    }
  }
  
  // 模拟状态更新（在工具不可用时使用）
  function simulateStatusUpdate() {
    // 如果当前有阶段信息，则基于该阶段模拟进度
    if (currentStage.value) {
      // 获取预设阶段顺序
      const stageOrder = ['start', 'analyzing', 'similar_example', 'business_metadata', 'generating', 'executing', 'complete'];
      
      // 找到当前阶段在预设顺序中的位置
      const currentIndex = stageOrder.indexOf(currentStage.value);
      
      // 如果找到了位置，则根据位置设置一个合理的进度值
      if (currentIndex >= 0) {
        // 为每个阶段分配一个进度范围
        const progressRanges = [
          [0, 5],    // start
          [5, 15],   // business_keyword_matching
          [15, 25],   // builtin_keyword_matching
          [25, 35],   // similar_query_search
          [35, 45],   // business_metadata
          [45, 65],   // sql_generation
          [65, 90],   // sql_fix
          [90, 95],   // result_analysis
          [95, 97],   // sql_execution_complete
          [97, 100]   // business_analysis
        ];
        
        // 获取当前阶段的进度范围
        const [min, max] = progressRanges[currentIndex];
        
        // 如果当前进度低于该阶段的最小值，则设置为最小值
        // 如果当前进度已经在范围内，则小幅增加
        // 如果当前进度高于该阶段的最大值，保持不变
        if (queryProgress.value < min) {
          queryProgress.value = min;
        } else if (queryProgress.value < max) {
          // 每次增加1-3%的进度
          queryProgress.value = Math.min(max, queryProgress.value + Math.floor(Math.random() * 3) + 1);
        }
      } else {
        // 如果当前阶段不在预设顺序中，则假设是中间阶段
        if (queryProgress.value < 50) {
          queryProgress.value = Math.min(90, queryProgress.value + Math.floor(Math.random() * 5) + 1);
        }
      }
    }
    
    // 返回模拟的状态对象
    return {
      current_stage: currentStage.value || 'unknown',
      progress: queryProgress.value,
      stage_history: stageHistory.value,
      simulated: true, // 标记此状态为模拟生成
      is_processing: is_processing.value,
      current_query: current_query.value
    };
  }
  
  // 开始状态轮询
  function startStatusPolling() {
    if (statusPollingInterval.value) {
      console.log('状态监听已经在运行中');
      return;
    }
    
    console.log('注册NL2SQL状态更新监听');
    statusPollingEnabled.value = true;
    isThinking.value = true; // 开始时强制设置思考状态
    
    // 不再进行任何主动轮询，完全依赖服务器推送
    // 不再主动调用一次状态获取
    // 仅注册监听器
    
    // 注册状态更新监听器
    if (client.value) {
      // 使用状态监听器接收服务器推送的状态更新
      const removeStatusListener = client.value.addStatusListener((type, data) => {
        console.log('收到状态更新:', type, data);
        if (type === 'status_update') {
          // 收到服务器推送的状态更新，无需再轮询请求
          handleStatusUpdate(data);
        }
      });
      
      // 使用工具结果监听器接收get_nl2sql_status工具的结果
      const removeToolListener = client.value.addToolResultListener('get_nl2sql_status', (result) => {
        console.log('收到工具状态更新:', result);
        // 处理工具结果中的状态数据
        if (result) {
          handleStatusUpdate(result);
        }
      });
      
      // 专门订阅mcp_doris_get_nl2sql_status工具的结果
      const removeDorisToolListener = client.value.addToolResultListener('mcp_doris_get_nl2sql_status', (result) => {
        console.log('收到Doris NL2SQL状态更新:', result);
        // 处理工具结果中的状态数据
        if (result) {
          handleStatusUpdate(result);
        }
      });
      
      // 存储移除监听器的函数，以便在停止时调用
      statusPollingInterval.value = {
        removeStatusListener,
        removeToolListener,
        removeDorisToolListener
      };
    } else {
      console.warn('客户端未连接，无法注册状态监听器');
      statusPollingEnabled.value = false;
    }
  }
  
  // 处理状态更新数据
  function handleStatusUpdate(data) {
    // 解析状态数据
    let status = data;
    
    // 尝试提取嵌套在其他结构中的状态数据
    if (typeof data === 'string') {
      try {
        status = JSON.parse(data);
      } catch (e) {
        console.warn('状态数据不是有效的JSON:', data);
        return;
      }
    }
    
    // 尝试从各种可能的响应结构中提取状态
    if (status.current_status) {
      status = status.current_status;
    } else if (status.result && status.result.current_status) {
      status = status.result.current_status;
    } else if (status.result) {
      status = status.result;
    }
    
    console.log('处理状态更新:', status);
    
    // 强制触发更新UI
    const updateTime = new Date().toISOString();
    console.log(`状态更新时间: ${updateTime}`);
    
    // 更新当前阶段
    if (status.current_stage !== undefined) {
      currentStage.value = status.current_stage;
      console.log(`设置当前阶段: ${status.current_stage}`);
    }
    
    // 更新进度
    if (status.progress !== undefined) {
      queryProgress.value = status.progress;
      console.log(`设置进度: ${status.progress}%`);
    }
    
    // 更新阶段历史
    if (status.stage_history && status.stage_history.length > 0) {
      stageHistory.value = [...status.stage_history];
      console.log(`设置阶段历史: ${status.stage_history.join(', ')}`);
    }
    
    // 更新处理状态
    if (status.is_processing !== undefined) {
      is_processing.value = status.is_processing;
      console.log(`设置处理状态: ${status.is_processing ? '处理中' : '空闲'}`);
    }
    
    // 更新当前查询
    if (status.current_query !== undefined) {
      current_query.value = status.current_query;
      console.log(`设置当前查询: ${status.current_query}`);
    }
    
    // 设置思考状态
    if (status.current_stage && status.current_stage !== 'waiting') {
      isThinking.value = true;
      console.log(`设置思考状态为true，因为当前阶段是 ${status.current_stage}`);
    } 
    // 只有当确定是waiting状态且非处理中时，才重置思考状态
    else if (status.current_stage === 'waiting' && status.is_processing === false) {
      isThinking.value = false;
      console.log('设置思考状态为false，因为当前阶段是waiting且非处理中');
    }
    // 如果is_processing为true，也设置思考状态为true
    else if (status.is_processing === true) {
      isThinking.value = true;
      console.log('设置思考状态为true，因为is_processing为true');
    }
  }
  
  // 停止状态轮询
  function stopStatusPolling() {
    if (statusPollingInterval.value) {
      console.log('停止NL2SQL状态监听');
      
      // 移除监听器
      if (typeof statusPollingInterval.value.removeStatusListener === 'function') {
        statusPollingInterval.value.removeStatusListener();
      }
      
      if (typeof statusPollingInterval.value.removeToolListener === 'function') {
        statusPollingInterval.value.removeToolListener();
      }
      
      if (typeof statusPollingInterval.value.removeDorisToolListener === 'function') {
        statusPollingInterval.value.removeDorisToolListener();
      }
      
      statusPollingInterval.value = null;
    }
    statusPollingEnabled.value = false;
  }
  
  // 修复SQL
  async function fixSql(sql, error_message, requirements = '') {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('fix_sql', { sql, error_message, requirements });
    } catch (error) {
      console.error('修复SQL出错:', error);
      throw error;
    }
  }
  
  // 检查服务器健康状态
  async function checkHealth() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('health');
    } catch (error) {
      console.error('检查健康状态失败:', error);
      throw error;
    }
  }
  
  // 获取服务器状态
  async function getStatus() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('status');
    } catch (error) {
      console.error('获取服务器状态失败:', error);
      throw error;
    }
  }
  
  // 列出LLM提供商
  async function listLLMProviders() {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('list_llm_providers');
    } catch (error) {
      console.error('获取LLM提供商列表失败:', error);
      throw error;
    }
  }
  
  // 设置LLM提供商
  async function setLLMProvider(provider_name) {
    if (!available.value) {
      throw new Error('MCP客户端未连接');
    }
    
    try {
      return await client.value.callTool('set_llm_provider', { provider_name });
    } catch (error) {
      console.error('设置LLM提供商失败:', error);
      throw error;
    }
  }
  
  // 强制设置思考状态
  function setThinkingState(thinking) {
    console.log(`强制设置思考状态: ${isThinking.value} -> ${thinking}`);
    isThinking.value = thinking;
  }
  
  // 强制设置当前阶段
  function setCurrentStage(stage) {
    console.log(`强制设置当前阶段: ${currentStage.value} -> ${stage}`);
    currentStage.value = stage;
    // 如果不是waiting阶段，则添加到历史
    if (stage && stage !== 'waiting' && !stageHistory.value.includes(stage)) {
      stageHistory.value.push(stage);
    }
  }
  
  // 强制设置进度
  function setProgress(progress) {
    console.log(`强制设置进度: ${queryProgress.value}% -> ${progress}%`);
    queryProgress.value = progress;
  }
  
  // 执行自然语言到SQL的流式查询
  async function nl2sqlQueryStream(query) {
    isThinking.value = true;
    clearThinkingState();

    try {
      if (!available.value) {
        console.warn("MCP客户端未连接，尝试连接");
        await connect();
      }

      console.log(`执行NL2SQL流式查询: ${query}`);

      // 注册状态轮询
      startStatusPolling();

      // 开始处理（由服务器端定义具体的8个阶段）
      currentStage.value = "start";
      thinkingLines.value.push("开始处理您的查询...");
      
      // 构建流式回调
      const callbacks = {
        onThinking: (data) => {
          console.log('收到思考事件:', data);
          
          // 提取有用信息
          const content = data.content || '';
          const stage = data.stage || data.type || 'thinking';
          const progress = data.progress || 0;
          
          // 更新状态
          if (stage && stage !== 'thinking') {
            currentStage.value = stage;
            console.log(`[Stream] 设置当前阶段: ${stage}`);
            
            // 添加到阶段历史
            if (!stageHistory.value.includes(stage)) {
              stageHistory.value.push(stage);
              console.log(`[Stream] 添加到阶段历史: ${stage}, 当前历史: ${stageHistory.value.join(', ')}`);
            }
          }
          
          if (progress) {
            queryProgress.value = progress;
            console.log(`[Stream] 设置进度: ${progress}%`);
          }
          
          // 添加思考内容
          if (content) {
            thinkingProcess.value += content + '\n';
            thinkingLines.value.push(content);
            console.log(`[Stream] 添加思考内容: ${content}`);
          }
        },
        onProgress: (data) => {
          console.log('收到进度事件:', data);
          
          // 尝试提取阶段和进度信息
          if (data.current_stage) {
            currentStage.value = data.current_stage;
            console.log(`[Stream] 进度事件设置当前阶段: ${data.current_stage}`);
          }
          
          if (data.progress !== undefined) {
            queryProgress.value = data.progress;
            console.log(`[Stream] 进度事件设置进度: ${data.progress}%`);
          }
          
          if (data.stage_history && Array.isArray(data.stage_history)) {
            stageHistory.value = [...data.stage_history];
            console.log(`[Stream] 进度事件设置阶段历史: ${data.stage_history.join(', ')}`);
          }
        },
        onPartial: (data) => {
          console.log('收到部分结果:', data);
        },
        onFinal: (data) => {
          console.log('收到最终结果:', data);
          
          // 确保设置完成阶段
          currentStage.value = 'complete';
          if (!stageHistory.value.includes('complete')) {
            stageHistory.value.push('complete');
          }
          
          // 设置进度为100%
          queryProgress.value = 100;
          
          // 添加到思考记录
          thinkingLines.value.push("查询处理完成");
          
          // 尝试分发事件给前端组件显示结果
          try {
            console.log("分发最终事件到前端组件");
            const finalEvent = new CustomEvent('nl2sql:event', {
              detail: {
                type: 'final',
                data: data
              }
            });
            window.dispatchEvent(finalEvent);
            console.log("最终事件分发完成");
          } catch (eventError) {
            console.error("分发最终事件失败:", eventError);
          }
          
          // 结束思考状态
          isThinking.value = false;
          
          // 停止状态轮询
          stopStatusPolling();
        },
        onError: (error) => {
          console.error('流式查询出错:', error);
          
          // 添加错误到思考记录
          thinkingLines.value.push(`处理出错: ${error.message || '未知错误'}`);
          
          // 结束思考状态
          isThinking.value = false;
          
          // 停止状态轮询
          stopStatusPolling();
        }
      };
      
      try {
        console.log("开始调用流式查询工具 nl2sql_query_stream");
        const result = await client.value.callToolStream('nl2sql_query_stream', { query }, callbacks);
        console.log("流式查询工具调用完成", result);
        return result;
      } catch (toolError) {
        console.error("流式查询工具调用失败:", toolError);
        
        // 使用通用方式尝试查询
        console.log("回退到标准查询方式");
        return await nl2sqlQuery(query);
      }
    } catch (error) {
      console.error('流式查询执行失败:', error);

      // 尝试恢复连接
      if (error.message && (error.message.includes('未连接') || error.message.includes('连接失败'))) {
        console.log("检测到连接问题，尝试重新连接");
        
        try {
          await connect();
          if (isConnected.value) {
            return nl2sqlQueryStream(query); // 重试流式查询
          }
        } catch (connError) {
          console.error('重新连接失败:', connError);
        }
      }
      
      // 返回友好的错误信息
      return {
        status: 'error',
        message: '查询处理过程中出错',
        error: { message: error.message || '执行查询时发生未知错误' }
      };
    }
  }
  
  // 添加直接调用NL2SQL流接口的方法
  async function nl2sqlQueryDirectStream(query) {
    console.log('调用直接NL2SQL流式接口, 查询:', query);
    
    // 准备请求数据 - 使用与服务器端相同的格式
    const requestData = {
      query: query,
      session_id: generateSessionId(), // 生成会话ID或使用现有的
      timestamp: Date.now()
    };
    
    // 打开一个直接到NL2SQL流接口的SSE连接
    try {
      console.log(`发送请求到 /nl2sql/stream: ${JSON.stringify(requestData)}`);
      
      const response = await fetch('/nl2sql/stream', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream'
        },
        body: JSON.stringify(requestData)
      });

      if (!response.ok) {
        console.error('NL2SQL流接口返回错误:', response.status, response.statusText);
        throw new Error(`服务器返回错误: ${response.status} ${response.statusText}`);
      }

      // 开始处理思考
      clearThinkingState();
      setThinkingState(true);
      addThinkingLine("开始处理您的查询...");

      // 创建SSE读取器
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      // 读取和处理SSE事件
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          console.log('读取完成');
          break;
        }

        // 解码二进制数据并添加到缓冲区
        buffer += decoder.decode(value, { stream: true });
        console.log(`收到SSE数据片段, 长度: ${value.length}`);

        // 处理事件
        const events = buffer.split('\n\n');
        buffer = events.pop() || ''; // 保留最后一个可能不完整的事件

        for (const event of events) {
          if (!event.trim()) continue;

          // 解析事件数据
          const lines = event.split('\n');
          let eventData = null;
          let eventType = 'message';

          for (const line of lines) {
            if (line.startsWith('data:')) {
              try {
                const dataContent = line.slice(5).trim();
                eventData = JSON.parse(dataContent);
                console.log('解析SSE事件数据成功:', typeof eventData);
              } catch (e) {
                console.warn('解析SSE事件数据失败:', line, e);
              }
            } else if (line.startsWith('event:')) {
              eventType = line.slice(6).trim();
              console.log('事件类型:', eventType);
            }
          }

          if (eventData) {
            // 尝试处理多种可能的数据结构
            let type = 'thinking';
            let data = eventData;
            
            // 处理不同的数据结构
            if (eventData.type) {
              type = eventData.type;
              data = eventData.data || eventData;
            } else if (eventData.event) {
              type = eventData.event;
              data = eventData.data || eventData;
            }
            
            console.log(`处理事件 [${type}]:`, data);
            
            // 处理关闭事件
            if (eventType === 'close' || type === 'close') {
              console.log('收到关闭事件，流式处理完成');
              setThinkingState(false);
              continue;
            }
            
            // 处理思考事件
            if (type === 'thinking') {
              let stage = '';
              let progress = 0;
              let content = '';
              
              // 尝试从不同的数据结构中提取信息
              if (typeof data === 'object') {
                stage = data.stage || '';
                progress = data.progress || 0;
                content = data.content || '';
              } else if (typeof data === 'string') {
                content = data;
              }
              
              if (stage) {
                setCurrentStage(stage);
                console.log(`设置当前阶段: ${stage}`);
              }
              
              if (progress) {
                setProgress(progress);
                console.log(`设置进度: ${progress}%`);
              }
              
              if (content) {
                addThinkingLine(content);
                console.log(`添加思考内容: ${content}`);
              }
            }
            // 处理进度事件
            else if (type === 'progress') {
              console.log('处理进度事件:', data);
              let stage = '';
              let progress = 0;
              
              if (typeof data === 'object') {
                stage = data.stage || '';
                progress = data.progress || 0;
              }
              
              if (stage) {
                setCurrentStage(stage);
              }
              
              if (progress) {
                setProgress(progress);
              }
            }
            // 处理最终结果
            else if (type === 'final') {
              console.log('处理最终结果:', data);
              setThinkingState(false);
            }
            // 处理错误
            else if (type === 'error') {
              console.error('处理错误:', data);
              let message = '未知错误';
              
              if (typeof data === 'object') {
                message = data.message || data.error || JSON.stringify(data);
              } else if (typeof data === 'string') {
                message = data;
              }
              
              addThinkingLine(`错误: ${message}`);
              setThinkingState(false);
            }
            
            // 触发事件处理
            const eventDetail = { type, data };
            const customEvent = new CustomEvent('nl2sql:event', { detail: eventDetail });
            window.dispatchEvent(customEvent);
          }
        }
      }

      console.log('NL2SQL流式处理完成');
      return true;
    } catch (error) {
      console.error('NL2SQL流式处理错误:', error);
      // 添加错误到思考记录
      addThinkingLine(`处理出错: ${error.message || '未知错误'}`);
      setThinkingState(false);
      throw error;
    }
  }
  
  // 生成唯一的会话ID
  function generateSessionId() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }
  
  return {
    client,
    isConnected,
    isConnecting,
    tools,
    resources,
    prompts,
    connectionError,
    
    // 流式响应状态
    thinkingProcess,
    thinkingLines,
    queryProgress,
    isThinking,
    currentStage,
    stageHistory,
    is_processing,
    current_query,
    
    // 计算属性
    available,
    
    connect,
    disconnect,
    fetchTools,
    fetchResources,
    fetchPrompts,
    nl2sqlQuery,
    streamNl2sqlQuery,
    sqlOptimize,
    fixSql,
    checkHealth,
    getStatus,
    listLLMProviders,
    setLLMProvider,
    getStageName,
    clearThinkingState,
    stageLabels,
    statusPollingEnabled,
    statusPollingInterval,
    pollingFrequency,
    getNl2sqlStatus,
    startStatusPolling,
    stopStatusPolling,
    
    // 新增方法
    setThinkingState,
    setCurrentStage,
    setProgress,
    addThinkingLine,
    updateThinkingProcess,
    nl2sqlQueryStream,
    nl2sqlQueryDirectStream
  };
}); 