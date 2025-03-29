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
  const queryProgress = ref(0);
  const isThinking = ref(false);
  const currentStage = ref('');
  const stageHistory = ref([]);  // 阶段历史记录
  const statusPollingEnabled = ref(false); // 是否启用状态轮询
  const statusPollingInterval = ref(null); // 状态轮询定时器
  const pollingFrequency = 1000; // 轮询频率（毫秒）
  const is_processing = ref(false); // 是否正在处理查询
  const current_query = ref(''); // 当前正在处理的查询
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
    return stageLabels[stage] || stage || '处理中';
  }
  
  // 清除思考状态
  function clearThinkingState() {
    thinkingProcess.value = '';
    queryProgress.value = 0;
    isThinking.value = false;
    currentStage.value = '';
    stageHistory.value = [];
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
      const result = await client.value.call('list_tools');
      tools.value = result.tools || [];
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
      
      // 先检查工具是否可用
      if (tools.value.length === 0) {
        try {
          await fetchTools();
        } catch (error) {
          console.warn('获取工具列表失败，但继续尝试调用工具:', error);
        }
      }
      
      // 定义接收流式数据的回调
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
            
            // 如果没有进度信息，根据阶段设置默认进度
            if (!data.progress && data.type) {
              const stageIndex = ['start', 'analyzing', 'similar_example', 'business_metadata', 'generating', 'executing', 'complete']
                .indexOf(data.type);
              
              if (stageIndex >= 0) {
                // 根据阶段序号设置默认进度
                const oldProgress = queryProgress.value;
                const defaultProgress = Math.min(Math.round((stageIndex + 1) / 7 * 100), 100);
                console.log(`为阶段 ${data.type} 设置默认进度: ${oldProgress}% -> ${defaultProgress}%`);
                queryProgress.value = defaultProgress;
              }
            }
          }
          
          // 同样检查step字段
          if (data.step && (!data.type || data.step !== data.type)) {
            const oldStage = currentStage.value;
            currentStage.value = data.step;
            console.log(`更新思考阶段(从step): ${oldStage} -> ${data.step}`);
            
            // 添加到阶段历史
            if (!stageHistory.value.includes(data.step)) {
              stageHistory.value.push(data.step);
              console.log(`添加阶段到历史: ${data.step}, 当前历史:`, stageHistory.value);
            }
          }
          
          // 明确设置进度，如果有的话
          if (data.progress !== undefined) {
            const oldProgress = queryProgress.value;
            queryProgress.value = data.progress;
            console.log(`更新进度(从思考事件): ${oldProgress}% -> ${data.progress}%`);
          }
          
          // 更新思考过程
          if (data.message) {
            // 如果是新的思考过程，添加换行符
            if (thinkingProcess.value && !thinkingProcess.value.endsWith('\n')) {
              thinkingProcess.value += '\n';
            }
            
            // 添加新的思考内容
            thinkingProcess.value += data.message;
            console.log(`添加思考内容: ${data.message.substring(0, 50)}${data.message.length > 50 ? '...' : ''}`);
            
            // 调用外部回调
            if (callbacks.onThinking) {
              callbacks.onThinking({
                message: data.message,
                fullThinking: thinkingProcess.value,
                type: data.type || data.step || 'thinking',
                sql: data.sql || ''
              });
            }
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
          
          // 更新当前阶段 - 优先使用step字段
          if (data.step) {
            const oldStage = currentStage.value;
            currentStage.value = data.step;
            console.log(`更新处理阶段: ${oldStage} -> ${data.step}`);
            
            // 添加到阶段历史
            if (!stageHistory.value.includes(data.step)) {
              stageHistory.value.push(data.step);
              console.log(`添加阶段到历史: ${data.step}, 当前历史:`, stageHistory.value);
            }
          }
          
          // 如果没有step但有type，也可以使用type作为阶段
          if (!data.step && data.type) {
            const oldStage = currentStage.value;
            currentStage.value = data.type;
            console.log(`更新处理阶段(从type): ${oldStage} -> ${data.type}`);
            
            // 添加到阶段历史
            if (!stageHistory.value.includes(data.type)) {
              stageHistory.value.push(data.type);
              console.log(`添加阶段到历史: ${data.type}, 当前历史:`, stageHistory.value);
            }
          }
          
          // 如果没有进度信息，根据阶段设置默认进度
          if (data.progress === undefined && (data.step || data.type)) {
            const stage = data.step || data.type;
            const stageIndex = ['start', 'analyzing', 'similar_example', 'business_metadata', 'generating', 'executing', 'complete']
              .indexOf(stage);
            
            if (stageIndex >= 0) {
              // 根据阶段序号设置默认进度
              const oldProgress = queryProgress.value;
              const defaultProgress = Math.min(Math.round((stageIndex + 1) / 7 * 100), 100);
              console.log(`为阶段 ${stage} 设置默认进度: ${oldProgress}% -> ${defaultProgress}%`);
              queryProgress.value = defaultProgress;
            }
          }
          
          // 添加进度消息到思考过程
          if (data.message) {
            // 如果是新的进度消息，添加换行符
            if (thinkingProcess.value && !thinkingProcess.value.endsWith('\n')) {
              thinkingProcess.value += '\n';
            }
            
            // 添加带有进度和当前阶段的消息
            const progressStr = data.progress !== undefined ? `[${data.progress}%]` : '';
            const stageStr = currentStage.value ? `[${getStageName(currentStage.value)}]` : '';
            thinkingProcess.value += `${progressStr} ${stageStr} ${data.message}`;
            
            // 调用外部回调
            if (callbacks.onProgress) {
              callbacks.onProgress({
                message: data.message,
                progress: data.progress !== undefined ? data.progress : queryProgress.value,
                fullThinking: thinkingProcess.value,
                step: data.step || data.type || currentStage.value
              });
            }
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
      
      // 使用流式响应调用工具
      await client.value.callToolStream('nl2sql_query', { query }, streamCallbacks);
      
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
          [0, 10],    // start
          [10, 30],   // analyzing
          [30, 40],   // similar_example
          [40, 50],   // business_metadata
          [50, 70],   // generating
          [70, 90],   // executing
          [90, 100]   // complete
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
      console.log('状态轮询已经在运行中');
      return;
    }
    
    console.log('开始NL2SQL状态轮询');
    statusPollingEnabled.value = true;
    isThinking.value = true; // 开始轮询时强制设置思考状态
    
    // 立即执行一次状态获取
    getNl2sqlStatus().catch(err => {
      console.warn('初始状态获取失败，但不影响轮询:', err);
    });
    
    // 设置轮询间隔
    statusPollingInterval.value = setInterval(async () => {
      if (!statusPollingEnabled.value) {
        stopStatusPolling();
        return;
      }
      
      try {
        await getNl2sqlStatus();
      } catch (error) {
        console.error('轮询状态出错，但继续轮询:', error);
      }
    }, 2000); // 更改为2秒一次
  }
  
  // 停止状态轮询
  function stopStatusPolling() {
    if (statusPollingInterval.value) {
      console.log('停止NL2SQL状态轮询');
      clearInterval(statusPollingInterval.value);
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
    setProgress
  };
}); 