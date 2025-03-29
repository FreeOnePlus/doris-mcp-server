/**
 * MCP客户端
 * 实现Model Context Protocol标准
 */

export class MCPClient {
  constructor(options = {}) {
    this.host = options.host || 'localhost';
    this.port = options.port || 5000;
    this.protocol = options.protocol || 'http';
    this.baseUrl = `${this.protocol}://${this.host}:${this.port}`;
    this.maxRetries = options.maxRetries || 1;
    this.retryDelay = options.retryDelay || 1000;
    this.timeout = options.timeout || 10000; // 默认10秒超时
    this.withCredentials = false; // 不发送凭证，避免CORS问题
    this.isConnected = false;
    this.clientId = options.clientId || this._generateClientId();
    
    // 输出连接配置信息，方便调试
    console.info(`MCP客户端初始化: ${this.baseUrl}, clientId: ${this.clientId}`);
    
    // 配置HTTP请求选项
    this.requestOptions = {
      credentials: 'omit', // 不发送凭证
      mode: 'cors',
      headers: {
        'Content-Type': 'application/json'
      }
    };
    
    this.sessionId = null;
    this.messageEndpoint = null;
    this.eventSource = null;
    this.abortControllers = new Map();
    this.retryCount = 0;
    this.callId = 0;
    
    // 流式响应回调函数
    this.streamCallbacks = new Map();
  }

  _generateClientId() {
    return 'client_' + Math.random().toString(36).substring(2, 9);
  }

  /**
   * 连接到MCP服务器
   * @returns {Promise<boolean>} 连接是否成功
   */
  async connect() {
    // 如果已经连接，则不再尝试连接
    if (this.isConnected) {
      console.log('MCP客户端已连接，无需重复连接');
      return true;
    }
    
    try {
      console.log(`连接到MCP服务器: ${this.baseUrl}`);
      
      // 首先尝试健康检查
      try {
        console.log(`尝试健康检查: ${this.baseUrl}/health`);
        const healthResponse = await fetch(`${this.baseUrl}/health`, {
          method: 'GET',
          headers: { 'Content-Type': 'application/json' },
          signal: AbortSignal.timeout(3000), // 限制健康检查时间为3秒
          mode: 'cors',  // 增加CORS模式
          credentials: 'omit'  // 禁用凭证
        });
        
        if (healthResponse.ok) {
          console.log('服务器健康检查通过');
        } else {
          console.warn(`服务器健康检查失败，状态码: ${healthResponse.status}, 但继续尝试连接`);
        }
      } catch (error) {
        console.warn('健康检查失败，继续尝试连接', error);
      }
      
      // 使用SSE传输初始化连接
      const sseUrl = `${this.baseUrl}/sse`;
      console.log(`尝试建立SSE连接: ${sseUrl}`);
      
      // 使用Promise.race实现整体超时控制
      const connectionPromise = new Promise((resolve, reject) => {
        try {
          // 使用EventSource正确处理SSE连接
          const eventSource = new EventSource(sseUrl, {
            withCredentials: false  // 禁用凭证要求
          });
          
          // 设置连接超时
          const timeout = setTimeout(() => {
            eventSource.close();
            reject(new Error('SSE连接超时'));
          }, 5000); // 减少到5秒
          
          // 处理endpoint事件
          eventSource.addEventListener('endpoint', (event) => {
            clearTimeout(timeout);
            
            try {
              const data = event.data;
              console.log('收到endpoint事件:', data);
              
              // 解析消息端点
              this.messageEndpoint = `${this.baseUrl}${data}`;
              this.sessionId = data.split('session_id=')[1];
              
              console.log(`SSE连接成功: 会话ID=${this.sessionId}`);
              console.log(`消息端点: ${this.messageEndpoint}`);
              
              this.isConnected = true;
              this.retryCount = 0;  // 重置重试计数
              
              // 保存EventSource以便后续使用
              this.eventSource = eventSource;
              
              resolve(true);
            } catch (error) {
              console.error('处理endpoint事件出错:', error);
              eventSource.close();
              reject(error);
            }
          });
          
          // 处理常规消息
          eventSource.onmessage = (event) => {
            console.log('收到SSE消息:', event.data);
          };
          
          // 处理错误
          eventSource.onerror = (error) => {
            console.error('SSE连接错误:', error);
            clearTimeout(timeout);
            
            // 如果收到错误但已经成功连接，不要关闭连接
            if (!this.isConnected) {
              eventSource.close();
              reject(new Error('SSE连接失败'));
            } else {
              // 已连接状态下收到错误，可能是临时网络波动，记录但不断开
              console.warn('SSE连接已建立但收到错误，可能是暂时性问题');
            }
          };
        } catch (error) {
          console.error('创建EventSource出错:', error);
          reject(error);
        }
      });
      
      // 绝对超时Promise
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('整体连接操作超时')), 8000);
      });
      
      // 使用Promise.race确保不会永久挂起
      return await Promise.race([connectionPromise, timeoutPromise]);
      
    } catch (error) {
      console.error('连接到MCP服务器失败:', error);
      this.isConnected = false;
      
      // 实现重试逻辑
      if (this.retryCount < this.maxRetries) {
        this.retryCount++;
        console.log(`连接失败，正在尝试第 ${this.retryCount} 次重试...`);
        
        // 延迟重试，每次等待时间增加
        const delay = 1000 * this.retryCount;
        await new Promise(resolve => setTimeout(resolve, delay));
        
        return this.connect();  // 递归调用重试
      }
      
      throw error;
    }
  }
  
  /**
   * 调用MCP服务方法（tool、resource或prompt）
   * @param {string} method - 方法名称
   * @param {Object} params - 参数对象
   * @returns {Promise<any>} 调用结果
   */
  async call(method, params = {}) {
    if (!this.isConnected || !this.messageEndpoint) {
      await this.connect();
    }
    
    const id = ++this.callId;
    
    // 创建可取消的请求
    const abortController = new AbortController();
    this.abortControllers.set(id, abortController);
    
    try {
      // 根据方法名称确定请求类型
      let requestType;
      if (method.startsWith('resources/')) {
        requestType = 'resource';
      } else if (method.startsWith('prompts/')) {
        requestType = 'prompt';
      } else {
        requestType = 'tool';
      }
      
      // 构建请求对象
      const requestBody = {
        id: id.toString(),
        session_id: this.sessionId,
        type: requestType
      };
      
      // 根据请求类型添加特定参数
      if (requestType === 'tool') {
        requestBody.tool = method;
        requestBody.params = params;
      } else if (requestType === 'resource') {
        requestBody.uri = method.replace('resources/', '');
      } else if (requestType === 'prompt') {
        requestBody.prompt = method.replace('prompts/', '');
        requestBody.params = params;
      }
      
      // 发送请求
      console.log(`调用MCP方法: ${method}`, params);
      console.log(`发送请求到: ${this.messageEndpoint}`, requestBody);
      
      // 设置请求超时
      const timeoutId = setTimeout(() => {
        abortController.abort();
      }, this.timeout);
      
      const response = await fetch(this.messageEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(requestBody),
        signal: abortController.signal,
        mode: 'cors',
        credentials: 'omit'  // 禁用凭证要求
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error(`请求失败: ${response.status} ${response.statusText}`, errorText);
        throw new Error(`请求失败: ${response.status} ${response.statusText}`);
      }
      
      // 检查响应内容类型
      const contentType = response.headers.get('content-type');
      
      // 解析响应
      let result;
      if (contentType && contentType.includes('application/json')) {
        const text = await response.text();
        console.log(`收到原始响应: ${text}`);
        if (text.trim() === '') {
          // 处理空响应
          console.warn(`收到空响应，使用默认值`);
          result = { 
            status: "error", 
            error: { message: "服务器返回了空响应" } 
          };
        } else {
          try {
            result = JSON.parse(text);
          } catch (e) {
            console.error(`解析JSON响应失败:`, e);
            throw new Error(`解析响应失败: ${e.message}`);
          }
        }
      } else {
        // 非JSON响应
        const text = await response.text();
        console.warn(`收到非JSON响应: ${text}`);
        try {
          // 尝试作为JSON解析
          result = JSON.parse(text);
        } catch (e) {
          // 如果解析失败，创建一个错误对象
          result = { 
            status: "error", 
            error: { message: `收到非JSON响应: ${text.substring(0, 100)}${text.length > 100 ? '...' : ''}` } 
          };
        }
      }
      
      console.log(`收到MCP响应:`, result);
      
      // 检查是否有错误
      if (result && result.error) {
        const errorMessage = result.error.message || '未知错误';
        console.error(`API返回错误: ${errorMessage}`);
        throw new Error(errorMessage);
      }
      
      return result || { status: "error", error: { message: "服务器返回了无效响应" } };
    } catch (error) {
      console.error(`调用方法 ${method} 失败:`, error);
      
      // 如果是未连接错误，尝试重新连接
      if (error.message.includes('未连接') || error.message.includes('连接失败')) {
        this.isConnected = false;
        await this.connect();
        return this.call(method, params);  // 重试调用
      }
      
      throw error;
    } finally {
      this.abortControllers.delete(id);
    }
  }
  
  /**
   * 调用工具方法
   * @param {string} toolName - 工具名称
   * @param {Object} params - 参数对象
   * @returns {Promise<any>} 工具调用结果
   */
  async callTool(toolName, params = {}) {
    try {
      console.log(`尝试调用工具: ${toolName}`, params);
      
      // 检查特殊工具名，提供更友好的错误处理
      if (toolName === 'nl2sql_query') {
        console.log(`正在调用NL2SQL查询工具...`);
        
        try {
          // 尝试先检查健康状态
          await this.checkHealth();
        } catch (error) {
          console.warn(`健康检查失败，但继续尝试调用工具:`, error);
        }
      }
      
      // 调用工具
      const result = await this.call(toolName, params);
      
      // 检查是否收到了工具调用的实际响应，而不仅仅是请求确认
      if (result && result.result && typeof result.result === 'object') {
        if (result.result.message && result.result.message.includes('收到消息')) {
          console.warn(`工具 ${toolName} 只返回了请求确认，未返回处理结果`);
          console.log('原始响应:', JSON.stringify(result));
          
          // 构造更友好的错误信息
          return {
            status: 'error',
            error: {
              message: `服务器未正确处理 ${toolName} 请求`,
              details: `服务器似乎只是回应了收到请求的确认，但没有执行实际处理。请检查服务器端工具实现。`
            }
          };
        }
      }
      
      // 对空返回值进行特殊处理
      if (!result) {
        console.warn(`工具 ${toolName} 返回空值`);
        return {
          status: 'error',
          error: { 
            message: `工具 ${toolName} 返回空数据，可能未正确实现` 
          }
        };
      }
      
      return result;
    } catch (error) {
      console.error(`调用工具 ${toolName} 失败:`, error);
      
      // 返回友好的错误信息
      return {
        status: 'error',
        error: { 
          message: `工具调用失败: ${error.message || '未知错误'}` 
        }
      };
    }
  }
  
  /**
   * 访问资源
   * @param {string} uri - 资源URI
   * @returns {Promise<any>} 资源内容
   */
  async accessResource(uri) {
    return this.call(`resources/${uri}`);
  }
  
  /**
   * 使用提示模板
   * @param {string} promptName - 提示模板名称
   * @param {Object} params - 提示参数
   * @returns {Promise<any>} 提示结果
   */
  async usePrompt(promptName, params = {}) {
    return this.call(`prompts/${promptName}`, params);
  }
  
  /**
   * 获取可用工具列表
   * @returns {Promise<Array>} 工具列表
   */
  async listTools() {
    try {
      const result = await this.call('list_tools');
      return result.tools || [];
    } catch (error) {
      console.error('获取工具列表失败:', error);
      return [];
    }
  }
  
  /**
   * 取消正在进行的调用
   * @param {number} id - 调用ID
   */
  cancelCall(id) {
    const controller = this.abortControllers.get(id);
    if (controller) {
      controller.abort();
      this.abortControllers.delete(id);
      console.log(`已取消调用 ${id}`);
    }
  }

  /**
   * 断开连接
   */
  disconnect() {
    try {
      // 关闭EventSource连接
      if (this.eventSource) {
        this.eventSource.close();
        this.eventSource = null;
      }
      
      // 取消所有正在进行的请求
      for (const controller of this.abortControllers.values()) {
        controller.abort();
      }
      this.abortControllers.clear();
      
      this.isConnected = false;
      this.sessionId = null;
      this.messageEndpoint = null;
      this.eventsEndpoint = null;
      
      console.log('MCP连接已断开');
      return true;
    } catch (error) {
      console.error('断开MCP连接时出错:', error);
      return false;
    }
  }

  async checkHealth() {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);
      
      console.log(`检查MCP服务器健康状态: ${this.baseUrl}/health`);
      
      const response = await fetch(`${this.baseUrl}/health`, {
        ...this.requestOptions,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`健康检查失败: ${response.status} ${errorText}`);
      }
      
      const data = await response.json();
      console.log('健康检查结果:', data);
      return data;
    } catch (error) {
      console.error('健康检查出错:', error.message);
      throw error;
    }
  }

  async checkStatus() {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);
      
      console.log(`检查MCP服务器状态: ${this.baseUrl}/status`);
      
      const response = await fetch(`${this.baseUrl}/status`, {
        ...this.requestOptions,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`状态检查失败: ${response.status} ${errorText}`);
      }
      
      const data = await response.json();
      console.log('状态检查结果:', data);
      return data;
    } catch (error) {
      console.error('状态检查出错:', error.message);
      throw error;
    }
  }

  /**
   * 获取MCP服务器状态
   * @returns {Promise<Object>} 服务器状态信息
   */
  async getMCPStatus() {
    if (!this.isConnected) {
      throw new Error('请先连接到MCP服务器');
    }
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);
      
      const response = await fetch(`${this.baseUrl}/status`, {
        ...this.requestOptions,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`获取MCP状态失败: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error.message || '获取MCP状态时发生错误');
      }
      
      return result;
    } catch (error) {
      console.error('获取MCP状态出错:', error);
      throw error;
    }
  }
  
  /**
   * 获取可用工具列表
   * @returns {Promise<Array>} 工具列表
   */
  async getTools() {
    if (!this.isConnected) {
      throw new Error('请先连接到MCP服务器');
    }
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);
      
      const response = await fetch(`${this.baseUrl}/tools`, {
        ...this.requestOptions,
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`获取工具列表失败: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error.message || '获取工具列表时发生错误');
      }
      
      return result.tools || [];
    } catch (error) {
      console.error('获取工具列表出错:', error);
      throw error;
    }
  }

  /**
   * 调用工具方法 - 流式响应版本
   * @param {string} toolName - 工具名称
   * @param {Object} params - 参数对象
   * @param {Object} callbacks - 回调函数对象，包含 onThinking, onProgress, onPartial, onFinal, onError
   * @returns {Promise<void>} 
   */
  async callToolStream(toolName, params = {}, callbacks = {}) {
    // 生成调用ID
    const id = ++this.callId;
    
    try {
      console.log(`尝试流式调用工具: ${toolName}`, params);
      
      // 确保已连接
      if (!this.isConnected || !this.messageEndpoint) {
        await this.connect();
      }
      
      // 创建URL带上流式标志
      const streamUrl = `${this.messageEndpoint}&stream=true`;
      
      // 构建请求对象
      const requestBody = {
        id: id.toString(),
        session_id: this.sessionId,
        type: 'tool',
        tool: toolName,
        params: params
      };
      
      console.log(`发送流式请求到: ${streamUrl}`, requestBody);
      
      // 创建可取消的请求
      const abortController = new AbortController();
      this.abortControllers.set(id, abortController);
      
      // 创建fetch请求
      const response = await fetch(streamUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream'
        },
        body: JSON.stringify(requestBody),
        signal: abortController.signal,
        mode: 'cors',
        credentials: 'omit'  // 禁用凭证要求
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error(`流式请求失败: ${response.status} ${response.statusText}`, errorText);
        
        if (callbacks.onError) {
          callbacks.onError({
            message: `请求失败: ${response.status} ${response.statusText}`,
            details: errorText
          });
        }
        
        return;
      }
      
      // 创建流式响应处理器
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      
      // 读取并处理流数据
      while (true) {
        const { value, done } = await reader.read();
        
        if (done) {
          console.log('流式响应完成');
          break;
        }
        
        // 解码本次接收的数据
        const chunk = decoder.decode(value, { stream: true });
        console.log('收到WebSocket数据块:', chunk);
        buffer += chunk;
        
        // 解析出完整的SSE事件
        const events = buffer.split('\n\n');
        buffer = events.pop() || ''; // 最后一个可能不完整，留在buffer中
        
        console.log(`处理 ${events.length} 个SSE事件`);
        
        // 处理每个完整事件
        for (const event of events) {
          if (!event.trim().startsWith('data:')) {
            console.log('跳过非数据事件:', event);
            continue;
          }
          
          try {
            // 提取data部分并解析JSON
            const dataStr = event.trim().substring(5).trim();
            console.log('解析事件数据:', dataStr);
            const eventData = JSON.parse(dataStr);
            
            console.log('收到流式事件类型:', eventData.type);
            
            // 根据事件类型调用对应回调
            const data = eventData.data;
            
            // 对所有事件数据进行预处理，确保规范化的字段
            if (data && data.result) {
              // 调试原始事件数据
              console.log('事件原始数据:', JSON.stringify(data.result));
              
              // 处理阶段和进度字段
              const result = data.result;
              
              // 确保type和step字段的一致性
              if (result.step && !result.type) {
                result.type = result.step;
                console.log('从step设置type:', result.type);
              } else if (result.type && !result.step) {
                result.step = result.type;
                console.log('从type设置step:', result.step);
              }
              
              // 确保阶段有描述性名称
              if (result.step && !result.message) {
                result.message = `处理阶段: ${result.step}`;
                console.log('设置默认消息:', result.message);
              }
              
              // 如果指定了进度，确保是数字
              if (result.progress !== undefined) {
                // 确保进度是数字
                const origProgress = result.progress;
                result.progress = Number(result.progress);
                // 如果是NaN，设置默认值
                if (isNaN(result.progress)) {
                  console.log('进度值无效，使用默认值0:', origProgress);
                  result.progress = 0;
                }
                // 限制范围在0-100
                result.progress = Math.max(0, Math.min(100, result.progress));
                console.log('设置进度值:', result.progress);
              }
              
              console.log('规范化后的事件数据:', JSON.stringify(result));
            } else {
              console.warn('事件数据缺少result字段或为空:', data);
            }
            
            // 检查回调是否存在并处理事件
            switch (eventData.type) {
              case 'thinking':
                if (callbacks.onThinking) {
                  if (data.result) {
                    callbacks.onThinking(data.result);
                  } else {
                    console.warn('思考事件缺少result属性:', data);
                    callbacks.onThinking(data);
                  }
                }
                break;
              case 'progress':
                if (callbacks.onProgress) {
                  if (data.result) {
                    callbacks.onProgress(data.result);
                  } else {
                    console.warn('进度事件缺少result属性:', data);
                    callbacks.onProgress(data);
                  }
                }
                break;
              case 'partial':
                if (callbacks.onPartial) callbacks.onPartial(data);
                break;
              case 'final':
                if (callbacks.onFinal) callbacks.onFinal(data);
                break;
              case 'error':
                if (callbacks.onError) callbacks.onError(data.error || { message: '未知错误' });
                break;
              default:
                console.warn('未知事件类型:', eventData.type);
            }
          } catch (error) {
            console.error('解析事件数据失败:', error, event);
            if (callbacks.onError) {
              callbacks.onError({
                message: '解析服务器响应失败',
                details: error.message
              });
            }
          }
        }
      }
      
      console.log(`流式调用 ${toolName} 完成`);
      
    } catch (error) {
      console.error(`流式调用工具 ${toolName} 失败:`, error);
      
      if (callbacks.onError) {
        callbacks.onError({
          message: `调用失败: ${error.message || '未知错误'}`,
          details: error.stack
        });
      }
    } finally {
      this.abortControllers.delete(id);
    }
  }
} 