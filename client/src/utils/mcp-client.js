/**
 * MCP客户端
 * 实现Model Context Protocol标准
 */

export class MCPClient {
  constructor(options = {}) {
    this.host = options.host || 'localhost';
    this.port = options.port || 3000;
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
    
    // 状态更新和工具结果监听器
    this.statusListeners = [];
    this.toolResultListeners = new Map();
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
        console.log(`尝试健康检查: ${this.baseUrl}/status`);
        const healthResponse = await fetch(`${this.baseUrl}/status`, {
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
      const sseUrl = `${this.baseUrl}/mcp`;
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
              
              // 发送初始化请求
              this._sendInitializeRequest();
              
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
            try {
              const message = JSON.parse(event.data);
              this._handleMessage(message);
            } catch (error) {
              console.error('处理SSE消息出错:', error);
            }
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
   * 发送初始化请求
   * @private
   */
  async _sendInitializeRequest() {
    const initializeMessage = {
      jsonrpc: "2.0",
      id: ++this.callId,
      method: "initialize",
      params: {}
    };
    
    try {
      const response = await fetch(this.messageEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(initializeMessage),
        mode: 'cors',
        credentials: 'omit'
      });
      
      if (response.ok) {
        console.log('初始化请求发送成功');
      } else {
        console.warn('初始化请求失败:', response.status);
      }
    } catch (error) {
      console.error('发送初始化请求出错:', error);
    }
  }

  /**
   * 处理接收到的消息
   * @private
   */
  _handleMessage(message) {
    if (message.method === 'notifications/initialized') {
      console.log('收到初始化完成通知');
      // 发送工具列表请求
      this.listTools();
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
      
      // 触发工具结果监听器通知
      this._notifyToolResultListeners(toolName, result);
      
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
        params: params,
        stream: true  // 在请求正文中明确指定流式响应
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
        console.log('收到流式数据块:', chunk.length, '字节');
        console.log('数据块内容预览:', chunk.substring(0, 100));
        buffer += chunk;
        
        try {
          // 尝试直接解析整个响应
          const response = JSON.parse(buffer);
          console.log('成功解析完整JSON响应:', response);
          
          // 检查是否有正确的结果结构
          if (response.result && response.id === id.toString()) {
            // 这是最终结果
            if (callbacks.onFinal) {
              console.log('调用onFinal回调处理最终结果');
              callbacks.onFinal(response);
            }
            // 清空缓冲区
            buffer = '';
            break;
          }
        } catch (e) {
          // 如果不能解析完整JSON，可能是流还未完成，继续处理
          // 解析出完整的SSE事件
          console.log('无法解析为完整JSON，尝试解析SSE事件格式');
          const events = buffer.split('\n\n');
          buffer = events.pop() || ''; // 最后一个可能不完整，留在buffer中
          
          console.log(`检测到 ${events.length} 个可能的SSE事件`);
          
          // 处理每个完整事件
          for (const event of events) {
            console.log('处理SSE事件:', event.substring(0, 100));
            
            if (!event.trim().startsWith('data:')) {
              console.log('跳过非数据事件，事件类型:', event.split('\n')[0]);
              continue;
            }
            
            try {
              // 提取data部分并解析JSON
              const dataStr = event.trim().substring(5).trim();
              console.log('提取的事件数据:', dataStr.substring(0, 100));
              
              // 尝试解析JSON
              try {
                const eventData = JSON.parse(dataStr);
                console.log('成功解析事件数据为JSON:', eventData.type || '未知类型');
                
                // 如果是jsonrpc 2.0格式的完整响应
                if (eventData.jsonrpc === "2.0" && eventData.id && eventData.result) {
                  console.log('收到完整的jsonrpc响应:', eventData.id);
                  
                  // 这是最终结果
                  if (callbacks.onFinal) {
                    console.log('调用onFinal回调处理jsonrpc响应');
                    callbacks.onFinal(eventData);
                  }
                  buffer = '';
                  break;
                }
                
                console.log('收到流式事件类型:', eventData.type || '未指定类型');
                
                // 根据事件类型调用对应回调
                if (!eventData.type) {
                  console.warn('事件数据缺少type字段:', eventData);
                  continue;
                }
                
                const data = eventData.data || eventData;
                
                // 检查回调是否存在并处理事件
                switch (eventData.type) {
                  case 'thinking':
                    if (callbacks.onThinking) {
                      console.log('处理thinking事件:', {
                        type: eventData.stage || 'thinking',
                        content: eventData.content || '',
                        progress: eventData.progress || 0,
                        stage: eventData.stage || 'thinking'
                      });
                      
                      callbacks.onThinking({
                        type: eventData.stage || 'thinking',
                        content: eventData.content || '',
                        progress: eventData.progress || 0,
                        stage: eventData.stage || 'thinking'
                      });
                      
                      console.log('thinking事件处理完成');
                    } else {
                      console.warn('收到thinking事件但未提供onThinking回调');
                    }
                    break;
                  case 'progress':
                    if (callbacks.onProgress) {
                      console.log('处理progress事件:', data);
                      callbacks.onProgress(data);
                    }
                    break;
                  case 'partial':
                    if (callbacks.onPartial) {
                      console.log('处理partial事件:', data);
                      callbacks.onPartial(data);
                    }
                    break;
                  case 'final':
                    if (callbacks.onFinal) {
                      console.log('处理final事件:', data);
                      callbacks.onFinal(data);
                    }
                    break;
                  case 'error':
                    if (callbacks.onError) {
                      console.log('处理error事件:', data.error || { message: '未知错误' });
                      callbacks.onError(data.error || { message: '未知错误' });
                    }
                    break;
                  default:
                    console.warn('未知事件类型:', eventData.type);
                }
              } catch (parseError) {
                console.error(`解析事件数据失败: ${parseError}`);
              }
            } catch (error) {
              console.error(`处理SSE事件时出错: ${error}`);
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

  /**
   * 添加状态更新监听器
   * @param {Function} listener - 状态更新监听器函数，接收(type, data)参数
   * @returns {Function} 移除监听器的函数
   */
  addStatusListener(listener) {
    if (typeof listener !== 'function') {
      console.error('状态监听器必须是函数');
      return () => {};
    }
    
    this.statusListeners.push(listener);
    console.log(`添加状态监听器，当前监听器数量: ${this.statusListeners.length}`);
    
    // 返回移除监听器的函数
    return () => {
      const index = this.statusListeners.indexOf(listener);
      if (index !== -1) {
        this.statusListeners.splice(index, 1);
        console.log(`移除状态监听器，剩余监听器数量: ${this.statusListeners.length}`);
      }
    };
  }
  
  /**
   * 添加工具结果监听器
   * @param {string} toolName - 要监听的工具名称
   * @param {Function} listener - 结果监听器函数，接收(result)参数
   * @returns {Function} 移除监听器的函数
   */
  addToolResultListener(toolName, listener) {
    if (typeof listener !== 'function') {
      console.error('工具结果监听器必须是函数');
      return () => {};
    }
    
    if (!this.toolResultListeners.has(toolName)) {
      this.toolResultListeners.set(toolName, []);
    }
    
    const listeners = this.toolResultListeners.get(toolName);
    listeners.push(listener);
    console.log(`添加工具[${toolName}]结果监听器，当前监听器数量: ${listeners.length}`);
    
    // 返回移除监听器的函数
    return () => {
      const listeners = this.toolResultListeners.get(toolName);
      if (!listeners) return;
      
      const index = listeners.indexOf(listener);
      if (index !== -1) {
        listeners.splice(index, 1);
        console.log(`移除工具[${toolName}]结果监听器，剩余监听器数量: ${listeners.length}`);
      }
    };
  }
  
  /**
   * 触发状态更新通知
   * @private
   */
  _notifyStatusListeners(type, data) {
    for (const listener of this.statusListeners) {
      try {
        listener(type, data);
      } catch (error) {
        console.error('调用状态监听器出错:', error);
      }
    }
  }
  
  /**
   * 触发工具结果通知
   * @private
   */
  _notifyToolResultListeners(toolName, result) {
    // 通知特定工具的监听器
    const listeners = this.toolResultListeners.get(toolName);
    if (listeners && listeners.length > 0) {
      for (const listener of listeners) {
        try {
          listener(result);
        } catch (error) {
          console.error(`调用工具[${toolName}]结果监听器出错:`, error);
        }
      }
    }
  }

  /**
   * 处理SSE事件数据（streaming模式）
   * @param {string} chunk - SSE事件数据
   */
  handleSSEChunk(chunk) {
    try {
      // 检查chunk是否为空
      if (!chunk || chunk.trim() === '') {
        console.log('收到空的SSE数据块，忽略');
        return;
      }

      console.log(`收到WebSocket数据块: ${chunk}`);

      // 解析SSE事件，处理多个事件
      const events = this._parseSSEEvents(chunk);
      console.log(`处理 ${events.length} 个SSE事件`);

      // 处理每个事件
      events.forEach(event => {
        // 解析事件数据为JSON
        try {
          let eventData = null;
          try {
            eventData = JSON.parse(event);
            console.log(`解析事件数据: ${JSON.stringify(eventData)}`);
          } catch (parseError) {
            console.error(`解析SSE事件数据失败: ${parseError}`);
            return; // 跳过此事件
          }
          
          // 检查事件类型
          const eventType = eventData.type;
          console.log(`收到流式事件类型: ${eventType}`);

          // 根据事件类型处理
          if (eventType === 'thinking') {
            // 处理思考过程事件
            if (this.callbacks.onThinking) {
              console.log('收到thinking事件:', {
                type: eventData.stage || 'thinking',
                content: eventData.content || '',
                progress: eventData.progress || 0,
                stage: eventData.stage || 'thinking'
              });
              
              this.callbacks.onThinking({
                type: eventData.stage || 'thinking',
                content: eventData.content || '',
                progress: eventData.progress || 0,
                stage: eventData.stage || 'thinking'
              });
            }
          } else if (eventType === 'progress') {
            // 进度更新事件
            this._handleProgressEvent(eventData);
          } else if (eventType === 'partial') {
            // 部分结果事件
            this._handlePartialEvent(eventData);
          } else if (eventType === 'final') {
            // 最终结果事件
            this._handleFinalEvent(eventData);
          } else if (eventType === 'error') {
            // 错误事件
            this._handleErrorEvent(eventData);
          } else {
            console.warn(`未知的事件类型: ${eventType}`);
          }
        } catch (error) {
          console.error(`处理SSE事件时出错: ${error}`);
        }
      });
    } catch (error) {
      console.error(`处理SSE数据块时出错: ${error}`);
    }
  }

  /**
   * 解析SSE事件数据流
   * @private
   * @param {string} chunk - SSE事件数据流
   * @returns {Array} 解析后的事件数据数组
   */
  _parseSSEEvents(chunk) {
    try {
      console.log('开始解析SSE事件数据流:', chunk);
      
      // 处理不同格式的事件数据
      if (chunk.startsWith('data:')) {
        // 标准SSE格式，数据行以data:开头
        const events = [];
        const lines = chunk.split('\n');
        let currentEvent = '';
        
        for (const line of lines) {
          if (line.startsWith('data:')) {
            // 提取data:后面的内容
            const data = line.substring(5).trim();
            currentEvent += data;
          } else if (line.trim() === '') {
            // 空行表示事件结束
            if (currentEvent) {
              try {
                events.push(JSON.parse(currentEvent));
                console.log('解析到SSE事件:', currentEvent);
              } catch (e) {
                console.warn('事件数据不是有效的JSON:', currentEvent);
              }
              currentEvent = '';
            }
          }
        }
        
        // 处理最后一个事件（如果有）
        if (currentEvent) {
          try {
            events.push(JSON.parse(currentEvent));
            console.log('解析到最后一个SSE事件:', currentEvent);
          } catch (e) {
            console.warn('最后一个事件数据不是有效的JSON:', currentEvent);
          }
        }
        
        console.log(`共解析到 ${events.length} 个SSE事件`);
        return events;
      } else {
        // 尝试直接解析为JSON
        try {
          const jsonData = JSON.parse(chunk);
          console.log('直接解析为JSON成功:', jsonData);
          return [jsonData];
        } catch (e) {
          console.warn('数据块不是有效的JSON，尝试其他解析方式');
          
          // 尝试将数据分割为多个JSON对象
          const jsonObjects = [];
          let bracketCount = 0;
          let currentObject = '';
          
          for (let i = 0; i < chunk.length; i++) {
            const char = chunk[i];
            currentObject += char;
            
            if (char === '{') {
              bracketCount++;
            } else if (char === '}') {
              bracketCount--;
              
              // 当括号匹配时，可能是一个完整的JSON对象
              if (bracketCount === 0 && currentObject.trim() !== '') {
                try {
                  const jsonObj = JSON.parse(currentObject);
                  jsonObjects.push(jsonObj);
                  console.log('解析到JSON对象:', currentObject);
                  currentObject = '';
                } catch (e) {
                  // 不是有效的JSON，继续
                }
              }
            }
          }
          
          if (jsonObjects.length > 0) {
            console.log(`解析到 ${jsonObjects.length} 个JSON对象`);
            return jsonObjects;
          }
          
          // 都失败了，返回空数组
          console.warn('无法解析数据块为任何有效的事件');
          return [];
        }
      }
    } catch (error) {
      console.error('解析SSE事件数据流出错:', error);
      return [];
    }
  }

  /**
   * 处理进度更新事件
   * @private
   * @param {Object} eventData - 事件数据
   */
  _handleProgressEvent(eventData) {
    console.log('处理进度更新事件:', eventData);
    if (this.callbacks.onProgress) {
      try {
        const progressData = eventData.data || eventData;
        this.callbacks.onProgress(progressData);
        
        // 同时通知状态监听器
        this._notifyStatusListeners('status_update', progressData);
        
        // 如果这是工具进度更新，通知对应的工具监听器
        if (eventData.tool) {
          this._notifyToolResultListeners(eventData.tool, progressData);
        }
      } catch (error) {
        console.error('处理进度事件出错:', error);
      }
    }
  }

  /**
   * 处理部分结果事件
   * @private
   * @param {Object} eventData - 事件数据
   */
  _handlePartialEvent(eventData) {
    console.log('处理部分结果事件:', eventData);
    if (this.callbacks.onPartial) {
      try {
        const partialData = eventData.data || eventData;
        this.callbacks.onPartial(partialData);
        
        // 如果这是工具部分结果，通知对应的工具监听器
        if (eventData.tool) {
          this._notifyToolResultListeners(eventData.tool, partialData);
        }
      } catch (error) {
        console.error('处理部分结果事件出错:', error);
      }
    }
  }

  /**
   * 处理最终结果事件
   * @private
   * @param {Object} eventData - 事件数据
   */
  _handleFinalEvent(eventData) {
    console.log('处理最终结果事件:', eventData);
    if (this.callbacks.onFinal) {
      try {
        const finalData = eventData.data || eventData;
        this.callbacks.onFinal(finalData);
        
        // 如果这是工具最终结果，通知对应的工具监听器
        if (eventData.tool) {
          this._notifyToolResultListeners(eventData.tool, finalData);
        }
      } catch (error) {
        console.error('处理最终结果事件出错:', error);
      }
    }
  }

  /**
   * 处理错误事件
   * @private
   * @param {Object} eventData - 事件数据
   */
  _handleErrorEvent(eventData) {
    console.log('处理错误事件:', eventData);
    if (this.callbacks.onError) {
      try {
        const errorData = eventData.error || eventData.data || { message: '未知错误' };
        this.callbacks.onError(errorData);
        
        // 如果这是工具错误，通知对应的工具监听器
        if (eventData.tool) {
          this._notifyToolResultListeners(eventData.tool, { error: errorData });
        }
      } catch (error) {
        console.error('处理错误事件出错:', error);
      }
    }
  }
} 