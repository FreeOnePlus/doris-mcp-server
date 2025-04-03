import { JsonRpcMessage } from './jsonrpc';

export class MCPClient {
  constructor(serverUrl, clientId) {
    this.serverUrl = serverUrl;
    this.clientId = clientId;
    this.sessionId = null;
    this.eventSource = null;
    this.messageEndpoint = null;
    this.connected = false;
    this.handlers = new Map();
    this.waitingResponses = {};
    this.lastHeartbeat = null;
    this.debug = false;
    
    console.log('MCP客户端初始化:', serverUrl, 'clientId:', clientId);
  }

  /**
   * 连接到MCP服务器
   */
  connect() {
    if (this.connected) {
      console.log('已经连接到MCP服务器');
      return Promise.resolve();
    }

    console.log('连接到MCP服务器:', this.serverUrl);
    
    return new Promise((resolve, reject) => {
      // 健康检查
      const healthUrl = `${this.serverUrl}/status`;
      console.log('尝试健康检查:', healthUrl);
      
      fetch(healthUrl)
        .then(response => {
          if (!response.ok) {
            throw new Error(`健康检查失败: ${response.status}`);
          }
          return response.json();
        })
        .then(data => {
          console.log('服务器健康检查通过');
          
          // 建立SSE连接
          const sseUrl = `${this.serverUrl}/mcp`;
          console.log('尝试建立SSE连接:', sseUrl);
          
          // 使用自定义fetch实现替代EventSource
          this._createCustomEventSource(sseUrl, {
            onOpen: () => {
              console.log('SSE连接已打开');
            },
            onMessage: (data) => {
              try {
                // 检查数据是否为空
                if (!data || data.trim() === '') {
                  return;
                }
                
                // 解析JSON数据
                const parsedData = JSON.parse(data);
                console.log('收到SSE消息:', parsedData);
                
                // 处理不同类型的消息
                if (parsedData.type === 'endpoint') {
                  // 处理端点消息
                  hasReceivedEndpoint = true;
                  const endpoint = parsedData.endpoint;
                  console.log('收到endpoint消息:', endpoint);
                  
                  const sessionId = this._extractSessionId(endpoint);
                  console.log('SSE连接成功: 会话ID=' + sessionId);
                  
                  // 设置消息端点
                  this.messageEndpoint = `${this.serverUrl}${endpoint}`;
                  console.log('消息端点:', this.messageEndpoint);
                  
                  // 存储会话ID
                  this.sessionId = sessionId;
                  
                  // 初始化连接
                  this.connected = true;
                  this.initialize().then(() => {
                    resolve();
                  }).catch(error => {
                    console.error('初始化失败:', error);
                    reject(error);
                  });
                } else if (parsedData.type === 'heartbeat') {
                  // 处理心跳消息
                  this._handleHeartbeat(parsedData.timestamp);
                } else {
                  // 处理常规消息
                  this._handleMessage(parsedData);
                }
              } catch (error) {
                console.error('解析SSE消息失败:', error, data);
              }
            },
            onError: (error) => {
              console.log('SSE连接错误:', error);
              
              // 如果已经收到了endpoint并成功建立了连接，则错误可能是暂时性的
              if (hasReceivedEndpoint) {
                console.log('SSE连接已建立但收到错误，可能是暂时性问题');
                return;
              }
              
              // 如果未收到endpoint，尝试重连
              retryCount++;
              if (retryCount <= MAX_RETRIES) {
                console.log(`SSE连接错误，尝试重连 (${retryCount}/${MAX_RETRIES})...`);
                // 这里不需要做任何事情，_createCustomEventSource会自动重连
              } else {
                console.error('SSE连接失败，已达到最大重试次数');
                reject(new Error('SSE连接失败，已达到最大重试次数'));
              }
            }
          });
        })
        .catch(error => {
          console.error('连接失败:', error);
          reject(error);
        });
    });
  }

  /**
   * 断开与MCP服务器的连接
   */
  disconnect() {
    if (!this.connected) {
      console.log('未连接到MCP服务器');
      return;
    }
    
    console.log('断开与MCP服务器的连接');
    
    // 关闭EventSource
    if (this.eventSource) {
      console.log('关闭EventSource');
      this.eventSource.close();
      this.eventSource = null;
    }
    
    // 关闭自定义事件源
    if (this._closeEventSource) {
      console.log('关闭自定义事件源');
      this._closeEventSource();
      this._closeEventSource = null;
    }
    
    // 重置连接状态
    this.connected = false;
    this.messageEndpoint = null;
    this.sessionId = null;
  }

  /**
   * 初始化连接
   */
  async initialize() {
    try {
      const message = JsonRpcMessage.createInitializeRequest();
      const response = await this.call(message);
      console.log('初始化请求发送成功');
      return response;
    } catch (error) {
      console.error('初始化请求失败:', error);
      throw error;
    }
  }

  /**
   * 调用工具
   * @param {string} toolName - 工具名称
   * @param {object} params - 工具参数
   */
  async callTool(toolName, params = {}) {
    console.log('尝试调用工具:', toolName, params);
    try {
      const message = JsonRpcMessage.createToolCallRequest(toolName, params);
      message.type = 'tool';
      message.tool = toolName;
      message.arguments = params;
      const response = await this.call(message);
      return response;
    } catch (error) {
      console.error('调用工具失败:', error);
      throw error;
    }
  }

  /**
   * 获取工具列表
   */
  async listTools() {
    try {
      const message = JsonRpcMessage.createListToolsRequest();
      message.type = 'tool';
      message.tool = 'list_tools';
      const response = await this.call(message);
      return response;
    } catch (error) {
      console.error('获取工具列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取资源列表
   */
  async listResources() {
    try {
      const message = JsonRpcMessage.createListResourcesRequest();
      const response = await this.call(message);
      return response;
    } catch (error) {
      console.error('获取资源列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取提示模板列表
   */
  async listPrompts() {
    try {
      const message = JsonRpcMessage.createListPromptsRequest();
      const response = await this.call(message);
      return response;
    } catch (error) {
      console.error('获取提示模板列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取功能列表
   */
  async listOfferings() {
    try {
      const message = JsonRpcMessage.createListOfferingsRequest();
      const response = await this.call(message);
      return response;
    } catch (error) {
      console.error('获取功能列表失败:', error);
      throw error;
    }
  }

  /**
   * 发送请求到服务器
   * @param {object} message - 请求消息
   */
  async call(message) {
    if (!this.connected || !this.messageEndpoint) {
      throw new Error('未连接到服务器');
    }

    console.log('调用MCP方法:', message.method, message.params);
    console.log('发送请求到:', this.messageEndpoint, message);

    try {
      // 构造兼容两种格式的请求体
      const requestBody = {
        // JSON-RPC 2.0 格式
        jsonrpc: "2.0",
        id: message.id,
        method: message.method,
        params: message.params || {},
        // 额外的字段，支持旧格式
        session_id: this.sessionId,
        type: message.type || 'jsonrpc',
        tool: message.tool || message.method,
      };

      // 使用原始JSON-RPC格式发送请求
      const response = await fetch(this.messageEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        credentials: 'same-origin', // 改为same-origin
        mode: 'cors',
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP错误: ${response.status}, ${errorText}`);
      }

      const data = await response.json();
      console.log("收到原始响应:", JSON.stringify(data));
      
      // 处理响应，兼容不同格式
      let result = data;
      if (data.success === false && data.error) {
        throw new Error(data.error);
      } else if (data.error) {
        throw new Error(data.error.message || '未知错误');
      }
      
      console.log("收到MCP响应:", result);
      return result;
    } catch (error) {
      console.error('调用方法失败:', error);
      throw error;
    }
  }

  /**
   * 处理接收到的消息
   * @param {object} message - 接收到的消息
   */
  _handleMessage(message) {
    // 处理不同类型的消息
    if (message.method === 'notifications/initialized') {
      // 处理初始化完成通知
      console.log('收到初始化完成通知');
    } else if (message.result) {
      // 处理正常响应
      console.log('收到响应:', message);
    } else if (message.error) {
      // 处理错误响应
      console.error('收到错误:', message.error);
    }

    // 如果有消息处理器，则调用它
    if (this.onMessage) {
      try {
        this.onMessage(message);
      } catch (error) {
        console.error('调用消息处理器时出错:', error);
      }
    }
    
    // 如果是流式响应的部分结果
    if (message.partial && message.id && this.waitingResponses[message.id]) {
      const { onPartialResult } = this.waitingResponses[message.id];
      if (onPartialResult && typeof onPartialResult === 'function') {
        onPartialResult(message.result);
      }
      return; // 不继续处理，等待完整响应
    }
    
    // 如果是响应消息，查找该消息ID的等待队列
    if (message && message.id && this.waitingResponses[message.id]) {
      const { resolve, reject } = this.waitingResponses[message.id];
      
      // 检查消息是否成功
      if (message.error) {
        // 如果有错误，拒绝Promise
        reject(new Error(message.error));
      } else {
        // 否则解析Promise
        resolve(message);
      }
      
      // 从等待队列中删除
      delete this.waitingResponses[message.id];
    }
  }

  /**
   * 处理SSE心跳事件
   * @private
   * @param {Object} data - 心跳数据
   */
  _handleHeartbeat(timestamp) {
    // 更新最后活动时间
    this.lastHeartbeat = timestamp;
    
    // 打印调试信息（可选）
    if (this.debug) {
      console.debug(`收到SSE心跳: ${new Date(timestamp * 1000).toISOString()}`);
    }
  }

  /**
   * 从endpoint提取会话ID
   * @private
   * @param {string} endpoint - 端点URL
   * @returns {string} 会话ID
   */
  _extractSessionId(endpoint) {
    try {
      // 检查是否包含session_id参数
      if (endpoint.includes('session_id=')) {
        const url = new URL(endpoint, 'http://example.com');
        return url.searchParams.get('session_id');
      } else {
        // 如果endpoint格式不包含session_id，可能是其他格式
        const match = endpoint.match(/\/([a-f0-9-]{36})/);
        return match ? match[1] : null;
      }
    } catch (error) {
      console.error('提取会话ID失败:', error);
      return null;
    }
  }

  /**
   * 流式调用工具
   * @param {string} toolName - 工具名称
   * @param {object} params - 工具参数
   * @param {function} onPartialResult - 部分结果的回调函数
   */
  async streamCallTool(toolName, params = {}, onPartialResult = null) {
    if (!this.connected) {
      throw new Error('未连接到MCP服务器');
    }
    
    console.log('尝试流式调用工具:', toolName, params);
    
    try {
      // 生成请求ID
      const requestId = `${Date.now()}-${Math.floor(Math.random() * 1000)}`;
      
      // 构造请求体 - 兼容两种格式
      const requestBody = {
        // JSON-RPC 2.0 格式
        jsonrpc: "2.0",
        id: requestId,
        method: "mcp/callTool",
        params: {
          name: toolName,
          arguments: params
        },
        // 额外的字段
        session_id: this.sessionId,
        type: 'tool',
        tool: toolName,
        arguments: params,
        stream: true
      };
      
      // 构造流式请求URL
      const streamUrl = `${this.messageEndpoint}&stream=true`;
      console.log('发送流式请求到:', streamUrl, requestBody);
      
      // 发送流式请求
      const response = await fetch(streamUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Origin': window.location.origin
        },
        credentials: 'include',
        mode: 'cors',
        body: JSON.stringify(requestBody)
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP错误: ${response.status}, ${errorText}`);
      }
      
      // 读取响应
      const responseData = await response.json();
      console.log('收到流式请求初始响应:', responseData);
      
      // 存储请求ID，用于接收SSE事件
      this.streamRequests = this.streamRequests || new Set();
      this.streamRequests.add(requestId);
      
      // 创建Promise用于等待流式响应完成
      return new Promise((resolve, reject) => {
        // 存储最终结果
        let finalResult = null;
        
        // 存储解析和拒绝函数
        this.waitingResponses[requestId] = {
          resolve: (result) => {
            // 移除请求ID
            this.streamRequests.delete(requestId);
            // 调用外部解析函数
            resolve(result);
          },
          reject: (error) => {
            // 移除请求ID
            this.streamRequests.delete(requestId);
            // 调用外部拒绝函数
            reject(error);
          },
          onPartialResult: (data) => {
            // 如果有部分结果回调，则调用它
            if (onPartialResult && typeof onPartialResult === 'function') {
              onPartialResult(data);
            }
          }
        };
        
        // 设置超时，防止永久等待
        setTimeout(() => {
          if (this.streamRequests.has(requestId)) {
            this.streamRequests.delete(requestId);
            if (this.waitingResponses[requestId]) {
              delete this.waitingResponses[requestId];
              reject(new Error('流式请求超时'));
            }
          }
        }, 60000); // 60秒超时
      });
    } catch (error) {
      console.error('流式调用工具失败:', error);
      throw error;
    }
  }

  /**
   * 创建自定义的事件源，用于替代标准的EventSource
   * @private
   * @param {string} url - 事件流URL
   * @param {object} handlers - 事件处理程序
   * @param {function} handlers.onOpen - 连接打开时的回调
   * @param {function} handlers.onMessage - 收到消息时的回调
   * @param {function} handlers.onError - 出错时的回调
   */
  _createCustomEventSource(url, handlers) {
    // 最大重试次数
    const MAX_RETRIES = 3;
    let retryCount = 0;
    let hasReceivedEndpoint = false;
    
    // 存储处理程序
    this._eventSourceHandlers = handlers;
    
    // 存储重连超时ID
    this._eventSourceReconnectTimeout = null;
    
    // 存储是否关闭
    this._eventSourceClosed = false;
    
    // 连接函数
    const connect = async () => {
      if (this._eventSourceClosed) {
        return;
      }
      
      try {
        // 发送请求
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'Accept': 'text/plain, text/event-stream',
            'Origin': window.location.origin
          },
          credentials: 'include',
          mode: 'cors'
        });
        
        // 检查响应状态
        if (!response.ok) {
          throw new Error(`HTTP错误: ${response.status}`);
        }
        
        // 获取响应体reader
        const reader = response.body.getReader();
        
        // 通知打开
        if (handlers.onOpen) {
          handlers.onOpen();
        }
        
        // 处理数据
        const textDecoder = new TextDecoder();
        let buffer = '';
        
        // 读取数据
        while (true) {
          const { done, value } = await reader.read();
          
          if (done) {
            // 流结束，尝试重连
            console.log('事件流已关闭，尝试重连');
            reconnect();
            break;
          }
          
          // 解码数据并添加到缓冲区
          buffer += textDecoder.decode(value, { stream: true });
          
          // 处理缓冲区中的行
          const lines = buffer.split('\n');
          
          // 保留最后一行（可能是不完整的）
          buffer = lines.pop() || '';
          
          // 处理每一行
          for (const line of lines) {
            const trimmedLine = line.trim();
            if (trimmedLine) {
              // 调用消息处理程序
              if (handlers.onMessage) {
                handlers.onMessage(trimmedLine);
              }
            }
          }
        }
      } catch (error) {
        console.error('事件流错误:', error);
        
        // 通知错误
        if (handlers.onError) {
          handlers.onError(error);
        }
        
        // 尝试重连
        reconnect();
      }
    };
    
    // 重连函数
    const reconnect = () => {
      if (this._eventSourceClosed) {
        return;
      }
      
      retryCount++;
      if (retryCount <= MAX_RETRIES) {
        // 设置重连超时
        const delay = Math.min(1000 * Math.pow(2, retryCount - 1), 30000); // 指数退避，最大30秒
        console.log(`将在 ${delay}ms 后重连`);
        
        this._eventSourceReconnectTimeout = setTimeout(() => {
          console.log('尝试重新连接事件流');
          connect();
        }, delay);
      } else {
        console.error('达到最大重试次数，放弃重连');
        
        // 通知错误
        if (handlers.onError) {
          const error = new Error('达到最大重试次数');
          handlers.onError(error);
        }
      }
    };
    
    // 创建关闭方法
    this._closeEventSource = () => {
      console.log('关闭自定义事件源');
      this._eventSourceClosed = true;
      
      // 清除重连超时
      if (this._eventSourceReconnectTimeout) {
        clearTimeout(this._eventSourceReconnectTimeout);
        this._eventSourceReconnectTimeout = null;
      }
    };
    
    // 开始连接
    connect();
  }
} 