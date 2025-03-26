# MCP协议详解

本文档详细说明Model Context Protocol (MCP)的工作原理、连接流程和使用方法，以帮助开发者正确连接和使用Doris MCP服务器。

## 什么是MCP

Model Context Protocol (MCP) 是一种设计用于与AI模型服务器进行通信的协议。MCP使用Server-Sent Events (SSE) 作为底层通信机制，允许服务器向客户端推送实时更新，非常适合AI模型的流式输出。

MCP的主要特点：

1. **基于SSE的双向通信** - 客户端通过HTTP POST发送请求，服务器通过SSE推送响应
2. **会话管理** - 使用会话ID跟踪客户端状态
3. **结构化消息格式** - 使用JSON-RPC风格的请求和响应格式
4. **资源URI系统** - 使用URI标识符访问服务器资源

## 协议工作原理

### 1. 连接流程

MCP协议的连接流程如下：

1. 客户端向服务器的SSE端点（通常是`/sse`）发起GET请求，请求头中包含`Accept: text/event-stream`
2. 服务器建立SSE连接，并发送一个包含会话ID和消息URL的初始化事件
3. 客户端接收初始化事件，获取会话ID和消息URL
4. 客户端使用获取到的会话ID，向消息URL发送POST请求
5. 服务器通过已建立的SSE连接返回响应

### 2. 消息格式

MCP使用类似JSON-RPC的消息格式：

**请求格式**:
```json
{
  "id": "请求ID",
  "method": "方法名",
  "session_id": "会话ID",
  "params": {
    // 参数对象
  }
}
```

**响应格式**:
```json
{
  "id": "请求ID",
  "result": {
    // 结果对象
  }
}
```

**错误响应格式**:
```json
{
  "id": "请求ID",
  "error": {
    "code": 错误代码,
    "message": "错误信息"
  }
}
```

### 3. 事件类型

MCP协议中的SSE事件类型包括：

1. **init** - 初始化事件，包含会话ID和消息URL
2. **response** - 响应事件，包含对客户端请求的响应
3. **notification** - 通知事件，服务器主动发送的通知
4. **error** - 错误事件，表示服务器端发生错误

## 详细连接流程示例

下面是一个详细的MCP协议连接流程示例：

### 1. 客户端发起SSE连接

**HTTP请求**:
```
GET /sse HTTP/1.1
Host: localhost:3000
Accept: text/event-stream
```

### 2. 服务器发送初始化事件

**SSE响应**:
```
event: init
data: {"session_id": "abcd1234", "message_url": "http://localhost:3000/message"}

```

### 3. 客户端发送请求

**HTTP请求**:
```
POST /message HTTP/1.1
Host: localhost:3000
Content-Type: application/json

{
  "id": "req-001",
  "method": "ping",
  "session_id": "abcd1234"
}
```

### 4. 服务器通过SSE返回响应

**SSE事件**:
```
event: response
data: {"id": "req-001", "result": {"status": "ok", "time": "2023-07-01T12:34:56Z"}}

```

## 常见方法和资源

MCP协议支持以下常见方法：

1. **ping** - 测试服务器连接
2. **prompts/list** - 获取可用的提示模板列表
3. **resources/templates/list** - 获取可用的资源模板列表
4. **resource** - 访问服务器资源，需要提供`uri`参数

资源URI格式为`协议://路径`，例如：

- `doris://database/info` - 获取数据库信息
- `schema://ssb/customer` - 获取表结构
- `docs://guide/getting-started` - 获取入门指南文档

## 使用JavaScript实现客户端

下面是一个使用JavaScript实现的简单MCP客户端示例：

```javascript
class MCPClient {
  constructor(serverUrl) {
    this.serverUrl = serverUrl;
    this.sseUrl = `${serverUrl}/sse`;
    this.messageUrl = null;
    this.sessionId = null;
    this.eventSource = null;
    this.callbacks = new Map();
    this.connected = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      // 创建SSE连接
      this.eventSource = new EventSource(this.sseUrl);
      
      // 设置超时
      const timeout = setTimeout(() => {
        this.disconnect();
        reject(new Error('连接超时'));
      }, 5000);
      
      // 处理初始化事件
      this.eventSource.addEventListener('init', (event) => {
        clearTimeout(timeout);
        const data = JSON.parse(event.data);
        this.sessionId = data.session_id;
        this.messageUrl = data.message_url || `${this.serverUrl}/message`;
        this.connected = true;
        resolve();
      });
      
      // 处理响应事件
      this.eventSource.addEventListener('response', (event) => {
        const response = JSON.parse(event.data);
        const callback = this.callbacks.get(response.id);
        if (callback) {
          this.callbacks.delete(response.id);
          callback.resolve(response.result || response);
        }
      });
      
      // 处理错误事件
      this.eventSource.addEventListener('error', (event) => {
        if (this.eventSource.readyState === 2) { // CLOSED
          this.connected = false;
          reject(new Error('SSE连接错误'));
        }
      });
    });
  }
  
  request(method, params = {}) {
    if (!this.connected) {
      return Promise.reject(new Error('未连接到服务器'));
    }
    
    // 生成请求ID
    const id = `req-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    
    // 构建请求体
    const requestBody = {
      id,
      method,
      session_id: this.sessionId
    };
    
    if (Object.keys(params).length > 0) {
      requestBody.params = params;
    }
    
    // 返回Promise
    return new Promise((resolve, reject) => {
      this.callbacks.set(id, { resolve, reject });
      
      // 发送请求
      fetch(this.messageUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
      }).catch(error => {
        this.callbacks.delete(id);
        reject(error);
      });
      
      // 设置超时
      setTimeout(() => {
        if (this.callbacks.has(id)) {
          this.callbacks.delete(id);
          reject(new Error('请求超时'));
        }
      }, 30000);
    });
  }
  
  disconnect() {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
    this.connected = false;
    this.sessionId = null;
    this.callbacks.clear();
  }
}

// 使用示例
async function example() {
  const client = new MCPClient('http://localhost:3000');
  
  try {
    await client.connect();
    console.log('已连接到服务器');
    
    // 发送ping请求
    const pingResult = await client.request('ping');
    console.log('Ping结果:', pingResult);
    
    // 获取数据库信息
    const dbInfo = await client.request('resource', { uri: 'doris://database/info' });
    console.log('数据库信息:', dbInfo);
    
  } catch (error) {
    console.error('错误:', error);
  } finally {
    client.disconnect();
  }
}

example();
```

## 使用Python实现客户端

下面是一个使用Python实现的MCP客户端示例：

```python
import json
import time
import uuid
import threading
import requests
import sseclient

class MCPClient:
    def __init__(self, server_url):
        self.server_url = server_url.rstrip('/')
        self.sse_url = f"{self.server_url}/sse"
        self.session_id = None
        self.message_url = None
        self.sse_connection = None
        self.sse_client = None
        self.connected = False
        self.callbacks = {}
        self.sse_thread = None
    
    def connect(self):
        try:
            # 创建SSE连接
            headers = {"Accept": "text/event-stream"}
            self.sse_connection = requests.get(
                self.sse_url, 
                headers=headers, 
                stream=True
            )
            
            if self.sse_connection.status_code != 200:
                raise Exception(f"SSE连接失败: {self.sse_connection.status_code}")
            
            # 创建SSE客户端
            self.sse_client = sseclient.SSEClient(self.sse_connection)
            
            # 启动监听线程
            self.sse_thread = threading.Thread(
                target=self._listen_sse, 
                daemon=True
            )
            self.sse_thread.start()
            
            # 等待连接初始化
            timeout = time.time() + 5  # 5秒超时
            while not self.connected and time.time() < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise Exception("连接超时")
            
            return True
            
        except Exception as e:
            self.disconnect()
            raise e
    
    def _listen_sse(self):
        try:
            for event in self.sse_client:
                if event.event == "init":
                    # 处理初始化事件
                    data = json.loads(event.data)
                    self.session_id = data.get("session_id")
                    self.message_url = data.get("message_url") or f"{self.server_url}/message"
                    self.connected = True
                    
                elif event.event == "response":
                    # 处理响应事件
                    data = json.loads(event.data)
                    request_id = data.get("id")
                    
                    if request_id in self.callbacks:
                        # 调用回调函数
                        callback = self.callbacks.pop(request_id)
                        callback(data)
        except:
            self.connected = False
    
    def request(self, method, params=None, timeout=30):
        if not self.connected or not self.session_id:
            raise Exception("未连接到服务器")
        
        if params is None:
            params = {}
        
        # 生成请求ID
        request_id = str(uuid.uuid4())
        
        # 创建请求体
        request_body = {
            "id": request_id,
            "method": method,
            "session_id": self.session_id
        }
        
        if params:
            request_body["params"] = params
        
        # 结果占位符
        result = None
        
        # 创建事件以等待响应
        response_event = threading.Event()
        
        # 定义回调
        def on_response(response):
            nonlocal result
            result = response
            response_event.set()
        
        # 注册回调
        self.callbacks[request_id] = on_response
        
        # 发送请求
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.message_url, 
                headers=headers, 
                json=request_body
            )
            
            if response.status_code != 202:
                # 如果不是202 Accepted，可能是直接响应
                try:
                    result = response.json()
                    response_event.set()
                except:
                    raise Exception(f"请求失败: {response.status_code}")
                
        except Exception as e:
            self.callbacks.pop(request_id, None)
            raise e
        
        # 等待响应
        if not response_event.wait(timeout):
            self.callbacks.pop(request_id, None)
            raise Exception(f"请求超时: {method}")
        
        # 检查错误
        if result and "error" in result:
            raise Exception(f"请求错误: {result.get('error')}")
        
        return result.get("result", result)
    
    def disconnect(self):
        self.connected = False
        
        if self.sse_connection:
            try:
                self.sse_connection.close()
            except:
                pass
            self.sse_connection = None
        
        self.sse_client = None
        self.session_id = None
        self.callbacks = {}
```

## 常见问题和解决方法

### 1. 连接失败

如果连接失败，请检查以下几点：

1. 确保服务器正在运行并且可以访问
2. 检查是否使用了正确的服务器URL
3. 确保客户端支持SSE连接
4. 检查网络环境是否支持持久连接

### 2. 请求超时

请求超时通常有以下原因：

1. 服务器处理请求时间过长
2. 网络连接不稳定
3. 服务器未正确响应请求

解决方法：

1. 增加请求超时时间
2. 检查服务器日志查找问题
3. 尝试简化请求减少处理时间

### 3. SSE连接断开

SSE连接断开可能因为：

1. 长时间没有活动
2. 网络连接问题
3. 服务器端关闭连接

解决方法：

1. 实现重连机制
2. 定期发送ping请求保持连接活跃
3. 在连接断开时自动重新连接

## 总结

MCP协议是一种基于SSE的双向通信协议，专为AI模型服务器设计。使用MCP协议时，客户端需要先建立SSE连接，然后通过HTTP POST发送请求，服务器通过SSE连接返回响应。

关键步骤：
1. 建立SSE连接
2. 获取会话ID
3. 使用会话ID发送请求
4. 通过SSE接收响应

遵循这些步骤，可以成功与Doris MCP服务器进行通信，获取数据库信息、表结构等资源，以及使用服务器提供的各种功能。 