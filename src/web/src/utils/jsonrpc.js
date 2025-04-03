/**
 * JSON-RPC 2.0 消息处理工具类
 */
export class JsonRpcMessage {
  static generateId() {
    return Math.random().toString(36).substring(2, 15);
  }

  /**
   * 创建一个初始化请求消息
   */
  static createInitializeRequest() {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "initialize",
      params: {}
    };
  }

  /**
   * 创建一个工具调用消息
   * @param {string} toolName - 工具名称
   * @param {object} args - 工具参数
   */
  static createToolCallRequest(toolName, args = {}) {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "mcp/callTool",
      params: {
        name: toolName,
        arguments: args
      }
    };
  }

  /**
   * 创建一个列出工具请求消息
   */
  static createListToolsRequest() {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "mcp/listTools",
      params: {}
    };
  }

  /**
   * 创建一个列出资源请求消息
   */
  static createListResourcesRequest() {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "mcp/listResources",
      params: {}
    };
  }

  /**
   * 创建一个列出提示模板请求消息
   */
  static createListPromptsRequest() {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "mcp/listPrompts",
      params: {}
    };
  }

  /**
   * 创建一个列出功能请求消息
   */
  static createListOfferingsRequest() {
    return {
      jsonrpc: "2.0",
      id: this.generateId(),
      method: "mcp/listOfferings",
      params: {}
    };
  }
} 