import { MCPClient } from '../utils/mcp-client';

export class MCPService {
  constructor() {
    this.client = null;
    this.connected = false;
    this.tools = [];
    this.resources = [];
    this.prompts = [];
  }

  /**
   * 连接到MCP服务器
   * @param {string} serverUrl - 服务器URL
   */
  async connect(serverUrl) {
    try {
      // 生成客户端ID
      const clientId = 'client_' + Math.random().toString(36).substring(2, 8);
      
      // 创建客户端实例
      this.client = new MCPClient(serverUrl, clientId);
      
      // 连接到服务器
      await this.client.connect();
      
      // 获取工具列表
      await this.fetchTools();
      
      this.connected = true;
      console.log('已连接到MCP服务');
      
      return true;
    } catch (error) {
      console.error('连接到MCP服务失败:', error);
      this.connected = false;
      throw error;
    }
  }

  /**
   * 断开连接
   */
  disconnect() {
    if (this.client) {
      this.client.disconnect();
      this.client = null;
    }
    this.connected = false;
    this.tools = [];
    this.resources = [];
    this.prompts = [];
  }

  /**
   * 获取工具列表
   */
  async fetchTools() {
    try {
      const response = await this.client.listTools();
      this.tools = response.result.tools || [];
      return this.tools;
    } catch (error) {
      console.error('获取工具列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取资源列表
   */
  async fetchResources() {
    try {
      const response = await this.client.listResources();
      this.resources = response.result.resources || [];
      return this.resources;
    } catch (error) {
      console.error('获取资源列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取提示模板列表
   */
  async fetchPrompts() {
    try {
      const response = await this.client.listPrompts();
      this.prompts = response.result.prompts || [];
      return this.prompts;
    } catch (error) {
      console.error('获取提示模板列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取LLM提供商列表
   */
  async listLLMProviders() {
    try {
      const response = await this.client.callTool('list_llm_providers');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取LLM提供商列表失败:', error);
      throw error;
    }
  }

  /**
   * 设置LLM提供商
   * @param {string} provider - 提供商名称
   */
  async setLLMProvider(provider) {
    try {
      const response = await this.client.callTool('set_llm_provider', { provider });
      return response.result.content[0].text;
    } catch (error) {
      console.error('设置LLM提供商失败:', error);
      throw error;
    }
  }

  /**
   * 执行自然语言到SQL的查询
   * @param {string} query - 自然语言查询
   */
  async nl2sqlQuery(query) {
    try {
      const response = await this.client.callTool('mcp_doris_nl2sql_query', { query });
      return response.result.content[0].text;
    } catch (error) {
      console.error('执行NL2SQL查询失败:', error);
      throw error;
    }
  }

  /**
   * 执行自然语言到SQL的流式查询
   * @param {string} query - 自然语言查询
   */
  async nl2sqlQueryStream(query) {
    try {
      const response = await this.client.callTool('mcp_doris_nl2sql_query_stream', { query });
      return response.result.content[0].text;
    } catch (error) {
      console.error('执行NL2SQL流式查询失败:', error);
      throw error;
    }
  }

  /**
   * 获取数据库表列表
   */
  async listDatabaseTables() {
    try {
      const response = await this.client.callTool('mcp_doris_list_database_tables');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取数据库表列表失败:', error);
      throw error;
    }
  }

  /**
   * 获取表结构说明
   */
  async explainTable() {
    try {
      const response = await this.client.callTool('mcp_doris_explain_table');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取表结构说明失败:', error);
      throw error;
    }
  }

  /**
   * 获取业务概览
   */
  async getBusinessOverview() {
    try {
      const response = await this.client.callTool('mcp_doris_get_business_overview');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取业务概览失败:', error);
      throw error;
    }
  }

  /**
   * 刷新元数据
   */
  async refreshMetadata() {
    try {
      const response = await this.client.callTool('mcp_doris_refresh_metadata');
      return response.result.content[0].text;
    } catch (error) {
      console.error('刷新元数据失败:', error);
      throw error;
    }
  }

  /**
   * SQL优化分析
   */
  async sqlOptimize() {
    try {
      const response = await this.client.callTool('mcp_doris_sql_optimize');
      return response.result.content[0].text;
    } catch (error) {
      console.error('SQL优化分析失败:', error);
      throw error;
    }
  }

  /**
   * 修复SQL
   */
  async fixSQL() {
    try {
      const response = await this.client.callTool('mcp_doris_fix_sql');
      return response.result.content[0].text;
    } catch (error) {
      console.error('修复SQL失败:', error);
      throw error;
    }
  }

  /**
   * 获取NL2SQL状态
   */
  async getNL2SQLStatus() {
    try {
      const response = await this.client.callTool('mcp_doris_get_nl2sql_status');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取NL2SQL状态失败:', error);
      throw error;
    }
  }

  /**
   * 获取服务器状态
   */
  async getStatus() {
    try {
      const response = await this.client.callTool('mcp_doris_status');
      return response.result.content[0].text;
    } catch (error) {
      console.error('获取服务器状态失败:', error);
      throw error;
    }
  }

  /**
   * 健康检查
   */
  async health() {
    try {
      const response = await this.client.callTool('mcp_doris_health');
      return response.result.content[0].text;
    } catch (error) {
      console.error('健康检查失败:', error);
      throw error;
    }
  }
} 