import { ref, computed } from 'vue';
import { useLangStore } from '../stores/langStore';

// 英文语言包
const enLocale = {
  common: {
    appTitle: 'NL2SQL Natural Language Query',
    debugMode: 'Debug Mode',
    connected: 'Connected',
    disconnected: 'Disconnected',
    reconnect: 'Reconnect',
    clearChat: 'Clear Chat',
    startChat: 'Start a conversation, enter your natural language query',
    send: 'Send',
    scrollToBottom: 'Scroll to Bottom',
    fullResults: 'Full Query Results'
  },
  query: {
    inputPlaceholder: 'Enter your natural language query, for example: "What are the three products with the highest sales in the first quarter of 2023?"',
    processing: 'Processing your query...',
    thinking: 'Thinking Process',
    expand: 'Expand',
    collapse: 'Collapse',
    sql: 'SQL Query',
    copy: 'Copy',
    queryResults: 'Query Results',
    records: 'records',
    businessAnalysis: 'Business Analysis',
    visualization: 'Visualization',
    trends: 'Main Trends',
    recommendations: 'Business Recommendations',
    error: 'Error',
    errorDetails: 'Error Details',
    serverDiagnosis: 'Server Problem Diagnosis',
    serverError: 'The server returned an invalid response format. Possible issues:',
    suggestions: 'Suggestions:',
    queryProcessingError: 'Error processing query',
    copySuccess: 'SQL copied to clipboard',
    copyFailed: 'Copy failed'
  },
  status: {
    currentStage: 'Current Stage',
    processingStatus: 'Processing Status',
    progress: 'Progress',
    query: 'Query',
    stageHistory: 'Stage History',
    analyzing: 'Analyzing',
    exampleAnalysis: 'Example Analysis',
    businessAnalysis: 'Business Analysis',
    sqlGeneration: 'SQL Generation',
    sqlExecution: 'SQL Execution',
    processing: 'Processing',
    idle: 'Idle'
  },
  connection: {
    error: 'Connection Error',
    checkSuggestions: 'Suggestions:',
    serverStarted: 'Check if the server is started',
    configCorrect: 'Check if the WebSocket URL configuration is correct',
    networkConnection: 'Check if the network connection is normal',
    reconnect: 'Reconnect'
  },
  // 添加首页文本
  home: {
    title: 'Apache Doris MCP Client',
    subtitle: 'Intelligent Analysis Tool Based on MCP Protocol',
    features: {
      nl2sql: {
        title: 'NL2SQL Natural Language Query',
        description: 'Use natural language to describe your data needs, and the system will automatically convert it into an SQL query and execute it, while providing business analysis and visualization suggestions.',
        button: 'Get Started'
      },
      sqlOptimize: {
        title: 'SQL Intelligent Optimization',
        description: 'Submit your SQL query, and the system will automatically analyze performance bottlenecks, provide optimization suggestions, and generate improved SQL statements.',
        button: 'Get Started'
      },
      llmConfig: {
        title: 'LLM Configuration Management',
        description: 'Customize the large language models (LLM) used for different processing stages, and select appropriate models and parameter configurations for different tasks.',
        button: 'Manage Configurations'
      }
    },
    systemStatus: {
      title: 'System Status',
      connectionStatus: 'Connection Status',
      serverAddress: 'Server Address',
      protocol: 'Protocol',
      llmProviderCount: 'LLM Provider Count'
    },
    connectionError: {
      title: 'Not Connected to MCP Server',
      description: 'Please check if the server has been started, then click the button below to try to connect.',
      button: 'Connect to Server',
      connecting: 'Connecting...'
    },
    serverDebug: {
      title: 'Server Connection Debug',
      checkButton: 'Check Server Status',
      connectButton: 'Manual Connect',
      results: 'Server Check Results:',
      success: '✓ Success',
      failed: '✗ Failed',
      error: '✗ Error:',
      tips: {
        title: 'Debug Tips:',
        checkServer: 'Check if the MCP server is started',
        confirmPort: 'Confirm that the server is running on port',
        checkEnv: 'Check if the environment variables are correctly configured (.env file)',
        checkNetwork: 'Check network connection and firewall settings',
        checkLogs: 'Try to view logs in the MCP server console'
      }
    }
  },
  // 添加SQL优化页面文本
  sqlOptimize: {
    title: 'SQL Intelligent Optimization Analysis',
    form: {
      sqlStatement: 'SQL Statement',
      optimizationRequirements: 'Optimization Requirements',
      placeholder: 'Please enter your specific optimization requirements, such as: "Need to reduce the time consumed by JOIN operations" or "Want to optimize memory usage for GROUP BY operations"',
      analyze: 'Analyze & Optimize',
      reset: 'Reset'
    },
    result: {
      title: 'Optimization Analysis Results',
      fixResult: {
        title: 'SQL execution error, tried to fix',
        errorAnalysis: 'Error Analysis',
        fixedSQL: 'Fixed SQL',
        businessMeaning: 'Business Meaning',
        sqlLogic: 'SQL Logic Description',
        reuseFixed: 'Reanalyze with Fixed SQL'
      },
      success: {
        businessAnalysis: 'Business Analysis',
        performanceAnalysis: 'Performance Analysis',
        bottlenecks: 'Performance Bottlenecks',
        suggestions: 'Optimization Suggestions',
        optimizedQueries: 'Optimized SQL',
        plan: 'Optimization Plan',
        optimizationPoints: 'Optimization Points Description',
        expectedImprovement: 'Expected Performance Improvement',
        useOptimized: 'Reanalyze with This Optimization Plan'
      },
      error: 'Processing Error',
      copy: 'Copy',
      copySuccess: 'SQL copied to clipboard',
      copyFailed: 'Copy failed'
    }
  },
  // 添加LLM配置页面文本
  llmConfig: {
    title: 'LLM Configuration Management',
    overview: {
      title: 'Model Status Overview',
      connectionStatus: 'Connection Status',
      serviceStatus: 'Service Status',
      available: 'Available',
      unavailable: 'Unavailable',
      refreshConnection: 'Refresh Connection'
    },
    tabs: {
      current: 'Current Configuration',
      edit: 'Edit Configuration'
    },
    configError: 'Failed to get current configuration',
    noConfig: 'Unable to get current configuration',
    sections: {
      nl2sql: 'NL2SQL Configuration',
      sqlOptimize: 'SQL Optimization Configuration',
      system: 'System Configuration'
    },
    fields: {
      sqlGenerationModel: 'SQL Generation Model',
      sqlFixModel: 'SQL Fix Model',
      businessAnalysisModel: 'Business Analysis Model',
      sqlOptimizeModel: 'SQL Optimization Model',
      temperature: 'Temperature',
      maxTokens: 'Max Tokens',
      maxRetries: 'Max Retries',
      timeout: 'Timeout (seconds)',
      logLevel: 'Log Level',
      notConfigured: 'Not Configured',
      default: 'Default'
    },
    edit: {
      warning: 'Directly modifying the configuration may affect system stability. Please make sure you understand the role of the configuration items.',
      save: 'Save Configuration',
      reset: 'Reset',
      success: 'Configuration saved successfully',
      failed: 'Failed to save configuration:'
    }
  }
};

// 中文语言包
const zhLocale = {
  common: {
    appTitle: 'NL2SQL 自然语言查询',
    debugMode: '调试模式',
    connected: '已连接',
    disconnected: '未连接',
    reconnect: '重新连接',
    clearChat: '清空对话',
    startChat: '开始对话，输入您的自然语言查询',
    send: '发送',
    scrollToBottom: '滚动到底部',
    fullResults: '完整查询结果'
  },
  query: {
    inputPlaceholder: '输入您的自然语言查询，例如：\'2023年第一季度销售额最高的三个产品是什么？\'',
    processing: '正在分析您的查询...',
    thinking: '思考过程',
    expand: '展开',
    collapse: '折叠',
    sql: 'SQL查询',
    copy: '复制',
    queryResults: '查询结果',
    records: '条记录',
    businessAnalysis: '业务分析',
    visualization: '可视化',
    trends: '主要趋势',
    recommendations: '业务建议',
    error: '错误',
    errorDetails: '详细信息',
    serverDiagnosis: '服务器问题诊断',
    serverError: '服务器返回了无效的响应格式，问题可能是：',
    suggestions: '解决建议：',
    queryProcessingError: '处理查询时出错',
    copySuccess: 'SQL已复制到剪贴板',
    copyFailed: '复制失败'
  },
  status: {
    currentStage: '当前阶段',
    processingStatus: '处理状态',
    progress: '进度',
    query: '查询',
    stageHistory: '历史阶段',
    analyzing: '分析',
    exampleAnalysis: '示例分析',
    businessAnalysis: '业务分析',
    sqlGeneration: 'SQL生成',
    sqlExecution: 'SQL执行',
    processing: '处理中',
    idle: '空闲'
  },
  connection: {
    error: '连接错误',
    checkSuggestions: '建议检查：',
    serverStarted: '服务器是否已启动',
    configCorrect: 'WebSocket URL配置是否正确',
    networkConnection: '网络连接是否正常',
    reconnect: '重新连接'
  },
  // 添加首页文本
  home: {
    title: 'Apache Doris MCP 客户端',
    subtitle: '基于MCP协议的智能分析工具',
    features: {
      nl2sql: {
        title: 'NL2SQL 自然语言查询',
        description: '使用自然语言描述您的数据需求，系统将自动转换为SQL查询并执行，同时提供业务分析和可视化建议。',
        button: '开始使用'
      },
      sqlOptimize: {
        title: 'SQL 智能优化分析',
        description: '提交您的SQL查询，系统将自动分析性能瓶颈，提供优化建议，并生成改进后的SQL语句。',
        button: '开始使用'
      },
      llmConfig: {
        title: 'LLM 配置管理',
        description: '自定义各处理阶段使用的大语言模型(LLM)，为不同的任务选择合适的模型和参数配置。',
        button: '管理配置'
      }
    },
    systemStatus: {
      title: '系统状态',
      connectionStatus: '连接状态',
      serverAddress: '服务器地址',
      protocol: '协议',
      llmProviderCount: 'LLM提供商数量'
    },
    connectionError: {
      title: '未连接到MCP服务器',
      description: '请检查服务器是否已启动，然后点击下方按钮尝试连接。',
      button: '连接服务器',
      connecting: '连接中...'
    },
    serverDebug: {
      title: '服务器连接调试',
      checkButton: '检查服务器状态',
      connectButton: '手动连接',
      results: '服务器检查结果:',
      success: '✓ 成功',
      failed: '✗ 失败',
      error: '✗ 错误:',
      tips: {
        title: '调试提示:',
        checkServer: '检查MCP服务器是否已启动',
        confirmPort: '确认服务器运行在端口',
        checkEnv: '检查环境变量配置是否正确 (.env 文件)',
        checkNetwork: '检查网络连接和防火墙设置',
        checkLogs: '尝试在MCP服务器控制台查看日志输出'
      }
    }
  },
  // 添加SQL优化页面文本
  sqlOptimize: {
    title: 'SQL 智能优化分析',
    form: {
      sqlStatement: 'SQL语句',
      optimizationRequirements: '优化需求',
      placeholder: '请输入您的特定优化需求，例如："需要减少JOIN操作的耗时" 或 "希望优化GROUP BY操作的内存使用"',
      analyze: '分析优化',
      reset: '重置'
    },
    result: {
      title: '优化分析结果',
      fixResult: {
        title: 'SQL执行出错，已尝试修复',
        errorAnalysis: '错误分析',
        fixedSQL: '修复后的SQL',
        businessMeaning: '业务含义',
        sqlLogic: 'SQL逻辑说明',
        reuseFixed: '使用修复后的SQL重新分析'
      },
      success: {
        businessAnalysis: '业务分析',
        performanceAnalysis: '性能分析',
        bottlenecks: '性能瓶颈',
        suggestions: '优化建议',
        optimizedQueries: '优化后的SQL',
        plan: '优化方案',
        optimizationPoints: '优化点说明',
        expectedImprovement: '预期性能提升',
        useOptimized: '使用此优化方案重新分析'
      },
      error: '处理出错',
      copy: '复制',
      copySuccess: 'SQL已复制到剪贴板',
      copyFailed: '复制失败'
    }
  },
  // 添加LLM配置页面文本
  llmConfig: {
    title: 'LLM 配置管理',
    overview: {
      title: '模型状态概览',
      connectionStatus: '连接状态',
      serviceStatus: '服务状态',
      available: '可用',
      unavailable: '不可用',
      refreshConnection: '刷新连接'
    },
    tabs: {
      current: '现有配置',
      edit: '修改配置'
    },
    configError: '获取配置失败',
    noConfig: '无法获取当前配置',
    sections: {
      nl2sql: 'NL2SQL 配置',
      sqlOptimize: 'SQL优化配置',
      system: '系统配置'
    },
    fields: {
      sqlGenerationModel: 'SQL生成模型',
      sqlFixModel: 'SQL修复模型',
      businessAnalysisModel: '业务分析模型',
      sqlOptimizeModel: 'SQL优化模型',
      temperature: '温度系数',
      maxTokens: '最大tokens',
      maxRetries: '最大重试次数',
      timeout: '超时设置(秒)',
      logLevel: '日志级别',
      notConfigured: '未配置',
      default: '默认'
    },
    edit: {
      warning: '直接修改配置可能影响系统稳定性，请确保了解配置项的作用',
      save: '保存配置',
      reset: '重置',
      success: '配置保存成功',
      failed: '保存配置失败:'
    }
  }
};

// 整合所有语言包
const messages = {
  en: enLocale,
  zh: zhLocale
};

// 创建i18n函数
export function useI18n() {
  const langStore = useLangStore();
  
  // 初始化语言设置
  langStore.initLanguage();
  
  // 计算当前语言的翻译对象
  const t = computed(() => {
    const currentLang = langStore.currentLang;
    return (key) => {
      const keys = key.split('.');
      let result = messages[currentLang];
      
      // 循环查找翻译
      for (const k of keys) {
        if (result && result[k]) {
          result = result[k];
        } else {
          // 如果找不到翻译，返回原始key
          console.warn(`Translation key not found: ${key}`);
          return key;
        }
      }
      
      return result;
    };
  });
  
  return {
    t: t.value,
    currentLang: computed(() => langStore.currentLang),
    supportedLangs: langStore.supportedLangs,
    setLanguage: langStore.setLanguage
  };
} 