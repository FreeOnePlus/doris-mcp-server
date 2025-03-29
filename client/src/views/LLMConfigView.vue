<template>
  <div class="llm-config-view">
    <el-card class="main-card">
      <template #header>
        <div class="header">
          <h2>LLM 配置管理</h2>
        </div>
      </template>
      
      <div class="content">
        <!-- 模型状态概览 -->
        <el-descriptions title="模型状态概览" :column="1" border>
          <el-descriptions-item label="连接状态">
            <el-tag :type="mcpStore.isConnected ? 'success' : 'danger'">
              {{ mcpStore.isConnected ? '已连接' : '未连接' }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="服务状态">
            <el-tag :type="mcpStore.available ? 'success' : 'danger'">
              {{ mcpStore.available ? '可用' : '不可用' }}
            </el-tag>
          </el-descriptions-item>
        </el-descriptions>
        
        <!-- 刷新连接按钮 -->
        <div class="action-buttons">
          <el-button 
            type="primary" 
            :loading="mcpStore.isConnecting" 
            @click="reconnect"
          >
            刷新连接
          </el-button>
        </div>
        
        <!-- 主功能选项卡 -->
        <el-tabs v-model="activeTab" type="border-card" class="config-tabs">
          <el-tab-pane label="现有配置" name="current">
            <div v-if="isLoading" class="loading-indicator">
              <el-skeleton :rows="6" animated />
            </div>
            
            <div v-else-if="configError" class="config-error">
              <el-alert
                type="error"
                :title="configError"
                :closable="false"
                show-icon
              />
            </div>
            
            <div v-else-if="!currentConfig" class="no-config">
              <el-empty description="无法获取当前配置" />
            </div>
            
            <div v-else class="config-details">
              <el-collapse accordion>
                <!-- NL2SQL配置 -->
                <el-collapse-item title="NL2SQL 配置" name="nl2sql">
                  <el-descriptions :column="1" border>
                    <el-descriptions-item label="SQL生成模型">
                      {{ currentConfig.nl2sql?.sql_generation_model || '未配置' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="SQL修复模型">
                      {{ currentConfig.nl2sql?.sql_fix_model || '未配置' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="业务分析模型">
                      {{ currentConfig.nl2sql?.business_analysis_model || '未配置' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="温度系数">
                      {{ currentConfig.nl2sql?.temperature || '默认' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="最大tokens">
                      {{ currentConfig.nl2sql?.max_tokens || '默认' }}
                    </el-descriptions-item>
                  </el-descriptions>
                </el-collapse-item>
                
                <!-- SQL优化配置 -->
                <el-collapse-item title="SQL优化配置" name="sql_optimize">
                  <el-descriptions :column="1" border>
                    <el-descriptions-item label="SQL优化模型">
                      {{ currentConfig.sql_optimize?.model || '未配置' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="温度系数">
                      {{ currentConfig.sql_optimize?.temperature || '默认' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="最大tokens">
                      {{ currentConfig.sql_optimize?.max_tokens || '默认' }}
                    </el-descriptions-item>
                  </el-descriptions>
                </el-collapse-item>
                
                <!-- 系统配置 -->
                <el-collapse-item title="系统配置" name="system">
                  <el-descriptions :column="1" border>
                    <el-descriptions-item label="最大重试次数">
                      {{ currentConfig.system?.max_retries || '默认' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="超时设置(秒)">
                      {{ currentConfig.system?.timeout || '默认' }}
                    </el-descriptions-item>
                    <el-descriptions-item label="日志级别">
                      {{ currentConfig.system?.log_level || '默认' }}
                    </el-descriptions-item>
                  </el-descriptions>
                </el-collapse-item>
              </el-collapse>
            </div>
          </el-tab-pane>
          
          <el-tab-pane label="修改配置" name="edit">
            <div class="edit-config">
              <el-alert
                type="warning"
                title="直接修改配置可能影响系统稳定性，请确保了解配置项的作用"
                :closable="false"
                show-icon
                class="warning-alert"
              />
              
              <el-form 
                :model="editConfig" 
                label-position="top" 
                :rules="configRules"
                ref="configForm"
              >
                <!-- NL2SQL配置 -->
                <el-divider content-position="left">NL2SQL 配置</el-divider>
                
                <el-form-item label="SQL生成模型" prop="nl2sql.sql_generation_model">
                  <el-input 
                    v-model="editConfig.nl2sql.sql_generation_model"
                    placeholder="如: claude-3-sonnet-20240229"
                  />
                </el-form-item>
                
                <el-form-item label="SQL修复模型" prop="nl2sql.sql_fix_model">
                  <el-input 
                    v-model="editConfig.nl2sql.sql_fix_model"
                    placeholder="如: claude-3-sonnet-20240229"
                  />
                </el-form-item>
                
                <el-form-item label="业务分析模型" prop="nl2sql.business_analysis_model">
                  <el-input 
                    v-model="editConfig.nl2sql.business_analysis_model"
                    placeholder="如: claude-3-sonnet-20240229"
                  />
                </el-form-item>
                
                <el-form-item label="温度系数" prop="nl2sql.temperature">
                  <el-slider 
                    v-model="editConfig.nl2sql.temperature" 
                    :min="0" 
                    :max="1" 
                    :step="0.05"
                    show-input
                  />
                </el-form-item>
                
                <el-form-item label="最大tokens" prop="nl2sql.max_tokens">
                  <el-input-number 
                    v-model="editConfig.nl2sql.max_tokens" 
                    :min="100" 
                    :max="100000" 
                    :step="100"
                  />
                </el-form-item>
                
                <!-- SQL优化配置 -->
                <el-divider content-position="left">SQL优化配置</el-divider>
                
                <el-form-item label="SQL优化模型" prop="sql_optimize.model">
                  <el-input 
                    v-model="editConfig.sql_optimize.model"
                    placeholder="如: claude-3-sonnet-20240229"
                  />
                </el-form-item>
                
                <el-form-item label="温度系数" prop="sql_optimize.temperature">
                  <el-slider 
                    v-model="editConfig.sql_optimize.temperature" 
                    :min="0" 
                    :max="1" 
                    :step="0.05"
                    show-input
                  />
                </el-form-item>
                
                <el-form-item label="最大tokens" prop="sql_optimize.max_tokens">
                  <el-input-number 
                    v-model="editConfig.sql_optimize.max_tokens" 
                    :min="100" 
                    :max="100000" 
                    :step="100"
                  />
                </el-form-item>
                
                <!-- 系统配置 -->
                <el-divider content-position="left">系统配置</el-divider>
                
                <el-form-item label="最大重试次数" prop="system.max_retries">
                  <el-input-number 
                    v-model="editConfig.system.max_retries" 
                    :min="0" 
                    :max="10" 
                    :step="1"
                  />
                </el-form-item>
                
                <el-form-item label="超时设置(秒)" prop="system.timeout">
                  <el-input-number 
                    v-model="editConfig.system.timeout" 
                    :min="1" 
                    :max="300" 
                    :step="1"
                  />
                </el-form-item>
                
                <el-form-item label="日志级别" prop="system.log_level">
                  <el-select v-model="editConfig.system.log_level">
                    <el-option label="DEBUG" value="DEBUG" />
                    <el-option label="INFO" value="INFO" />
                    <el-option label="WARNING" value="WARNING" />
                    <el-option label="ERROR" value="ERROR" />
                    <el-option label="CRITICAL" value="CRITICAL" />
                  </el-select>
                </el-form-item>
                
                <el-form-item>
                  <el-button 
                    type="primary" 
                    @click="saveConfig"
                    :loading="isSaving"
                    :disabled="!mcpStore.available"
                  >
                    保存配置
                  </el-button>
                  <el-button 
                    @click="resetEditForm"
                    :disabled="isSaving"
                  >
                    重置
                  </el-button>
                </el-form-item>
              </el-form>
            </div>
          </el-tab-pane>
        </el-tabs>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, watch } from 'vue';
import { useMCPStore } from '../stores/mcp';
import { ElMessage } from 'element-plus';

const mcpStore = useMCPStore();
const activeTab = ref('current');
const isLoading = ref(false);
const isSaving = ref(false);
const configError = ref(null);
const currentConfig = ref(null);
const configForm = ref(null);

// 编辑配置
const editConfig = reactive({
  nl2sql: {
    sql_generation_model: '',
    sql_fix_model: '',
    business_analysis_model: '',
    temperature: 0.7,
    max_tokens: 4000
  },
  sql_optimize: {
    model: '',
    temperature: 0.7,
    max_tokens: 4000
  },
  system: {
    max_retries: 3,
    timeout: 60,
    log_level: 'INFO'
  }
});

// 配置验证规则
const configRules = {
  'nl2sql.temperature': [
    { type: 'number', min: 0, max: 1, message: '温度系数必须在0-1之间', trigger: 'change' }
  ],
  'sql_optimize.temperature': [
    { type: 'number', min: 0, max: 1, message: '温度系数必须在0-1之间', trigger: 'change' }
  ],
  'system.max_retries': [
    { type: 'number', min: 0, max: 10, message: '重试次数必须在0-10之间', trigger: 'change' }
  ],
  'system.timeout': [
    { type: 'number', min: 1, max: 300, message: '超时时间必须在1-300秒之间', trigger: 'change' }
  ]
};

// 获取当前配置
async function fetchCurrentConfig() {
  if (!mcpStore.available) return;
  
  isLoading.value = true;
  configError.value = null;
  
  try {
    const config = await mcpStore.getLLMConfig();
    currentConfig.value = config;
    
    // 更新编辑表单
    if (config) {
      updateEditConfig(config);
    }
  } catch (error) {
    configError.value = `获取配置失败: ${error.message || '未知错误'}`;
    console.error('Failed to fetch config:', error);
  } finally {
    isLoading.value = false;
  }
}

// 更新编辑配置
function updateEditConfig(config) {
  // NL2SQL配置
  if (config.nl2sql) {
    editConfig.nl2sql.sql_generation_model = config.nl2sql.sql_generation_model || '';
    editConfig.nl2sql.sql_fix_model = config.nl2sql.sql_fix_model || '';
    editConfig.nl2sql.business_analysis_model = config.nl2sql.business_analysis_model || '';
    editConfig.nl2sql.temperature = config.nl2sql.temperature !== undefined ? config.nl2sql.temperature : 0.7;
    editConfig.nl2sql.max_tokens = config.nl2sql.max_tokens || 4000;
  }
  
  // SQL优化配置
  if (config.sql_optimize) {
    editConfig.sql_optimize.model = config.sql_optimize.model || '';
    editConfig.sql_optimize.temperature = config.sql_optimize.temperature !== undefined ? config.sql_optimize.temperature : 0.7;
    editConfig.sql_optimize.max_tokens = config.sql_optimize.max_tokens || 4000;
  }
  
  // 系统配置
  if (config.system) {
    editConfig.system.max_retries = config.system.max_retries !== undefined ? config.system.max_retries : 3;
    editConfig.system.timeout = config.system.timeout || 60;
    editConfig.system.log_level = config.system.log_level || 'INFO';
  }
}

// 保存配置
async function saveConfig() {
  if (!mcpStore.available || isSaving.value) return;
  
  // 表单验证
  await configForm.value.validate((valid, fields) => {
    if (!valid) {
      console.log('表单验证失败:', fields);
      return false;
    }
    
    isSaving.value = true;
    
    // 调用API保存配置
    mcpStore.updateLLMConfig(editConfig)
      .then(() => {
        ElMessage.success('配置保存成功');
        fetchCurrentConfig(); // 重新获取配置
      })
      .catch((error) => {
        ElMessage.error(`保存配置失败: ${error.message || '未知错误'}`);
      })
      .finally(() => {
        isSaving.value = false;
      });
  });
}

// 重置编辑表单
function resetEditForm() {
  if (currentConfig.value) {
    updateEditConfig(currentConfig.value);
  } else {
    // 重置为默认值
    editConfig.nl2sql.sql_generation_model = '';
    editConfig.nl2sql.sql_fix_model = '';
    editConfig.nl2sql.business_analysis_model = '';
    editConfig.nl2sql.temperature = 0.7;
    editConfig.nl2sql.max_tokens = 4000;
    
    editConfig.sql_optimize.model = '';
    editConfig.sql_optimize.temperature = 0.7;
    editConfig.sql_optimize.max_tokens = 4000;
    
    editConfig.system.max_retries = 3;
    editConfig.system.timeout = 60;
    editConfig.system.log_level = 'INFO';
  }
  
  if (configForm.value) {
    configForm.value.resetFields();
  }
}

// 重新连接
function reconnect() {
  if (mcpStore.isConnecting) return;
  
  mcpStore.connect().then(() => {
    if (mcpStore.isConnected) {
      ElMessage.success('连接成功');
      fetchCurrentConfig();
    } else {
      ElMessage.error('连接失败');
    }
  });
}

// 监听连接状态变化
watch(() => mcpStore.available, (newValue) => {
  if (newValue) {
    fetchCurrentConfig();
  }
});

onMounted(() => {
  // 如果未连接，尝试连接
  if (!mcpStore.isConnected && !mcpStore.isConnecting) {
    mcpStore.connect();
  } else if (mcpStore.available) {
    fetchCurrentConfig();
  }
});
</script>

<style lang="scss" scoped>
.llm-config-view {
  max-width: 1200px;
  margin: 0 auto;
  
  .main-card {
    .header {
      h2 {
        margin: 0;
      }
    }
    
    .content {
      .action-buttons {
        margin: 20px 0;
        display: flex;
        justify-content: flex-end;
      }
      
      .config-tabs {
        margin-top: 20px;
      }
      
      .loading-indicator,
      .config-error,
      .no-config {
        margin: 20px 0;
      }
      
      .edit-config {
        .warning-alert {
          margin-bottom: 20px;
        }
      }
    }
  }
}
</style> 