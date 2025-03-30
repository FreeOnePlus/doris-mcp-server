<template>
  <div class="sql-optimize-view">
    <el-card class="main-card">
      <template #header>
        <div class="header">
          <h2>{{ t('sqlOptimize.title') }}</h2>
        </div>
      </template>
      
      <div class="content">
        <el-form :model="optimizeForm" label-position="top">
          <el-form-item :label="t('sqlOptimize.form.sqlStatement')" required>
            <monaco-editor
              v-model="optimizeForm.sql"
              language="sql"
              :options="editorOptions"
              height="200px"
            />
          </el-form-item>
          
          <el-form-item :label="t('sqlOptimize.form.optimizationRequirements')">
            <el-input
              v-model="optimizeForm.requirements"
              type="textarea"
              :rows="2"
              :placeholder="t('sqlOptimize.form.placeholder')"
            />
          </el-form-item>
          
          <el-form-item>
            <el-button 
              type="primary" 
              @click="optimizeSQL"
              :loading="isProcessing"
              :disabled="!optimizeForm.sql.trim() || !mcpStore.available"
            >
              {{ t('sqlOptimize.form.analyze') }}
            </el-button>
            <el-button 
              @click="resetForm"
              :disabled="isProcessing"
            >
              {{ t('sqlOptimize.form.reset') }}
            </el-button>
          </el-form-item>
        </el-form>
        
        <!-- 优化结果 -->
        <div v-if="optimizeResult" class="optimize-result">
          <el-divider content-position="center">{{ t('sqlOptimize.result.title') }}</el-divider>
          
          <!-- 错误修复结果 -->
          <div v-if="optimizeResult.status === 'fixed'" class="fix-result">
            <el-alert
              type="warning"
              :title="optimizeResult.error || t('sqlOptimize.result.fixResult.title')"
              :closable="false"
              show-icon
            />
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.fixResult.errorAnalysis') }}</h3>
              <div v-html="formatText(optimizeResult.fix_result.error_analysis)" class="section-content"></div>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.fixResult.fixedSQL') }}</h3>
              <div class="sql-wrapper">
                <monaco-editor
                  v-model="optimizeResult.fix_result.fixed_sql"
                  language="sql"
                  :options="readOnlyEditorOptions"
                  height="150px"
                />
                <el-button 
                  type="primary" 
                  size="small" 
                  class="copy-btn"
                  @click="copySQL(optimizeResult.fix_result.fixed_sql)"
                >
                  {{ t('sqlOptimize.result.copy') }}
                </el-button>
              </div>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.fixResult.businessMeaning') }}</h3>
              <div v-html="formatText(optimizeResult.fix_result.business_meaning)" class="section-content"></div>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.fixResult.sqlLogic') }}</h3>
              <div v-html="formatText(optimizeResult.fix_result.sql_logic)" class="section-content"></div>
            </div>
            
            <el-button 
              type="primary" 
              @click="useFixedSQL"
              :disabled="isProcessing"
            >
              {{ t('sqlOptimize.result.fixResult.reuseFixed') }}
            </el-button>
          </div>
          
          <!-- 优化分析结果 -->
          <div v-else-if="optimizeResult.status === 'success'" class="success-result">
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.success.businessAnalysis') }}</h3>
              <div v-html="formatText(optimizeResult.optimization_result.business_analysis)" class="section-content"></div>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.success.performanceAnalysis') }}</h3>
              <div v-html="formatText(optimizeResult.optimization_result.performance_analysis)" class="section-content"></div>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.success.bottlenecks') }}</h3>
              <el-tag 
                v-for="(bottleneck, index) in optimizeResult.optimization_result.bottlenecks" 
                :key="`bottleneck-${index}`"
                type="danger"
                class="bottleneck-tag"
              >
                {{ bottleneck }}
              </el-tag>
            </div>
            
            <div class="result-section">
              <h3>{{ t('sqlOptimize.result.success.suggestions') }}</h3>
              <ul class="suggestion-list">
                <li 
                  v-for="(suggestion, index) in optimizeResult.optimization_result.optimization_suggestions" 
                  :key="`suggestion-${index}`"
                >
                  {{ suggestion }}
                </li>
              </ul>
            </div>
            
            <div class="result-section" v-if="optimizeResult.optimization_result.optimized_queries && optimizeResult.optimization_result.optimized_queries.length">
              <h3>{{ t('sqlOptimize.result.success.optimizedQueries') }}</h3>
              
              <el-tabs type="border-card">
                <el-tab-pane 
                  v-for="(query, i) in optimizeResult.optimization_result.optimized_queries" 
                  :key="`query-${i}`"
                  :label="t('sqlOptimize.result.success.plan') + ' ' + (i+1)"
                >
                  <div class="sql-wrapper">
                    <monaco-editor
                      v-model="query.sql"
                      language="sql"
                      :options="readOnlyEditorOptions"
                      height="150px"
                    />
                    <el-button 
                      type="primary" 
                      size="small" 
                      class="copy-btn"
                      @click="copySQL(query.sql)"
                    >
                      {{ t('sqlOptimize.result.copy') }}
                    </el-button>
                  </div>
                  
                  <div class="query-explanation">
                    <h4>{{ t('sqlOptimize.result.success.optimizationPoints') }}</h4>
                    <div v-html="formatText(query.explanation)" class="explanation-content"></div>
                    
                    <h4>{{ t('sqlOptimize.result.success.expectedImprovement') }}</h4>
                    <div v-html="formatText(query.expected_improvement)" class="explanation-content"></div>
                    
                    <el-button 
                      type="primary" 
                      @click="useOptimizedSQL(query.sql)"
                      :disabled="isProcessing"
                    >
                      {{ t('sqlOptimize.result.success.useOptimized') }}
                    </el-button>
                  </div>
                </el-tab-pane>
              </el-tabs>
            </div>
          </div>
          
          <!-- 其他错误 -->
          <div v-else-if="optimizeResult.status === 'error'" class="error-result">
            <el-alert
              type="error"
              :title="optimizeResult.message || t('sqlOptimize.result.error')"
              :closable="false"
              show-icon
            />
          </div>
        </div>
        
        <!-- 加载中 -->
        <div v-if="isProcessing" class="loading-indicator">
          <el-skeleton :rows="6" animated />
        </div>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useMCPStore } from '../stores/mcp';
import { ElMessage } from 'element-plus';
import MonacoEditor from '@/components/MonacoEditor.vue';
import { useI18n } from '../i18n';

const mcpStore = useMCPStore();
const { t, currentLang } = useI18n();
const isProcessing = ref(false);
const optimizeResult = ref(null);

// 优化表单
const optimizeForm = ref({
  sql: '',
  requirements: ''
});

// Monaco编辑器选项
const editorOptions = {
  automaticLayout: true,
  minimap: { enabled: false },
  scrollBeyondLastLine: false,
  lineNumbers: 'on',
  tabSize: 2
};

const readOnlyEditorOptions = {
  ...editorOptions,
  readOnly: true
};

// 优化SQL
async function optimizeSQL() {
  if (!optimizeForm.value.sql.trim() || !mcpStore.available) return;
  
  isProcessing.value = true;
  optimizeResult.value = null;
  
  try {
    // 调用优化服务
    const result = await mcpStore.sqlOptimize(
      optimizeForm.value.sql, 
      optimizeForm.value.requirements
    );
    
    optimizeResult.value = result;
  } catch (error) {
    ElMessage.error(currentLang.value === 'en' 
      ? `Optimization analysis failed: ${error.message || 'Unknown error'}`
      : `优化分析失败: ${error.message || '未知错误'}`);
    optimizeResult.value = {
      status: 'error',
      message: error.message || (currentLang.value === 'en' ? 'Processing error' : '处理出错')
    };
  } finally {
    isProcessing.value = false;
  }
}

// 重置表单
function resetForm() {
  optimizeForm.value = {
    sql: '',
    requirements: ''
  };
  optimizeResult.value = null;
}

// 复制SQL
function copySQL(sql) {
  navigator.clipboard.writeText(sql)
    .then(() => ElMessage.success(t('sqlOptimize.result.copySuccess')))
    .catch(() => ElMessage.error(t('sqlOptimize.result.copyFailed')));
}

// 使用修复后的SQL
function useFixedSQL() {
  if (!optimizeResult.value || optimizeResult.value.status !== 'fixed') return;
  
  optimizeForm.value.sql = optimizeResult.value.fix_result.fixed_sql;
  optimizeResult.value = null;
}

// 使用优化后的SQL
function useOptimizedSQL(sql) {
  optimizeForm.value.sql = sql;
  optimizeResult.value = null;
}

// 格式化文本（处理换行等）
function formatText(text) {
  if (!text) return '';
  return text.replace(/\n/g, '<br>');
}

onMounted(() => {
  // 如果未连接，尝试连接
  if (!mcpStore.isConnected && !mcpStore.isConnecting) {
    mcpStore.connect();
  }
});
</script>

<style lang="scss" scoped>
.sql-optimize-view {
  max-width: 1200px;
  margin: 0 auto;
  
  .main-card {
    .header {
      h2 {
        margin: 0;
      }
    }
    
    .content {
      .optimize-result {
        margin-top: 20px;
        
        .result-section {
          margin-bottom: 20px;
          
          h3 {
            font-size: 16px;
            margin-top: 20px;
            margin-bottom: 10px;
            color: #303133;
          }
          
          .section-content {
            line-height: 1.6;
            color: #606266;
          }
          
          .sql-wrapper {
            position: relative;
            
            .copy-btn {
              position: absolute;
              top: 5px;
              right: 5px;
              z-index: 10;
            }
          }
          
          .bottleneck-tag {
            margin-right: 8px;
            margin-bottom: 8px;
          }
          
          .suggestion-list {
            padding-left: 20px;
            margin: 0;
            
            li {
              margin-bottom: 8px;
            }
          }
          
          .query-explanation {
            margin-top: 10px;
            
            h4 {
              font-size: 14px;
              margin-top: 15px;
              margin-bottom: 8px;
              color: #606266;
            }
            
            .explanation-content {
              margin-bottom: 15px;
              line-height: 1.6;
            }
          }
        }
      }
      
      .loading-indicator {
        margin-top: 20px;
      }
    }
  }
}
</style> 