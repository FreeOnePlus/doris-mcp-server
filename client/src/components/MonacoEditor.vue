<template>
  <div ref="editorContainer" class="monaco-editor-container" :style="{ height }"></div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import * as monaco from 'monaco-editor'

// 组件属性
const props = defineProps({
  modelValue: {
    type: String,
    default: ''
  },
  language: {
    type: String,
    default: 'javascript'
  },
  options: {
    type: Object,
    default: () => ({})
  },
  height: {
    type: String,
    default: '300px'
  }
})

// 定义emit事件
const emit = defineEmits(['update:modelValue', 'editor-mounted'])

// 本地状态
const editorContainer = ref(null)
let editor = null

// 初始化编辑器
function initMonaco() {
  if (!editorContainer.value) return
  
  // 默认配置
  const defaultOptions = {
    value: props.modelValue,
    language: props.language,
    theme: 'vs',
    automaticLayout: true,
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    scrollbar: {
      vertical: 'auto',
      horizontal: 'auto'
    }
  }
  
  // 合并选项
  const editorOptions = {
    ...defaultOptions,
    ...props.options
  }
  
  // 创建编辑器实例
  editor = monaco.editor.create(editorContainer.value, editorOptions)
  
  // 内容更改时触发事件
  editor.onDidChangeModelContent(() => {
    const value = editor.getValue()
    if (value !== props.modelValue) {
      emit('update:modelValue', value)
    }
  })
  
  // 触发编辑器挂载事件
  emit('editor-mounted', editor)
}

// 组件挂载时初始化编辑器
onMounted(() => {
  initMonaco()
})

// 组件卸载前销毁编辑器
onBeforeUnmount(() => {
  if (editor) {
    editor.dispose()
    editor = null
  }
})

// 监听props变化
watch(() => props.modelValue, (newValue) => {
  if (editor && newValue !== editor.getValue()) {
    editor.setValue(newValue)
  }
}, { deep: true })

watch(() => props.language, (newValue) => {
  if (editor) {
    monaco.editor.setModelLanguage(editor.getModel(), newValue)
  }
})

watch(() => props.options, (newValue) => {
  if (editor) {
    editor.updateOptions(newValue)
  }
}, { deep: true })
</script>

<style scoped>
.monaco-editor-container {
  width: 100%;
  border: 1px solid #eee;
  border-radius: 4px;
  overflow: hidden;
}
</style> 