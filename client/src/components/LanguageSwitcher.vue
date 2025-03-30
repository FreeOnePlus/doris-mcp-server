<template>
  <div class="language-switcher">
    <el-dropdown @command="changeLanguage" trigger="click">
      <span class="el-dropdown-link">
        {{ currentLanguageName }} <el-icon class="el-icon--right"><ArrowDown /></el-icon>
      </span>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item 
            v-for="lang in supportedLangs" 
            :key="lang.code" 
            :command="lang.code"
            :class="{ 'is-active': currentLang === lang.code }"
          >
            {{ lang.name }}
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<script setup>
import { computed } from 'vue';
import { ArrowDown } from '@element-plus/icons-vue';
import { useI18n } from '../i18n';

const { currentLang, supportedLangs, setLanguage } = useI18n();

// 获取当前语言的显示名称
const currentLanguageName = computed(() => {
  const lang = supportedLangs.find(l => l.code === currentLang.value);
  return lang ? lang.name : 'English';
});

// 切换语言
function changeLanguage(langCode) {
  setLanguage(langCode);
}
</script>

<style lang="scss" scoped>
.language-switcher {
  display: inline-block;
  
  .el-dropdown-link {
    cursor: pointer;
    display: flex;
    align-items: center;
    color: #606266;
    font-size: 14px;
    font-weight: 500;
    padding: 4px 8px;
    border-radius: 4px;
    transition: all 0.3s;
    
    &:hover {
      color: #409EFF;
      background-color: #f0f7ff;
    }
  }
  
  :deep(.el-dropdown-menu__item) {
    &.is-active {
      color: #409EFF;
      font-weight: bold;
      background-color: #ecf5ff;
    }
  }
}

// 在全局导航中的特殊样式
:deep(.global-lang-switcher) {
  .el-dropdown-link {
    padding: 4px 10px;
    border: 1px solid #dcdfe6;
    border-radius: 4px;
    
    &:hover {
      border-color: #409EFF;
    }
  }
}
</style> 