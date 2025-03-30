import { defineStore } from 'pinia';
import { ref } from 'vue';

export const useLangStore = defineStore('lang', () => {
  // 默认语言为英文
  const currentLang = ref('en');
  
  // 支持的语言列表
  const supportedLangs = [
    { code: 'en', name: 'English' },
    { code: 'zh', name: '中文' }
  ];
  
  // 切换语言的方法
  function setLanguage(langCode) {
    if (['en', 'zh'].includes(langCode)) {
      const oldLang = currentLang.value;
      
      // 如果语言已经改变，才需要刷新
      if (oldLang !== langCode) {
        // 先更新语言值
        currentLang.value = langCode;
        // 保存到本地存储
        localStorage.setItem('preferred_language', langCode);
        
        // 触发页面刷新以应用新语言设置
        setTimeout(() => {
          window.location.reload();
        }, 100);
      }
    }
  }
  
  // 在初始化时从本地存储加载语言设置
  function initLanguage() {
    const savedLang = localStorage.getItem('preferred_language');
    if (savedLang && ['en', 'zh'].includes(savedLang)) {
      currentLang.value = savedLang;
    }
  }
  
  return {
    currentLang,
    supportedLangs,
    setLanguage,
    initLanguage
  };
}); 