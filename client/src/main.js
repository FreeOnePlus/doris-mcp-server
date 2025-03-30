import { createApp } from 'vue';
import { createPinia } from 'pinia';
import ElementPlus from 'element-plus';
import 'element-plus/dist/index.css';
import App from './App.vue';
import router from './router';
import './assets/styles/main.scss';

// 导入语言存储初始化
import { useLangStore } from './stores/langStore';

// 创建Vue应用
const app = createApp(App);

// 使用插件
const pinia = createPinia();
app.use(pinia);
app.use(router);
app.use(ElementPlus);

// 初始化语言设置
const langStore = useLangStore(pinia);
langStore.initLanguage();

// 挂载应用
app.mount('#app'); 