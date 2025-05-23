#!/bin/bash
# Doris MCP服务器启动脚本
# 确保以SSE模式运行服务

# 设置颜色
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========== Doris MCP服务器启动脚本 ==========${NC}"

# 检查虚拟环境
if [ -d "venv" ]; then
    echo -e "${CYAN}找到虚拟环境，正在激活...${NC}"
    source venv/bin/activate
fi

# 清理缓存文件
echo -e "${CYAN}正在清理缓存文件...${NC}"
echo -e "${CYAN}清理Python缓存文件...${NC}"
find . -type d -name "__pycache__" -exec rm -rf {} +  2>/dev/null || true
echo -e "${CYAN}清理临时文件...${NC}"
rm -rf .pytest_cache 2>/dev/null || true
echo -e "${CYAN}清理日志文件...${NC}"
find ./log -type f -name "*.log" -delete 2>/dev/null || true

# 重新加载环境变量
if [ -f .env ]; then
    echo -e "${CYAN}加载.env文件中的环境变量...${NC}"
    source .env
fi

# 在启动前输出关键环境变量
echo -e "${CYAN}数据库设置:${NC}"
echo "DB_HOST=${DB_HOST}"
echo "DB_PORT=${DB_PORT}"
echo "DB_DATABASE=${DB_DATABASE}"
echo "FORCE_REFRESH_METADATA=${FORCE_REFRESH_METADATA}"

# 启动服务器
python -m src.main

# 清理缓存文件
echo -e "${YELLOW}正在清理缓存文件...${NC}"

# 后端缓存清理
echo -e "${GREEN}清理Python缓存文件...${NC}"
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
rm -rf ./.pytest_cache

# 清理临时文件
echo -e "${GREEN}清理临时文件...${NC}"
rm -rf ./tmp
mkdir -p tmp

# 前端缓存清理
if [ -d "client" ]; then
    echo -e "${GREEN}清理前端缓存文件...${NC}"
    rm -rf ./client/dist
    rm -rf ./client/.cache
    rm -rf ./client/node_modules/.cache
fi

# 清理日志文件
echo -e "${GREEN}清理日志文件...${NC}"
rm -rf ./logs/*.log
mkdir -p logs

# 设置环境变量，强制使用SSE模式
export MCP_PORT=3000
export ALLOWED_ORIGINS="*"
export LOG_LEVEL="info"
export MCP_ALLOW_CREDENTIALS="false"

# 添加适配器调试支持
export MCP_DEBUG_ADAPTER="true"
export PYTHONPATH="$(pwd):$PYTHONPATH"  # 确保src模块可以被导入

# 创建日志目录
mkdir -p logs

# 调试信息
echo -e "${GREEN}环境变量:${NC}"
echo -e "MCP_TRANSPORT_TYPE=${MCP_TRANSPORT_TYPE}"
echo -e "MCP_PORT=${MCP_PORT}"
echo -e "ALLOWED_ORIGINS=${ALLOWED_ORIGINS}"
echo -e "LOG_LEVEL=${LOG_LEVEL}"
echo -e "MCP_ALLOW_CREDENTIALS=${MCP_ALLOW_CREDENTIALS}"
echo -e "MCP_DEBUG_ADAPTER=${MCP_DEBUG_ADAPTER}"

echo -e "${GREEN}正在启动MCP服务器 (SSE模式)...${NC}"
echo -e "${YELLOW}服务将在 http://localhost:3000 上运行${NC}"
echo -e "${YELLOW}健康检查: http://localhost:3000/health${NC}"
echo -e "${YELLOW}SSE测试: http://localhost:3000/sse-test${NC}"
echo -e "${YELLOW}使用 Ctrl+C 停止服务${NC}"

# 启动MCP服务器
python src/main.py

# 如果服务器异常退出，输出错误信息
if [ $? -ne 0 ]; then
    echo -e "${RED}服务器异常退出！查看日志获取更多信息${NC}"
    exit 1
fi

# 显示浏览器缓存清理提示
echo -e "${YELLOW}提示：如果页面显示异常，请清理浏览器缓存或使用无痕模式访问${NC}"
echo -e "${YELLOW}Chrome浏览器清理缓存快捷键: Ctrl+Shift+Del (Windows) 或 Cmd+Shift+Del (Mac)${NC}" 