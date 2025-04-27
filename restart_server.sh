#!/bin/bash
# Doris MCP服务器重启脚本
# 检测端口和进程占用，终止已有进程，然后重新启动服务器

# 设置终端颜色
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# 服务器配置
MCP_PORT=3000
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SCRIPT="${SCRIPT_DIR}/start_server.sh"

echo -e "${GREEN}========== Doris MCP服务器重启脚本 ==========${NC}"

# 检查start_server.sh是否存在
if [ ! -f "$START_SCRIPT" ]; then
    echo -e "${RED}错误: 启动脚本 $START_SCRIPT 不存在${NC}"
    exit 1
fi

# 检查端口占用
check_port() {
    echo -e "${YELLOW}检查端口 $MCP_PORT 占用情况...${NC}"
    PORT_PID=$(lsof -ti:$MCP_PORT)
    if [ -n "$PORT_PID" ]; then
        echo -e "${YELLOW}端口 $MCP_PORT 被进程 $PORT_PID 占用${NC}"
        return 0
    else
        echo -e "${GREEN}端口 $MCP_PORT 未被占用${NC}"
        return 1
    fi
}

# 检查Python进程是否在运行src/main.py
check_python_process() {
    echo -e "${YELLOW}检查Python进程是否运行src/main.py...${NC}"
    PYTHON_PID=$(ps aux | grep "[p]ython.*src.main" | awk '{print $2}')
    if [ -n "$PYTHON_PID" ]; then
        echo -e "${YELLOW}检测到Python进程 $PYTHON_PID 正在运行src/main.py${NC}"
        return 0
    else
        echo -e "${GREEN}未检测到Python进程运行src/main.py${NC}"
        return 1
    fi
}

# 杀死进程
kill_process() {
    local PID=$1
    echo -e "${YELLOW}正在终止进程 $PID...${NC}"
    kill $PID 2>/dev/null
    
    # 等待进程终止
    for i in {1..5}; do
        if ! ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}进程 $PID 已终止${NC}"
            return 0
        fi
        echo -e "${YELLOW}等待进程终止 (${i}/5)...${NC}"
        sleep 1
    done
    
    # 如果进程仍然运行，强制终止
    if ps -p $PID > /dev/null 2>&1; then
        echo -e "${YELLOW}进程仍在运行，强制终止进程 $PID...${NC}"
        kill -9 $PID 2>/dev/null
        sleep 1
        if ! ps -p $PID > /dev/null 2>&1; then
            echo -e "${GREEN}进程 $PID 已被强制终止${NC}"
            return 0
        else
            echo -e "${RED}无法终止进程 $PID${NC}"
            return 1
        fi
    fi
    
    return 0
}

# 清理所有进程和端口占用
cleanup() {
    # 检查并终止占用端口的进程
    check_port
    if [ $? -eq 0 ]; then
        kill_process $PORT_PID
    fi
    
    # 检查并终止Python进程
    check_python_process
    if [ $? -eq 0 ]; then
        kill_process $PYTHON_PID
    fi
    
    # 再次检查端口占用，确保已经释放
    check_port
    if [ $? -eq 0 ]; then
        echo -e "${RED}警告: 无法释放端口 $MCP_PORT，请手动检查进程${NC}"
        return 1
    fi
    
    # 清理可能的Python字节码缓存
    echo -e "${YELLOW}清理Python字节码缓存...${NC}"
    find "$SCRIPT_DIR" -name "*.pyc" -delete
    find "$SCRIPT_DIR" -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    
    echo -e "${GREEN}清理完成${NC}"
    return 0
}

# 启动服务器
start_server() {
    echo -e "${YELLOW}正在停止现有的Doris MCP服务器进程...${NC}"
    pkill -f "python -m src.main" || true

    # 等待进程完全停止
    sleep 2

    echo -e "${YELLOW}正在启动Doris MCP服务器...${NC}"
    nohup python -m src.main >> logs/doris_mcp.log 2>> logs/doris_mcp.error &
    
    # 等待服务器启动
    sleep 5

    echo -e "${YELLOW}检查服务器是否成功启动...${NC}"
    if pgrep -f "python -m src.main" > /dev/null; then
        echo -e "${GREEN}Doris MCP服务器已成功启动${NC}"
        echo -e "${GREEN}服务地址: http://localhost:$MCP_PORT/${NC}"
            return 0
    else
        echo -e "${RED}服务器启动失败，请检查日志文件${NC}"
        tail -n 20 logs/doris_mcp.error
        return 1
        fi
}

# 主函数
main() {
    echo -e "${YELLOW}开始重启Doris MCP服务器...${NC}"
    
    # 清理现有进程
    cleanup
    if [ $? -ne 0 ]; then
        echo -e "${RED}清理现有进程失败，重启中止${NC}"
        exit 1
    fi
    
    # 等待端口完全释放
    sleep 2
    
    # 启动服务器
    start_server
    if [ $? -ne 0 ]; then
        echo -e "${RED}服务器启动失败${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}服务器重启成功${NC}"
    echo -e "${YELLOW}服务运行于: http://localhost:$MCP_PORT${NC}"
    echo -e "${YELLOW}健康检查: http://localhost:$MCP_PORT/health${NC}"
    echo -e "${YELLOW}SSE测试: http://localhost:$MCP_PORT/sse-test${NC}"
}

# 运行主函数
main 