FROM node:18-slim

# 安装 Python 和所需的包
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# 复制所有源代码
COPY . .

# 暴露端口（如果需要）
EXPOSE 3000

# 默认命令
CMD ["python3", "src/main.py"] 