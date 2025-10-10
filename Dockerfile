FROM python:3.11-slim

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /code

ENV HF_HOME=/code/hf_cache
ENV TRANSFORMERS_CACHE=/code/hf_cache

# 拷贝依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝代码
COPY . .

# 默认启动 Flask
CMD ["python", "app.py"]
