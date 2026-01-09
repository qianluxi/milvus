FROM python:3.10-slim

WORKDIR /app

# 先复制 requirements，保证依赖层可缓存
COPY requirements.txt .

# 强制使用稳定依赖组合
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip check

# 再复制业务代码
COPY . .

CMD ["python", "attu_app.py"]
