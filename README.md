---
# 领域，可根据实际修改
domain:
  - nlp

# 自定义标签
tags:
  - vector-search
  - flask
  - document-retrieval

# （可选）关联的数据集
datasets:
  evaluation: []
  test: []
  train: []

pre_install:
  - pip install --upgrade pip
  - pip install --upgrade modelscope

# （可选）关联的模型
models:
  - Qwen/Qwen2.5-VL-7B-Instruct

# 部署启动文件
deployspec:
  entry_file: app.py     # 关键行：告诉平台入口是 app.py 而不是 index.html

# 许可证
license: Apache License 2.0
---


# milvus
milvus using
无需设置本地存储目录，完全依赖milvus存储