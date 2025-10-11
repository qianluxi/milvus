# config.py
import os

# 从环境变量获取敏感信息，提供默认值仅用于本地开发
HF_TOKEN = os.environ.get("HF_TOKEN", "your_hf_token_here")   # 从环境变量获取

MODEL_CONFIG = {
    'embedding': {
        'model_id': 'iic/nlp_gte_sentence-embedding_chinese-base'
    },
    'llm': {
        'model_id': 'deepseek-ai/DeepSeek-R1-0528',
        'temperature': 0.7,
        'max_new_tokens': 512
    }
}

ZILLIZ_CONFIG = {
    "endpoint": os.environ.get("ZILLIZ_ENDPOINT", "https://in03-a43203d37be04a4.serverless.ali-cn-hangzhou.cloud.zilliz.com.cn"),
    "user": os.environ.get("ZILLIZ_USER", "db_XXX"),
    "password": os.environ.get("ZILLIZ_PASSWORD", "XXX"),
    "secure": True
}

