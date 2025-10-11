from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
import logging
import requests
import json
import time
from datetime import datetime
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed, retry_if_exception_type
import ssl
import httpx 
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_huggingface import HuggingFaceEndpoint
from sentence_transformers import SentenceTransformer
from typing import Set, List, Dict, Optional, Tuple, Any
import uuid
import sys
from docx import Document  # 新增：用于处理Word文档的跨平台库
from config import MODEL_CONFIG, ZILLIZ_CONFIG, HF_TOKEN
from pathlib import Path

# 启用详细日志记录（调试时使用）
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("VectorSearchSystem")

# 初始化 Hugging Face 客户端（全局用这个）
client = OpenAI(
    base_url="https://router.huggingface.co/v1",
    api_key=HF_TOKEN
)

# Zilliz Cloud 配置已导入

# 模型配置 - 更新为huggingface配置已导入

# class ModelScopeLLM:
#     """OpenAI兼容封装的ModelScope LLM API"""
#     def __init__(self, api_base, api_key, model_id):
#         self.client = OpenAI(
#             base_url=api_base,
#             api_key=api_key
#         )
#         self.model_id = model_id
        
#     # 修改generate方法支持流式输出
#     def generate(self, prompt, **kwargs):
#         response = self.client.chat.completions.create(
#             model=self.model_id,
#             messages=[{"role": "user", "content": prompt}],
#             stream=True,  # 启用流式
#             max_tokens=kwargs.get("max_tokens", 2048),
#         )
        
#         # 逐步收集响应内容
#         full_response = ""
#         for chunk in response:
#             if chunk.choices[0].delta.content:
#                 full_response += chunk.choices[0].delta.content
#         return full_response

class VectorSearchSystem:
    def __init__(self, collection_name="text_search", max_retries=5, zilliz_config=None, model_config=None):
        # 设置 HuggingFace 缓存目录，避免 /.cache 权限问题
        os.environ["HF_HOME"] = "/tmp/hf_cache"
        os.environ["TRANSFORMERS_CACHE"] = "/tmp/hf_cache"
        
        self.collection_name = collection_name
        self.dim = 1024  # 初始值，实际由嵌入模型决定
        self.collection = None
        self.max_retries = max_retries
        self.reranker_model = None
        self.metadata_dict = {}
        self.field_handlers = {
            'filename': {'max_len': 256, 'truncate': 'hash'},
            'chapter_title': {'max_len': 300, 'truncate': 'hash'},
            'subsection_title': {'max_len': 500, 'truncate': 'simple'},
            'content': {'max_len': 65535, 'truncate': 'reject'}
        }
        self.project_metadata = {}  # 项目元数据存储 {project_name: metadata}

        # 使用传入的配置，或者默认的全局配置
        self.model_config = model_config
        self.zilliz_config = zilliz_config
        
        # Embedding
        self.embeddings = HuggingFaceEmbeddings(
            model_name=MODEL_CONFIG["embedding"]["model_id"]   # 从 config.py 读取
        )

        # LLM
        self.llm = HuggingFaceEndpoint(
            repo_id=MODEL_CONFIG["llm"]["repo_id"],
            task="conversational",   # ✅ 显式指定对话任务
            temperature=0.7,
            max_new_tokens=512,
            huggingfacehub_api_token=os.getenv(HF_TOKEN),
        )
        
        # 新增：上下文状态管理
        self.context_manager = {
            "current_project_set": None,
            "constraints": [],  # 初始化为空列表（而非None）
            "session_id": str(uuid.uuid4())
        }

        # 初始化
        self._initialize()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(3),
        retry=retry_if_exception_type((httpx.NetworkError, 
                                     httpx.TimeoutException,
                                     ssl.SSLError)),
        reraise=True
    )

    def update_context(self, constraint_type: str, value: any):
        """
        更新上下文约束条件（支持多格式时间范围，自动去重，避免全集约束）
        """
        if self.context_manager is None:
            self.context_manager = {
                "current_project_set": None,
                "constraints": [],
                "session_id": str(uuid.uuid4())
            }

        if value is None:
            logger.warning("跳过空约束值")
            return
        if isinstance(value, str) and not value.strip():
            logger.warning("跳过空字符串约束")
            return

        if constraint_type == "time_range":
            parsed_value = None
            value_str = value.strip() if isinstance(value, str) else str(value)

            range_match = re.match(r"^(\d{4})\s*-\s*(\d{4})\s*年$", value_str)
            if range_match:
                start_year = int(range_match.group(1))
                end_year = int(range_match.group(2))
                if start_year <= end_year:
                    parsed_value = (start_year, end_year)
                else:
                    logger.warning(f"时间范围无效（开始年份>结束年份）: {value_str}")
                    return

            after_match = re.match(r"^(\d{4})\s*年\s*以后$", value_str)
            if after_match and not parsed_value:
                start_year = int(after_match.group(1))
                parsed_value = (start_year, None)

            before_match = re.match(r"^(\d{4})\s*年\s*以前$", value_str)
            if before_match and not parsed_value:
                end_year = int(before_match.group(1))
                parsed_value = (None, end_year)

            if not parsed_value:
                logger.warning(f"不支持的时间格式: {value_str}（支持：YYYY-YYYY年/YYYY年以后/YYYY年以前）")
                return
            value = parsed_value

        for existing in self.context_manager["constraints"]:
            if (existing["type"] == constraint_type and 
                existing["value"] == value):
                logger.info(f"约束已存在，跳过添加: {constraint_type}={value}")
                return

        if constraint_type == "project_name":
            all_projects = set(self.get_all_projects())
            target_projects = set(value if isinstance(value, list) else [value])
            if target_projects == all_projects:
                logger.info("跳过全集项目约束（无需显示）")
                return

        self.context_manager["constraints"].append({
            "type": constraint_type,
            "value": value,
            "timestamp": datetime.now().isoformat()
        })

        self.context_manager["current_project_set"] = None
        logger.info(f"成功添加约束: {constraint_type}={value}")

    def remove_constraint(self, index: int):
        """移除指定索引的约束"""
        try:
            if self.context_manager is None:
                self.context_manager = {
                    "current_project_set": None,
                    "constraints": [],
                    "session_id": str(uuid.uuid4())
                }

            constraints = self.context_manager.get("constraints", [])
            if not isinstance(constraints, list):
                self.context_manager["constraints"] = []
                raise IndexError("约束列表未初始化")
            
            if 0 <= index < len(constraints):
                removed = constraints.pop(index)
                self.context_manager["current_project_set"] = None
                logger.info(f"已移除约束: {removed['type']}={removed['value']}")
            else:
                raise IndexError(f"无效的约束索引: {index}")
            
            return self._get_context_info()
        except Exception as e:
            logger.error(f"移除约束失败: {str(e)}")
            raise

    def clear_context(self):
        """清除所有上下文约束"""
        self.context_manager = {
            "current_project_set": None,
            "constraints": [],
            "session_id": str(uuid.uuid4())
        }
        logger.info("Context cleared")

    def get_current_project_set(self) -> Set[str]:
        """
        获取当前符合所有上下文约束条件的项目集合
        """
        try:
            if self.context_manager["current_project_set"] is not None:
                cached_set = self.context_manager["current_project_set"]
                logger.debug(f"使用缓存的项目集合，包含 {len(cached_set)} 个项目")
                return cached_set or set()

            all_projects = self.get_all_projects()
            current_set = set(all_projects) if all_projects else set()
            logger.debug(f"初始项目集合大小: {len(current_set)} 个项目")

            for constraint in self.context_manager["constraints"]:
                constraint_type = constraint["type"]
                constraint_value = constraint["value"]
                logger.debug(f"处理约束条件: {constraint_type} = {constraint_value}")

                if constraint_type == "project_name":
                    target_list = constraint_value if isinstance(constraint_value, list) else [constraint_value]
                    
                    project_name_to_code = {}
                    project_code_to_name = {}
                    for project in current_set:
                        meta = self.get_project_metadata(project)
                        if meta and "project_code" in meta:
                            code = meta["project_code"]
                            project_name_to_code[project] = code
                            project_code_to_name[code] = project
                    
                    matched_projects = set()
                    for target in target_list:
                        if target in current_set:
                            matched_projects.add(target)
                        elif target in project_code_to_name:
                            matched_projects.add(project_code_to_name[target])
                        else:
                            for project in current_set:
                                code = project_name_to_code.get(project, "")
                                if target in project or target in code:
                                    matched_projects.add(project)
                    
                    filtered_set = current_set.intersection(matched_projects)
                    logger.debug(f"项目约束后，匹配项目: {len(filtered_set)} 个")
                    current_set = filtered_set

                elif constraint_type == "time_range":
                    start_year, end_year = constraint_value
                    filtered_set = set()
                    
                    project_dates = self._get_project_dates_from_milvus(current_set)

                    for project_name in current_set:
                        start_date = project_dates.get(project_name)
                        
                        if not start_date:
                            logger.debug(f"项目 '{project_name}' 无日期信息，跳过时间筛选")
                            continue
                        
                        try:
                            if len(start_date) == 4 and start_date.isdigit():
                                project_year = int(start_date)
                            else:
                                project_year = int(start_date.split("-")[0])
                        except (ValueError, IndexError, TypeError) as e:
                            logger.warning(f"项目 '{project_name}' 的日期格式无效: {start_date} ({str(e)})")
                            continue
                        
                        in_range = True
                        if start_year is not None and project_year < start_year:
                            in_range = False
                        if end_year is not None and project_year > end_year:
                            in_range = False
                        
                        logger.debug(f"项目 '{project_name}': 年份={project_year}, 范围={start_year}-{end_year}, 在范围内={in_range}")
                        
                        if in_range:
                            filtered_set.add(project_name)
                    
                    logger.debug(f"时间范围约束后，集合大小: {len(filtered_set)} 个项目")
                    current_set = filtered_set

                elif constraint_type == "feature":
                    try:
                        query_vector = self.text_to_vector(constraint_value)
                        search_params = {"metric_type": "COSINE", "params": {"nprobe": 20, "ef": 50}}

                        if not self.collection:
                            logger.error("集合未初始化，无法执行特征搜索")
                            continue
                            
                        search_results = self.collection.search(
                            data=[query_vector],
                            anns_field="embedding",
                            param=search_params,
                            limit=1000,
                            output_fields=["project_name"]
                        )

                        feature_matched_projects = set()
                        for hits in search_results:
                            for hit in hits:
                                project_name = hit.entity.get("project_name")
                                if project_name:
                                    feature_matched_projects.add(project_name)
                        
                        logger.debug(f"特征搜索匹配到 {len(feature_matched_projects)} 个项目")
                        
                        filtered_set = current_set.intersection(feature_matched_projects)
                        logger.debug(f"特征约束后，集合大小: {len(filtered_set)} 个项目")
                        current_set = filtered_set

                    except Exception as e:
                        logger.error(f"特征约束处理失败: {str(e)}，跳过该约束")

                else:
                    logger.warning(f"未知约束类型: {constraint_type}，跳过处理")

                if not current_set:
                    logger.debug("约束处理后集合为空，提前退出")
                    break

            self.context_manager["current_project_set"] = current_set or set()
            logger.info(f"最终符合所有约束的项目集合大小: {len(current_set)} 个项目")

            return current_set or set()
            
        except Exception as e:
            logger.error(f"获取当前项目集合失败: {str(e)}", exc_info=True)
            return set()
        
    def _get_project_dates_from_milvus(self, project_names: Set[str]) -> Dict[str, str]:
        """从Milvus获取项目的日期信息"""
        if not project_names:
            return {}
        
        try:
            expr = f"project_name in {json.dumps(list(project_names))}"
            
            res = self.collection.query(
                expr=expr,
                output_fields=["project_name", "date"],
                limit=10000
            )
            
            project_dates = {}
            for item in res:
                project_name = item["project_name"]
                date_val = item.get("date")
                
                if not date_val:
                    continue
                
                if project_name in project_dates:
                    current_date = project_dates[project_name]
                    if date_val < current_date:
                        project_dates[project_name] = date_val
                else:
                    project_dates[project_name] = date_val
            
            logger.debug(f"从Milvus获取 {len(project_dates)} 个项目的日期信息")
            return project_dates
            
        except Exception as e:
            logger.error(f"从Milvus获取项目日期失败: {str(e)}")
            return {}

    def add_project_metadata(self, project_name: str, metadata: dict):
        """添加或更新项目元数据"""
        if "project_code" not in metadata:
            metadata["project_code"] = "NO_CODE"
            
        self.project_metadata[project_name] = metadata
        logger.info(f"Added metadata for project: {project_name} (Code: {metadata['project_code']})")
        
        if "start_date" not in metadata or not metadata["start_date"]:
            existing_meta = self.project_metadata.get(project_name, {})
            existing_date = existing_meta.get("start_date")
            
            if existing_date:
                metadata["start_date"] = existing_date
                logger.info(f"使用已有日期填充项目 '{project_name}' 元数据: {existing_date}")
            else:
                metadata["start_date"] = datetime.now().strftime("%Y-%m-%d")
                logger.warning(f"项目 '{project_name}' 无日期信息，使用当前日期: {metadata['start_date']}")
        
        self.project_metadata[project_name] = metadata
        logger.info(f"项目 '{project_name}' 元数据已更新")
        
    def get_project_metadata(self, project_name: str) -> Optional[dict]:
        """获取项目元数据"""
        meta = self.project_metadata.get(project_name, {})
        if "project_code" not in meta:
            try:
                expr = f"project_name == '{project_name}'"
                res = self.collection.query(
                    expr=expr, 
                    output_fields=["project_code"],
                    limit=1
                )
                if res and "project_code" in res[0]:
                    meta["project_code"] = res[0]["project_code"]
            except Exception:
                meta["project_code"] = "未知编号"
        return meta

    def text_to_vector(self, text: str) -> List[float]:
        """使用ModelScopeEmbeddings进行文本向量化"""
        try:
            return self.embeddings.embed_query(text)
        except Exception as e:
            logger.error(f"向量化失败: {type(e).__name__} - {str(e)}")
            raise

#    def generate_answer(self, prompt: str) -> str:
#        """使用ModelScope LLM生成回答"""
#        return self.llm.generate(prompt)

    def generate_answer(self, prompt) -> str:
        """
        生成文本回答。
        允许 prompt 为 str、dict 或其他可序列化类型，最终都会转成字符串发送给模型。
        """
        try:
            # 1️⃣ 统一处理输入类型
            if isinstance(prompt, dict):
                # 将字典转为 JSON 字符串
                #import json
                safe_prompt = json.dumps(prompt, ensure_ascii=False)
            else:
                # 其他类型强制转成字符串
                safe_prompt = str(prompt)

            # 2️⃣ 调用模型
            completion = client.chat.completions.create(
                model=MODEL_CONFIG["llm"]["repo_id"],        # 直接读配置
                messages=[{"role": "user", "content": safe_prompt}],
                max_tokens=MODEL_CONFIG["llm"]["max_new_tokens"],
                temperature=MODEL_CONFIG["llm"]["temperature"],
            )

            # 3️⃣ 返回结果
            return completion.choices[0].message.content.strip()

        except Exception as e:
            logger.error(f"生成回答失败: {e}")
            return "抱歉，生成回答时出现错误。"

    def _initialize(self):
        """初始化并验证 API"""
        logger.info("初始化ModelScope组件...")

        if not os.getenv("HF_TOKEN"):
            raise RuntimeError("HF_TOKEN 环境变量未设置")

        for attempt in range(self.max_retries + 1):
            try:
                # 1️⃣ 先测试嵌入
                test_vector = self.text_to_vector("test")
                self.dim = len(test_vector)
                logger.info(f"嵌入模型测试成功，向量维度: {self.dim}")

                # 2️⃣ 测试聊天模型
                completion = client.chat.completions.create(
                    model=MODEL_CONFIG["llm"]["repo_id"],
                    messages=[{"role": "user", "content": "你好"}],
                    temperature=MODEL_CONFIG["llm"]["temperature"],
                    max_tokens=50,
                )
                test_text = completion.choices[0].message.content.strip()
                logger.info(f"问答模型测试响应: {test_text[:50]}...")

                # 3️⃣ 连接 Milvus
                self._connect_milvus()
                logger.info("系统初始化完成")
                return

            except Exception as e:
                logger.error(f"初始化尝试 {attempt+1}/{self.max_retries} 失败: {e}")
                if attempt < self.max_retries:
                    wait_time = min(2 ** attempt, 30)
                    logger.warning(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.critical("所有初始化尝试均失败")
                    raise RuntimeError(f"系统无法初始化: {e}") from e


    def _connect_milvus(self):
        """连接Zilliz Cloud数据库"""
        try:
            logger.info("正在连接Zilliz Cloud数据库...")
            
            connections.connect(
                alias="default",
                uri=ZILLIZ_CONFIG["endpoint"],
                user=ZILLIZ_CONFIG["user"],
                password=ZILLIZ_CONFIG["password"],
                secure=ZILLIZ_CONFIG["secure"]
            )
            
            logger.info("Zilliz Cloud连接成功")
            self._create_collection_if_not_exists()
        except Exception as e:
            logger.error(f"Zilliz Cloud连接失败: {str(e)}")
    
    def _create_collection_if_not_exists(self):
        """如果不存在则创建集合（适配Zilliz Cloud）"""
        try:
            if not utility.has_collection(self.collection_name):
                fields = [
                    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.dim),
                    FieldSchema(name="file_hash", dtype=DataType.VARCHAR, max_length=256),
                    FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=256),
                    FieldSchema(name="project_name", dtype=DataType.VARCHAR, max_length=256),
                    FieldSchema(name="project_code", dtype=DataType.VARCHAR, max_length=50),
                    FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=300),
                    FieldSchema(name="subsection_title", dtype=DataType.VARCHAR, max_length=500),            
                    FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535, nullable=True),
                    FieldSchema(name="date", dtype=DataType.VARCHAR, max_length=20),
                    FieldSchema(name="metadata", dtype=DataType.JSON)
                ]

                schema = CollectionSchema(fields, "Multi-project text search collection")
                
                self.collection = Collection(
                    name=self.collection_name, 
                    schema=schema,
                    consistency_level="Strong"
                )
                
                index_params = {
                    "metric_type": "COSINE",
                    "index_type": "AUTOINDEX",
                    "params": {}
                }

                self.collection.create_index(
                    field_name="embedding", 
                    index_params=index_params,
                    index_name="embedding_index"
                )
                
                self.collection.create_index(
                    field_name="project_name",
                    index_name="project_name_index"
                )
                
                self.collection.create_index(
                    field_name="project_code",
                    index_name="project_code_index"
                )
                
                self.collection.load()
                logger.info(f"Collection '{self.collection_name}' created successfully on Zilliz Cloud.")
            else:
                self.collection = Collection(self.collection_name)
                self.collection.load()
                logger.info(f"Collection '{self.collection_name}' loaded successfully from Zilliz Cloud.")
                
        except Exception as e:
            logger.error(f"集合创建/加载失败: {str(e)}")

    def close(self):
        """关闭资源"""
        if hasattr(self, 'client') and client:
            try:
                client.close()
                logger.info("OpenAI客户端已关闭")
            except:
                pass

    def load_models(self, model_type='default'):
        """由于使用 API 模式，无需加载本地模型"""
        logger.info("当前使用 ModelScope API 模式，跳过本地模型加载")
        return
    
    def parse_document_structure(self, sections: List[Dict]) -> List[Dict]:
        """重构的文档结构解析"""
        chapters = []
        current_chapter = {"title": "未命名章节", "content": [], "subsections": []}
        current_subsection = None
        content_buffer = []
        chapter_counter = 0

        for section in sections:
            try:
                if section["type"] == "main_chapter":
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    if current_chapter["title"] != "未命名章节":
                        chapters.append(current_chapter)
                        chapter_counter += 1
                    
                    current_subsection = None
                    
                    current_chapter = {
                        "title": section["text"],
                        "content": [],
                        "subsections": []
                    }
                    continue

                if section["type"] == "sub_chapter" and section["level"] == 1:
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    current_subsection = {
                        "title": section["text"],
                        "content": [section["text"]],
                        "is_merged": False,
                        "level": 1
                    }
                    current_chapter["subsections"].append(current_subsection)
                    continue

                if section["type"] == "sub_chapter" and section["level"] >= 2:
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    if current_subsection:
                        if "subsections" not in current_subsection:
                            current_subsection["subsections"] = []
                            
                        current_subsection["subsections"].append({
                            "title": section["text"],
                            "content": [section["text"]],
                            "is_merged": False,
                            "level": section["level"]
                        })
                    else:
                        current_subsection = {
                            "title": f"章节{chapter_counter}-未命名",
                            "content": [],
                            "is_merged": False,
                            "level": 1,
                            "subsections": [{
                                "title": section["text"],
                                "content": [section["text"]],
                                "is_merged": False,
                                "level": section["level"]
                            }]
                        }
                        current_chapter["subsections"].append(current_subsection)
                    continue

                content_buffer.append(section["text"])

            except Exception as e:
                print(f"结构解析出错（跳过）：{e}")
                continue

        if content_buffer:
            self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
        
        if current_chapter["title"] != "未命名章节":
            chapters.append(current_chapter)
        
        for chapter in chapters:
            if chapter["content"] and not chapter["subsections"]:
                chapter["subsections"].append({
                    "title": chapter['title'],
                    "content": chapter["content"],
                    "is_merged": False,
                    "level": 1
                })
                chapter["content"] = []
        
        MIN_SUBSECTION_LENGTH = 500
        
        for chapter in chapters:
            top_level_subsections = [sub for sub in chapter["subsections"] if sub.get("level") == 1]
            
            merged_subsections = []
            current_merge = None
            
            for sub in top_level_subsections:
                if "subsections" in sub:
                    for child_sub in sub.get("subsections", []):
                        sub["content"].extend(child_sub["content"])
                
                content_length = sum(len(text) for text in sub["content"])
                
                if content_length < MIN_SUBSECTION_LENGTH:
                    if current_merge is None:
                        current_merge = {
                            "titles": [sub["title"]],
                            "content": sub["content"].copy(),
                            "level": 1
                        }
                    else:
                        current_merge["titles"].append(sub["title"])
                        current_merge["content"].extend(sub["content"])
                else:
                    if current_merge is not None:
                        merged_subsections.append({
                            "title": " · ".join(current_merge["titles"]),
                            "content": current_merge["content"],
                            "is_merged": True,
                            "level": 1
                        })
                        current_merge = None
                    
                    merged_subsections.append({
                        "title": sub["title"],
                        "content": sub["content"],
                        "is_merged": False,
                        "level": 1
                    })
            
            if current_merge is not None:
                merged_subsections.append({
                    "title": " · ".join(current_merge["titles"]),
                    "content": current_merge["content"],
                    "is_merged": True,
                    "level": 1
                })
            
            for sub in merged_subsections:
                original_sub = next((s for s in top_level_subsections if s["title"] in sub["title"]), None)
                if original_sub and "subsections" in original_sub:
                    sub["subsections"] = original_sub["subsections"]
            
            chapter["subsections"] = merged_subsections
        
        return chapters
    
    def _commit_content_buffer(self, buffer: List[str], chapter: Dict, current_subsection: Optional[Dict] = None):
        """安全提交缓冲内容到当前结构"""
        if not buffer:
            return
        
        if current_subsection is not None:
            current_subsection["content"].extend(buffer)
        elif chapter["subsections"]:
            last_sub = chapter["subsections"][-1]
            last_sub["content"].extend(buffer)
        else:
            chapter["content"].extend(buffer)
    
    def determine_section_type(self, text: str, style_name: str, list_value: str) -> str:
        """增强版段落类型判断函数"""
        print(f"参数接收: text={text}, style={style_name}, list={list_value}")
        debug_info = f"判断段落: '{text[:20]}'... | 样式: {style_name} | 编号: {list_value} -> "
        
        if not text.strip():
            logger.debug(debug_info + "EMPTY")
            return "paragraph"

        main_chapter_pattern = r'^(【第[一二三四五六七八九十零\d]+章[^】]*】|第[一二三四五六七八九十零\d]+章\s*.+)'
        if re.match(main_chapter_pattern, text):
            print(debug_info + "MAIN_CHAPTER")
            return "main_chapter"

        sub_chapter_pattern = r'^(\s*' + '|'.join([
            r'\d+[、.)]',
            r'[(（][一二三四五六七八九十零\d]+[)）]',
            r'第?[一二三四五六七八九十零\d]+[节条项]',
            r'[①②③④⑤⑥⑦⑧⑨⑩]',
            r'[A-Za-z]\d?[、.)]',
            r'[IVX]+\.',
            r'【.+】',
            r'[▶♦●■]',
        ]) + r')'
        
        if re.match(sub_chapter_pattern, text.strip()):
            print(debug_info + "SUB_CHAPTER")
            return "sub_chapter"

        if style_name and "heading" in style_name.lower():
            print(debug_info + "HEADING")
            return "heading"

        if list_value and list_value.strip():
            print(debug_info + "AUTO_NUMBERED")
            return "auto_numbered"

        if re.match(r'^[-=]{10,}$', text.strip()):
            print(debug_info + "SECTION_DIVIDER")
            return "section_divider"

        print(debug_info + "PARAGRAPH")
        return "paragraph"

    def read_docx_with_python_docx(self, filepath: str) -> List[Dict]:
        """使用python-docx库读取Word文档（跨平台替代方案）"""
        sections = []
        
        try:
            doc = Document(filepath)
            
            for para in doc.paragraphs:
                try:
                    raw_text = para.text.strip()
                    if not raw_text:
                        continue
                    
                    # 尝试获取样式名称
                    style_name = para.style.name if para.style else ""
                    
                    # 尝试判断是否为列表项
                    list_value = ""
                    list_level = 0
                    
                    # 检查段落是否有编号
                    if para._p.pPr is not None and para._p.pPr.numPr is not None:
                        # 这是一个列表项
                        list_level = 1  # 默认级别为1
                        
                        # 尝试获取更准确的级别信息
                        try:
                            # 通过缩进估计级别
                            if para.paragraph_format.left_indent is not None:
                                indent = para.paragraph_format.left_indent
                                if indent is not None:
                                    # 简单假设每36pt为一级
                                    list_level = max(1, int(indent.pt / 36) + 1)
                        except:
                            pass
                        
                        # 尝试从文本开头提取编号
                        number_match = re.match(r'^(\d+[、.)]|[(（][一二三四五六七八九十零\d]+[)）])', raw_text)
                        if number_match:
                            list_value = number_match.group(1)
                    
                    final_text = raw_text
                    
                    # 手动编号检测
                    manual_number_match = re.match(
                        r'^(\d+[、.)]|[(（][一二三四五六七八九十零\d]+[)）]|第?[一二三四五六七八九十零\d]+[章节条项])', 
                        raw_text
                    )
                    
                    if manual_number_match:
                        final_text = raw_text
                        list_value = manual_number_match.group(1)
                    elif list_value:
                        indent = "\t" * (list_level - 1)
                        final_text = f"{indent}{list_value} {raw_text}"
                    
                    section_type = self.determine_section_type(
                        text=final_text,
                        style_name=style_name,
                        list_value=list_value
                    )
                    
                    sections.append({
                        "type": section_type,
                        "text": final_text,
                        "number": list_value,
                        "level": list_level,
                        "style": style_name,
                        "raw_text": raw_text
                    })
                    
                except Exception as para_error:
                    print(f"段落处理出错（跳过）：{para_error}")
                    continue
                    
            return sections
            
        except Exception as e:
            print(f"文档解析失败：{e}")
            raise

    def _calculate_segment_hash(self, content: str, filename: str, chapter_index: int, subsection_index: int) -> str:
        """为每个片段计算唯一的哈希值"""
        segment_identifier = f"{filename}_chap{chapter_index}_sec{subsection_index}"
        content_with_identifier = content + segment_identifier
        return hashlib.sha256(content_with_identifier.encode()).hexdigest()

    def check_if_file_exists(self, file_hash: str) -> bool:
        expr = f"file_hash == '{file_hash}'"
        try:
            results = self.collection.query(expr=expr, output_fields=["id"])
            return len(results) > 0
        except Exception as e:
            logger.warning(f"查询数据库失败: {e}")
            return False    
       
    @staticmethod
    def log_subsections_to_file(filename: str, chapters: List[Dict], log_dir: str = "./uploads"):
        """生成格式优化的解析日志"""
        try:
            os.makedirs(log_dir, exist_ok=True)
            base_name = os.path.splitext(os.path.basename(filename))[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(log_dir, f"{base_name}_parsed_{timestamp}.txt")
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"文档解析日志：{filename}\n")
                f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总章节数：{len(chapters)}\n")
                f.write("=" * 80 + "\n\n")
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    chap_title = chapter.get('title', f'第{chap_idx}章')
                    f.write("=" * 80 + "\n")
                    f.write(f"【{chap_title}】\n")
                    f.write("=" * 80 + "\n\n")
                    
                    chap_content = '\n'.join(chapter.get('content', []))
                    if chap_content:
                        f.write("[章节主内容]\n")
                        f.write(chap_content + "\n\n")
                    
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        sub_title = subsection.get('title', f'子章节{sub_idx}')
                        level = subsection.get('level', 1)
                        merge_flag = " [合并]" if subsection.get('is_merged', False) else ""
                        f.write("-" * 60 + "\n")
                        f.write(f"* L{level}: {sub_title}{merge_flag}\n")
                        f.write("-" * 60 + "\n")
                        
                        sub_content = '\n'.join(subsection.get('content', []))
                        f.write(sub_content + "\n\n")
                        
                        for child_idx, child_sub in enumerate(subsection.get('subsections', []), 1):
                            child_title = child_sub.get('title', f'子章节{sub_idx}.{child_idx}')
                            child_level = child_sub.get('level', 2)
                            f.write(f"  - L{child_level}: {child_title}\n")
                            child_content = '\n'.join(child_sub.get('content', []))
                            f.write(child_content + "\n\n")
            
            print(f"详细日志文件已生成：{output_path}")
            return output_path
            
        except Exception as e:
            print(f"生成日志失败：{str(e)}")
            return None

    def _process_field(self, field_name: str, value: str) -> str:
        """增强版字段处理"""
        handler = self.field_handlers.get(field_name, {})
        max_len = handler.get('max_len', float('inf'))
        
        if field_name == 'subsection_title':
            true_title = self._extract_true_title(value)
            if len(true_title) <= max_len:
                return true_title
            
            suffix = hashlib.md5(true_title.encode()).hexdigest()[:8]
            return f"{true_title[:max_len-9]}...{suffix}"[:max_len]
        
        if len(value) <= max_len:
            return value
            
        if field_name == 'subsection_title':
            suffix = hashlib.md5(value.encode()).hexdigest()[:8]
            return f"{value[:200]}...{suffix}"[:max_len]
            
        if handler['truncate'] == 'hash':
            keep_len = max(0, max_len - 9)
            suffix = hashlib.md5(value.encode()).hexdigest()[:8]
            return f"{value[:keep_len]}...{suffix}"
        elif handler['truncate'] == 'simple':
            return value[:max_len]
        else:
            raise ValueError(f"Field '{field_name}' exceeds max length ({len(value)} > {max_len})")
        
    def _extract_true_title(self, text: str) -> str:
        """从可能包含额外内容的文本中提取真正的标题"""
        first_line = text.split('\n')[0].strip()
        if len(first_line) <= 100:
            return first_line
        
        possible_title = text[:100]
        if '。' in possible_title or '；' in possible_title:
            for sep in ['。', '；', '\n', '.', ';']:
                if sep in possible_title:
                    return possible_title.split(sep)[0]
        
        return possible_title

    def insert_documents(self, file_dir: str, project_name: str = None, project_date: str = None, project_code: str = None):
        """重构的数据插入方法"""
        success_count = 0
        project_files = {}
        
        if project_name is None:
            project_name = os.path.basename(os.path.normpath(file_dir))
            logger.info(f"Auto-detected project name: {project_name}")
        else:
            project_name = project_name[:256]

        if not project_name or not isinstance(project_name, str):
            raise ValueError("Invalid project name") 
        
        if not project_date:
            project_date = datetime.now().strftime("%Y-%m-%d")
        elif len(project_date) == 4:
            project_date = f"{project_date}-01-01"    
        
        if not project_code:
            project_code = "NO_CODE"
            logger.warning(f"未提供项目编号，使用默认值: {project_code}")                        
        
        processed_hashes = set()
        
        for filename in os.listdir(file_dir):
            if not filename.endswith(".docx"):
                continue
                
            filepath = os.path.join(file_dir, filename)
            try:
                sections = self.read_docx_with_python_docx(filepath)
                chapters = self.parse_document_structure(sections)
                
                log_path = self.log_subsections_to_file(filename, chapters)
                logger.info(f"文档解析完成：{filename}，日志路径：{log_path}")
                
                if project_name not in project_files:
                    project_files[project_name] = []
                project_files[project_name].append(filename)
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        all_content = subsection['content'][:]
                        
                        if 'subsections' in subsection:
                            for child_sub in subsection['subsections']:
                                all_content.extend(child_sub['content'])
                        
                        subsection_content = '\n'.join(all_content)
                        
                        content_hash = hashlib.sha256(subsection_content.encode()).hexdigest()
                        if content_hash in processed_hashes:
                            logger.debug(f"跳过重复内容: {filename} {chap_idx}.{sub_idx}")
                            continue
                        processed_hashes.add(content_hash)
                        
                        try:
                            true_subsection_title = subsection['title']
                            
                            if true_subsection_title == chapter['title']:
                                true_subsection_title = f"{chapter['title']} - 子章节{sub_idx}"
                            
                            sub_hash = self._calculate_segment_hash(subsection_content, filename, chap_idx, sub_idx)
                            
                            metadata = {
                                "section_type": "subsection",
                                "original_title": subsection['title'],
                                "parent_chapter": chapter['title'],
                                "is_merged": subsection.get('is_merged', False),
                                "level": subsection.get('level', 1),
                                "log_path": log_path,
                                "start_date": project_date
                            }
                            
                            if 'subsections' in subsection:
                                metadata["child_subsections"] = [
                                    {"title": child['title'], "level": child.get('level', 2)} 
                                    for child in subsection['subsections']
                                ]
                            
                            doc = {
                                "embedding": self.text_to_vector(subsection_content),
                                "file_hash": sub_hash,
                                "filename": self._process_field('filename', filename),
                                "project_name": project_name,
                                "chapter_title": self._process_field('chapter_title', chapter['title']),
                                "subsection_title": self._process_field('subsection_title', true_subsection_title),
                                "content": subsection_content[:65535],
                                "project_code": project_code,
                                "date": project_date,
                                "metadata": metadata
                            }
                            self.collection.insert([doc])
                            success_count += 1
                            logger.debug(f"插入子章节: {filename} {chap_idx}.{sub_idx} - {true_subsection_title[:30]}...")
                        except Exception as e:
                            logger.error(f"子章节插入失败: {filename} {chap_idx}.{sub_idx} - {str(e)}")
                            
            except Exception as e:
                logger.error(f"文件处理错误（跳过）: {filename} - {str(e)}")

        if project_files:
            if not project_date:
                project_date = datetime.now().strftime("%Y-%m-%d")
                logger.warning(f"未提供项目日期，使用当前日期: {project_date}")
                
            for project, files in project_files.items():
                self.add_project_metadata(project, {
                    "file_count": len(files),
                    "files": files,
                    "last_updated": datetime.now().isoformat(),
                    "project_code": project_code,
                    "start_date": project_date
                })
                logger.info(f"项目 '{project}' 元数据已更新: 开始日期={project_date}")

        self.collection.load()
        logger.info(f"项目 '{project_name}' 处理完成: 成功插入 {success_count} 个片段")
        return success_count

    def execute_search(self, query_text: str, top_k: int = 10, 
                    use_llm: bool = True, keyword_filter: str = None,
                    project_names: Optional[List[str]] = None, 
                    time_range: Optional[Tuple[str, str]] = None,
                    feature_filter: str = None,
                    rerank: bool = False) -> Dict:
        """
        统一的搜索接口，适配Zilliz Cloud
        """
        if project_names:
            self.update_context("project_name", project_names)
        if time_range:
            self.update_context("time_range", time_range)
        if feature_filter:
            self.update_context("feature", feature_filter)
        
        project_set = self.get_current_project_set()
        if not project_set:
            return {
                "answer": "没有符合当前约束条件的项目",
                "type": "error",
                "context_info": self._get_context_info(),
                "results": []
            }
        
        query_vector = self.text_to_vector(query_text)
        
        # Zilliz Cloud搜索参数
        search_params = {
            "metric_type": "COSINE", 
            "params": {"nprobe": 20, "ef": 50}
        }
        
        candidate_k = top_k * 3 if rerank else top_k
        
        expr = f"project_name in {json.dumps(list(project_set))}" if project_set else ""
        
        try:
            raw_results = self.collection.search(
                data=[query_vector], 
                anns_field="embedding", 
                param=search_params, 
                limit=candidate_k,
                expr=expr,
                output_fields=["id", "file_hash", "filename", "project_name", 
                             "chapter_title", "subsection_title", "content", "date", "project_code"]
            )

            candidates = []
            for hits in raw_results:
                for hit in hits:
                    candidates.append({
                        "id": hit.id,
                        "file_hash": hit.entity.get("file_hash"),
                        "filename": hit.entity.get("filename"),
                        "project_name": hit.entity.get("project_name"),
                        "project_code": hit.entity.get("project_code"),
                        "chapter_title": hit.entity.get("chapter_title"),
                        "subsection_title": hit.entity.get("subsection_title"),
                        "content": hit.entity.get("content"),
                        "date": hit.entity.get("date"),
                        "distance": hit.distance
                    })
            
            if rerank and len(candidates) > top_k:
                try:
                    from sentence_transformers import CrossEncoder
                    
                    if not hasattr(self, 'reranker_model') or self.reranker_model is None:
                        self.reranker_model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
                    
                    pairs = [(query_text, doc['content']) for doc in candidates]
                    rerank_scores = self.reranker_model.predict(pairs)
                    
                    for doc, score in zip(candidates, rerank_scores):
                        doc['rerank_score'] = float(score)
                    
                    candidates.sort(key=lambda x: x['rerank_score'], reverse=True)
                
                except ImportError:
                    logger.warning("未安装sentence_transformers，无法重排序，使用原始结果")
                except Exception as e:
                    logger.error(f"重排序失败: {str(e)}，使用原始结果")
            
            if keyword_filter:
                keyword = keyword_filter.lower()
                candidates = [
                    r for r in candidates
                    if (keyword in r['content'].lower() or
                        keyword in r.get('chapter_title', '').lower() or
                        keyword in r.get('subsection_title', '').lower())
                ]
            
            final_results = candidates[:top_k]
            
            if use_llm and final_results:
                answer = self._generate_unified_answer(query_text, final_results)
            else:
                answer = "找到以下相关结果" if final_results else "未找到相关信息"
            
            return {
                "answer": answer,
                "type": "search",
                "context_info": self._get_context_info(),
                "results": final_results
            }
            
        except Exception as e:
            logger.error(f"搜索执行失败: {str(e)}")
            return {
                "answer": f"搜索失败: {str(e)}",
                "type": "error",
                "context_info": self._get_context_info(),
                "results": []
            }
        
    def _generate_unified_answer(self, query: str, results: List[Dict]) -> str:
        """基于多个检索结果，使用 HuggingFace Chat 接口生成统一回答"""
        context = "\n\n".join([
            f"来源文档: {res.get('filename','未知')} | 项目: {res.get('project_name','未知')} | "
            f"章节: {res.get('chapter_title','无章节标题')} - {res.get('subsection_title','无子章节标题')}\n"
            f"内容: {res.get('content','')}"
            for res in results
        ]) or "【无可用上下文】"

        prompt = (
            "你是一个专业的技术文档助手。请基于以下上下文回答问题，"
            "突出技术细节和关键点；若上下文不足，请说明需要补充哪些信息。\n\n"
            f"【上下文】\n{context}\n\n"
            f"【用户问题】\n{query}"
        )

        try:
            # 使用和其它地方一致的 OpenAI 兼容客户端
            completion = client.chat.completions.create(
                model=MODEL_CONFIG["llm"]["repo_id"],
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
                temperature=MODEL_CONFIG["llm"]["temperature"],
            )
            return completion.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM 生成答案失败: {e}")
            return "抱歉，无法生成答案，请稍后再试。"

    
    def calculate_statistics(self, feature: str) -> dict:
        """计算当前项目集合中具有特定特征的项目占比"""
        project_set = self.get_current_project_set()
        
        total_count = len(project_set)
        
        if total_count == 0:
            return {
                "total_projects": 0,
                "featured_projects": 0,
                "percentage": 0.0
            }
        
        feature_query = f"具有特征'{feature}'的项目"
        results = self.execute_search(feature_query, top_k=1000, rerank=False)
        
        featured_projects = set()
        for result in results:
            project_name = result.get("project_name")
            if project_name and project_name in project_set:
                featured_projects.add(project_name)
        
        featured_count = len(featured_projects)
        percentage = (featured_count / total_count) * 100
        
        return {
            "total_projects": total_count,
            "featured_projects": featured_count,
            "percentage": round(percentage, 2)
        }

    def get_project_summary(self) -> Dict[str, Dict]:
        """获取所有项目的统计信息"""
        try:
            res = self.collection.query(
                expr="",
                output_fields=["project_name"],
                limit=10000
            )
            project_names = list(set([item["project_name"] for item in res if "project_name" in item]))
            
            project_stats = {}
            for project in project_names:
                metadata = self.get_project_metadata(project) or {}
                expr = f"project_name == '{project}'"
                count = self.collection.query(expr=expr, count_only=True)
                project_code = metadata.get("project_code", "未知编号")                
                project_stats[project] = {
                    "document_count": count,
                    "last_updated": metadata.get("last_updated", "unknown"),
                    "file_count": metadata.get("file_count", 0),
                    "files": metadata.get("files", []),
                    "project_code": project_code
                }
            return project_stats
        except Exception as e:
            logger.error(f"Failed to get project summary: {e}")
            return {}
    
    def cross_project_search(self, query_text: str, top_k_per_project: int = 3, 
                            rerank: bool = True) -> Dict[str, List[Dict]]:
        """跨项目搜索，返回每个项目的top结果"""
        all_projects = list(self.project_metadata.keys())
        
        if not all_projects:
            res = self.collection.query(
                expr="",
                output_fields=["project_name"],
                partition_names=[],
                limit=10000
            )
            all_projects = list(set([item["project_name"] for item in res]))
        
        results_by_project = {}
        
        for project in all_projects:
            try:
                results = self.execute_search(
                    query_text, 
                    top_k=top_k_per_project, 
                    rerank=rerank, 
                    project_names=[project]
                )
                for result in results.get("results", []):
                    result["project_code"] = self.get_project_metadata(project).get("project_code", "")                
                results_by_project[project] = results
            except Exception as e:
                logger.error(f"Project {project} search failed: {str(e)}")
                results_by_project[project] = []
        
        return results_by_project
    
    def get_all_projects(self) -> List[str]:
        """获取所有项目名称列表"""
        try:
            if self.project_metadata:
                return list(self.project_metadata.keys())
            
            if not self.collection:
                logger.warning("集合未初始化，无法获取项目列表")
                return []
                
            res = self.collection.query(
                expr="",
                output_fields=["project_name"],
                limit=10000
            )
            
            project_names = set()
            for item in res:
                project_name = item.get("project_name")
                if project_name:
                    project_names.add(project_name)
                    
            return list(project_names)
            
        except Exception as e:
            logger.error(f"获取所有项目失败: {str(e)}", exc_info=True)
            return []

    def get_project_description(self, project_name: str) -> str:
        """获取项目描述/特点"""
        if project_name in self.project_metadata and "description" in self.project_metadata[project_name]:
            return self.project_metadata[project_name]["description"]
        
        try:
            search_result = self.execute_search(
                query_text="项目概述",
                top_k=3,
                rerank=True,
                project_names=[project_name],
                use_llm=False,
                output_fields=["content", "filename", "project_name"]
            )
            
            doc_results = search_result.get("results", [])
            if not doc_results:
                return "该项目暂无可用描述信息"
            
            context_snippets = []
            for idx, doc in enumerate(doc_results[:3]):
                content = doc.get("content", "")[:500]
                context_snippets.append(f"文档片段 {idx+1}: {content}")
            
            PROMPT = f"""
            请作为技术分析专家，根据以下文档内容总结项目特点：
            
            项目名称: {project_name}
            
            相关文档片段:
            {chr(10).join(context_snippets)}
            
            输出要求：
            1. 用1-2句话总结核心特点和技术重点
            2. 不超过200字，不编造信息，只基于提供的内容
            3. 语言简洁、专业
            """
            
            llm_description = self.generate_answer(PROMPT)
            
            if project_name not in self.project_metadata:
                self.project_metadata[project_name] = {}
            self.project_metadata[project_name]["description"] = llm_description
            
            return llm_description
            
        except KeyError as e:
            logger.error(f"生成项目描述时字段错误（可能字段不存在）: {str(e)}")
            return "项目描述生成失败（字段错误）"
        except Exception as e:
            logger.error(f"生成项目描述失败: {str(e)}")
            return "无法生成项目描述"

    def find_projects_by_feature(self, feature: str, top_k: int = 5) -> List[Dict]:
        """查找具有特定特点的项目"""
        project_results = self.cross_project_search(
            query_text=feature,
            top_k_per_project=3,
            rerank=True
        )
        
        scored_projects = []
        for project, results in project_results.items():
            if not results:
                continue
                
            best_score = max(
                result.get('rerank_score', 1 - result['distance']) 
                for result in results
            )
            scored_projects.append((project, best_score))
        
        scored_projects.sort(key=lambda x: x[1], reverse=True)
        return [{"project": p, "score": s} for p, s in scored_projects[:top_k]]
    
    def _llm_parse_time_range(self, question: str) -> tuple[Optional[str], Optional[str]]:
        """使用大语言模型解析时间范围"""
        def _normalize_date(date_str: Optional[str]) -> Optional[str]:
            if not date_str:
                return None
            date_str = date_str.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-")
            formats = ["%Y-%m-%d", "%Y-%m", "%Y", "%m-%d-%Y", "%d-%m-%Y"]
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    if fmt == "%Y-%m": return dt.strftime("%Y-%m-01")
                    if fmt == "%Y":   return dt.strftime("%Y-01-01")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None

        system_prompt = "你是一个时间解析专家，请从用户问题中提取时间范围，并严格按JSON格式返回结果。"
        user_prompt = f"""
        ## 输入问题:
        {question}
        ## 输出要求:
        - 必须包含 start_date 和 end_date
        - 日期格式 YYYY-MM-DD
        - 处理示例: “近三年” → 当前年份减3到当前年份
        """

        try:
            completion = client.chat.completions.create(
                model=MODEL_CONFIG["llm"]["repo_id"], 
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=200,
                temperature=MODEL_CONFIG["llm"]["temperature"],
            )
            generated = completion.choices[0].message.content.strip()
            logger.debug(f"LLM原始响应: {generated[:200]}")

            # 提取 JSON
            s, e = generated.find("{"), generated.rfind("}") + 1
            if s >= 0 and e > s:
                data = json.loads(generated[s:e])
                start_date = _normalize_date(data.get("start_date"))
                end_date   = _normalize_date(data.get("end_date"))
                if start_date and end_date and start_date > end_date:
                    logger.warning(f"时间范围无效: {start_date} > {end_date}")
                    return None, None
                if not start_date and not end_date:
                    return None, None
                if not start_date: start_date = "1900-01-01"
                if not end_date:   end_date   = datetime.now().strftime("%Y-%m-%d")
                return start_date, end_date

            logger.error("未找到JSON响应")
            return None, None

        except Exception as e:
            logger.error(f"时间解析异常: {e}")
            return None, None

    def _hybrid_parse_time_range(self, question: str) -> tuple:
        """混合解析策略"""
        start_date, end_date = self._parse_time_range(question)
        if start_date and end_date:
            return (f"{start_date}-01-01", f"{end_date}-12-31")
        
        logger.info(f"正则解析失败，使用LLM解析时间范围: {question}")
        return self._llm_parse_time_range(question)    

    def answer_question(self, question: str) -> Dict:
        """识别问题类型并调用相应处理逻辑"""
        project_names = self._extract_project_names(question)
        time_range = self._hybrid_parse_time_range(question)
        feature = self._extract_feature(question)
        
        return self.execute_search(
            query_text=question,
            top_k=5,
            use_llm=True,
            project_names=project_names,
            time_range=time_range,
            feature_filter=feature
        )
    
    def _extract_project_names(self, question: str) -> List[str]:
        """从问题中提取项目名称"""
        project_pattern = r"项目[\s名称是|名为|叫]?['\"](.+?)['\"]"
        matches = re.findall(project_pattern, question)
        return matches if matches else None
    
    def _extract_feature(self, question: str) -> str:
        """从问题中提取特征描述"""
        feature_pattern = r"具有(.+?)的项目|包含(.+?)的项目"
        match = re.search(feature_pattern, question)
        if match:
            return match.group(1) or match.group(2)
        return None

    def _get_context_info(self) -> dict:
        """获取当前上下文状态信息"""
        project_set = self.get_current_project_set()
        
        formatted_constraints = []
        for constraint in self.context_manager["constraints"]:
            constraint_type = constraint["type"]
            value = constraint["value"]
            
            if constraint_type == "time_range":
                start_year, end_year = value
                if start_year and end_year:
                    formatted_value = f"{start_year}-{end_year}年"
                elif start_year:
                    formatted_value = f"{start_year}年以后"
                elif end_year:
                    formatted_value = f"{end_year}年以前"
                else:
                    continue
            else:
                formatted_value = value
            
            formatted_constraints.append({
                "type": constraint_type,
                "value": formatted_value,
                "timestamp": constraint["timestamp"]
            })
        
        return {
            "project_count": len(project_set),
            "constraints": formatted_constraints,
            "session_id": self.context_manager.get("session_id", str(uuid.uuid4()))
        }
    
    def stream_search(self, query_text, use_llm=True, rerank=False):
        """流式搜索方法，返回生成器"""
        results = self.execute_search(
            query_text=query_text,
            use_llm=use_llm,
            rerank=rerank,
            stream=True
        )
        
        for chunk in self._generate_stream_chunks(results):
            yield f"data: {json.dumps({'text': chunk})}\n\n"
    
    def _generate_stream_chunks(self, results):
        """将搜索结果分块生成"""
        if 'answer' in results:
            answer = results['answer']
            words = answer.split()
            for word in words:
                yield word + " "
                time.sleep(0.05)
        else:
            yield "抱歉，未能生成回答"

    def delete_projects(self, project_names: List[str]) -> Dict[str, Any]:
        """
        删除指定项目的所有数据
        
        参数:
            project_names: 要删除的项目名称列表
            
        返回:
            包含删除结果信息的字典
        """
        if not project_names:
            return {"success": False, "message": "未提供项目名称"}
        
        try:
            expr = f"project_name in {json.dumps(project_names)}"
            
            count_result = self.collection.query(
                expr=expr,
                count_only=True
            )
            
            delete_result = self.collection.delete(expr)
            
            for project in project_names:
                if project in self.project_metadata:
                    del self.project_metadata[project]
            
            self.context_manager["current_project_set"] = None
            
            logger.info(f"成功删除项目: {project_names}, 共删除 {count_result} 条记录")
            
            return {
                "success": True,
                "message": f"成功删除 {len(project_names)} 个项目",
                "deleted_count": count_result,
                "deleted_projects": project_names
            }
            
        except Exception as e:
            logger.error(f"删除项目失败: {str(e)}")
            return {
                "success": False,
                "message": f"删除失败: {str(e)}",
                "deleted_projects": []
            }

    def get_all_projects_with_stats(self) -> Dict[str, Dict]:
        """
        获取所有项目及其统计信息（用于前端显示）
        
        返回:
            项目名称到统计信息的映射
        """
        try:
            all_projects = self.get_all_projects()
            project_stats = {}
            
            for project in all_projects:
                expr = f"project_name == '{project}'"
                count = self.collection.query(expr=expr, count_only=True)
                
                metadata = self.get_project_metadata(project) or {}
                
                project_stats[project] = {
                    "document_count": count,
                    "project_code": metadata.get("project_code", "未知"),
                    "file_count": metadata.get("file_count", 0),
                    "last_updated": metadata.get("last_updated", "未知"),
                    "start_date": metadata.get("start_date", "未知")
                }
                
            return project_stats
            
        except Exception as e:
            logger.error(f"获取项目统计信息失败: {str(e)}")
            return {}
    
    def check_connection(self) -> bool:
        """检查Zilliz Cloud连接状态"""
        try:
            self.collection.query(expr="id >= 0", output_fields=["id"], limit=1)
            return True
        except Exception as e:
            logger.error(f"连接检查失败: {str(e)}")
            return False

    def reconnect(self) -> bool:
        """重新连接Zilliz Cloud"""
        try:
            self.collection.release()
            connections.disconnect("default")
            self._connect_milvus()
            return self.check_connection()
        except Exception as e:
            logger.error(f"重新连接失败: {str(e)}")
            return False