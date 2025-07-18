from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
import logging
import pythoncom
import win32com.client as win32
from datetime import datetime
import requests
import json
import time
from openai import OpenAI
#import urllib3
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed, retry_if_exception_type
#import socket
#import certifi
import ssl
#import http.client
import httpx 
from langchain_community.embeddings import ModelScopeEmbeddings
from typing import Set, List, Dict, Optional, Tuple
import uuid

# 启用详细日志记录（调试时使用）
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vector_search.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VectorSearchSystem")

# 模型配置 - 更新为ModelScope配置
MODEL_CONFIG = {
    'embedding': {
        'model_id': 'iic/nlp_gte_sentence-embedding_chinese-base',
        'api_key': 'a008e17e-882d-4622-ae01-150918799925',
    },
    'llm': {
        'model_id': 'deepseek-ai/DeepSeek-R1-Distill-Qwen-7B',
        'api_base': 'https://api-inference.modelscope.cn/v1/',
        'api_key': 'a008e17e-882d-4622-ae01-150918799925'
    }
}

class ModelScopeLLM:
    """OpenAI兼容封装的ModelScope LLM API"""
    def __init__(self, api_base, api_key, model_id):
        self.client = OpenAI(
            base_url=api_base,
            api_key=api_key
        )
        self.model_id = model_id
        
    # 修改generate方法支持流式输出
    def generate(self, prompt, **kwargs):
        response = self.client.chat.completions.create(
            model=self.model_id,
            messages=[{"role": "user", "content": prompt}],
            stream=True,  # 启用流式
            max_tokens=kwargs.get("max_tokens", 2048),
        )
        
        # 逐步收集响应内容
        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content:
                full_response += chunk.choices[0].delta.content
        return full_response

class VectorSearchSystem:
    def __init__(self, collection_name="text_search", max_retries=5):
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
        
        # 初始化ModelScope组件
        self.embeddings = ModelScopeEmbeddings(
            model_id=MODEL_CONFIG['embedding']['model_id']
        )
        self.llm = ModelScopeLLM(
            api_base=MODEL_CONFIG['llm']['api_base'],
            api_key=MODEL_CONFIG['llm']['api_key'],
            model_id=MODEL_CONFIG['llm']['model_id']
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
        
        参数:
            constraint_type: 约束类型（project_name/time_range/feature）
            value: 约束值（如项目名列表、时间字符串等）
        """
        # 关键修复：确保context_manager不为None
        if self.context_manager is None:
            self.context_manager = {
                "current_project_set": None,
                "constraints": [],
                "session_id": str(uuid.uuid4())
            }

        # 1. 基础校验：空值直接跳过
        if value is None:
            logger.warning("跳过空约束值")
            return
        if isinstance(value, str) and not value.strip():
            logger.warning("跳过空字符串约束")
            return

        # 2. 处理时间范围约束（支持多种格式）
        if constraint_type == "time_range":
            parsed_value = None
            value_str = value.strip() if isinstance(value, str) else str(value)

            # 匹配"2011-2015年"格式（优化正则，兼容空格和特殊字符）
            range_match = re.match(r"^(\d{4})\s*-\s*(\d{4})\s*年$", value_str)
            if range_match:
                start_year = int(range_match.group(1))
                end_year = int(range_match.group(2))
                if start_year <= end_year:
                    parsed_value = (start_year, end_year)
                else:
                    logger.warning(f"时间范围无效（开始年份>结束年份）: {value_str}")
                    return

            # 匹配"2015年以后"格式（优化空格处理）
            after_match = re.match(r"^(\d{4})\s*年\s*以后$", value_str)
            if after_match and not parsed_value:
                start_year = int(after_match.group(1))
                parsed_value = (start_year, None)

            # 匹配"2015年以前"格式（优化空格处理）
            before_match = re.match(r"^(\d{4})\s*年\s*以前$", value_str)
            if before_match and not parsed_value:
                end_year = int(before_match.group(1))
                parsed_value = (None, end_year)

            # 校验时间格式是否有效
            if not parsed_value:
                logger.warning(f"不支持的时间格式: {value_str}（支持：YYYY-YYYY年/YYYY年以后/YYYY年以前）")
                return
            value = parsed_value  # 替换为标准化元组（start_year, end_year）

        # 3. 去重校验：避免添加重复约束
        for existing in self.context_manager["constraints"]:
            if (existing["type"] == constraint_type and 
                existing["value"] == value):
                logger.info(f"约束已存在，跳过添加: {constraint_type}={value}")
                return

        # 4. 避免"全集约束"（如项目名包含所有项目时，无需显示）
        if constraint_type == "project_name":
            all_projects = set(self.get_all_projects())
            target_projects = set(value if isinstance(value, list) else [value])
            if target_projects == all_projects:
                logger.info("跳过全集项目约束（无需显示）")
                return

        # 5. 正式添加约束（记录时间戳，便于前端排序）
        self.context_manager["constraints"].append({
            "type": constraint_type,
            "value": value,
            "timestamp": datetime.now().isoformat()  # 用于前端展示顺序
        })

        # 6. 重置缓存的项目集合（下次查询将重新计算）
        self.context_manager["current_project_set"] = None
        logger.info(f"成功添加约束: {constraint_type}={value}")

    # 2. 修复remove_constraint方法（增加安全校验）
    def remove_constraint(self, index: int):
        """移除指定索引的约束（避免NoneType错误）"""
        try:
            # 关键修复：确保context_manager不为None
            if self.context_manager is None:
                self.context_manager = {
                    "current_project_set": None,
                    "constraints": [],
                    "session_id": str(uuid.uuid4())
                }

            # 确保constraints是列表（防止初始化错误）
            constraints = self.context_manager.get("constraints", [])
            if not isinstance(constraints, list):
                self.context_manager["constraints"] = []
                raise IndexError("约束列表未初始化")
            
            # 检查索引有效性
            if 0 <= index < len(constraints):
                removed = constraints.pop(index)
                self.context_manager["current_project_set"] = None  # 重置缓存
                logger.info(f"已移除约束: {removed['type']}={removed['value']}")
            else:
                raise IndexError(f"无效的约束索引: {index}")
            
            return self._get_context_info()  # 返回更新后的上下文
        except Exception as e:
            logger.error(f"移除约束失败: {str(e)}")
            raise  # 抛给前端处理     

    def clear_context(self):
        """清除所有上下文约束"""
        self.context_manager = {
            "current_project_set": None,
            "constraints": [],
            "session_id": str(uuid.uuid4())  # 生成新会话ID
        }
        logger.info("Context cleared")

    # 增强get_current_project_set方法，处理所有约束条件
    def get_current_project_set(self) -> Set[str]:
        """
        获取当前符合所有上下文约束条件的项目集合（多约束取交集）
        增强错误处理和空值保护
        
        返回:
            符合所有约束条件的项目名称集合（Set[str]），没有项目时返回空集合
        """
        try:
            # 1. 检查缓存，避免重复计算
            if self.context_manager["current_project_set"] is not None:
                cached_set = self.context_manager["current_project_set"]
                logger.debug(f"使用缓存的项目集合，包含 {len(cached_set)} 个项目")
                return cached_set or set()  # 确保返回集合类型

            # 2. 初始化项目集合为所有项目（使用安全方法）
            all_projects = self.get_all_projects()
            current_set = set(all_projects) if all_projects else set()
            logger.debug(f"初始项目集合大小: {len(current_set)} 个项目")

            # 3. 遍历所有约束条件，依次过滤（取交集）
            for constraint in self.context_manager["constraints"]:
                constraint_type = constraint["type"]
                constraint_value = constraint["value"]
                logger.debug(f"处理约束条件: {constraint_type} = {constraint_value}")

                # 3.1 项目名称约束（支持项目名和编号搜索）
                if constraint_type == "project_name":
                    # 标准化输入为列表
                    target_list = constraint_value if isinstance(constraint_value, list) else [constraint_value]
                    
                    # 构建项目名和编号的映射
                    project_name_to_code = {}
                    project_code_to_name = {}
                    for project in current_set:
                        meta = self.get_project_metadata(project)
                        if meta and "project_code" in meta:
                            code = meta["project_code"]
                            project_name_to_code[project] = code
                            project_code_to_name[code] = project
                    
                    # 查找匹配的项目（支持项目名和编号）
                    matched_projects = set()
                    for target in target_list:
                        # 1. 直接匹配项目名称
                        if target in current_set:
                            matched_projects.add(target)
                        
                        # 2. 匹配项目编号
                        elif target in project_code_to_name:
                            matched_projects.add(project_code_to_name[target])
                        
                        # 3. 模糊匹配（项目名或编号包含目标字符串）
                        else:
                            for project in current_set:
                                code = project_name_to_code.get(project, "")
                                if target in project or target in code:
                                    matched_projects.add(project)
                    
                    # 取交集
                    filtered_set = current_set.intersection(matched_projects)
                    logger.debug(f"项目约束后，匹配项目: {len(filtered_set)} 个")
                    current_set = filtered_set

                # 3.2 时间范围约束（基于项目开始年份）
                elif constraint_type == "time_range":
                    start_year, end_year = constraint_value
                    filtered_set = set()
                    
                    # 获取所有项目的日期信息
                    project_dates = self._get_project_dates_from_milvus(current_set)

                    for project_name in current_set:
                        # 从查询结果中获取项目日期
                        start_date = project_dates.get(project_name)
                        
                        if not start_date:
                            logger.debug(f"项目 '{project_name}' 无日期信息，跳过时间筛选")
                            continue
                        
                        # 提取年份（增强格式兼容性）
                        try:
                            # 处理多种日期格式：YYYY, YYYY-MM, YYYY-MM-DD
                            if len(start_date) == 4 and start_date.isdigit():
                                project_year = int(start_date)
                            else:
                                # 尝试解析日期字符串
                                project_year = int(start_date.split("-")[0])
                        except (ValueError, IndexError, TypeError) as e:
                            logger.warning(f"项目 '{project_name}' 的日期格式无效: {start_date} ({str(e)})")
                            continue
                        
                        # 检查是否在时间范围内
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

                # 3.3 特征约束（基于向量搜索的语义匹配）
                elif constraint_type == "feature":
                    try:
                        # 对特征描述生成向量，搜索相关项目
                        query_vector = self.text_to_vector(constraint_value)
                        search_params = {"metric_type": "COSINE", "params": {"ef": 50}}

                        # 搜索相关项目（限制返回1000个）
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

                        # 提取匹配的项目名称（去重）
                        feature_matched_projects = set()
                        for hits in search_results:
                            for hit in hits:
                                project_name = hit.entity.get("project_name")
                                if project_name:
                                    feature_matched_projects.add(project_name)
                        
                        logger.debug(f"特征搜索匹配到 {len(feature_matched_projects)} 个项目")
                        
                        # 取交集：保留当前集合中在特征匹配结果中的项目
                        filtered_set = current_set.intersection(feature_matched_projects)
                        logger.debug(f"特征约束后，集合大小: {len(filtered_set)} 个项目")
                        current_set = filtered_set

                    except Exception as e:
                        logger.error(f"特征约束处理失败: {str(e)}，跳过该约束")
                        # 失败时保留当前集合继续处理其他约束

                # 3.4 未知约束类型（跳过）
                else:
                    logger.warning(f"未知约束类型: {constraint_type}，跳过处理")
                    # 保持当前集合不变

                # 提前退出：如果集合已为空，无需继续处理其他约束
                if not current_set:
                    logger.debug("约束处理后集合为空，提前退出")
                    break

            # 4. 缓存结果，避免重复计算
            self.context_manager["current_project_set"] = current_set or set()  # 确保缓存的是集合
            logger.info(f"最终符合所有约束的项目集合大小: {len(current_set)} 个项目")

            return current_set or set()  # 确保返回集合类型
            
        except Exception as e:
            logger.error(f"获取当前项目集合失败: {str(e)}", exc_info=True)
            return set()  # 返回空集合保证前端不崩溃
        
    def _get_project_dates_from_milvus(self, project_names: Set[str]) -> Dict[str, str]:
        """从Milvus获取项目的日期信息（使用聚合查询）"""
        if not project_names:
            return {}
        
        try:
            # 构建查询表达式
            expr = f"project_name in {json.dumps(list(project_names))}"
            
            # 执行聚合查询获取每个项目的最早日期
            res = self.collection.query(
                expr=expr,
                output_fields=["project_name", "date"],
                limit=10000  # 假设项目文档不超过10000个
            )
            
            # 按项目分组，取每个项目的最小日期
            project_dates = {}
            for item in res:
                project_name = item["project_name"]
                date_val = item.get("date")
                
                if not date_val:
                    continue
                
                # 如果项目已有日期，取最小值（最早日期）
                if project_name in project_dates:
                    current_date = project_dates[project_name]
                    # 比较日期字符串（格式为YYYY-MM-DD）
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
        """添加或更新项目元数据（确保日期字段存在）"""
        # 确保包含项目编号
        if "project_code" not in metadata:
            metadata["project_code"] = "NO_CODE"
            
        self.project_metadata[project_name] = metadata
        logger.info(f"Added metadata for project: {project_name} (Code: {metadata['project_code']})")
        # 确保包含时间信息
        if "start_date" not in metadata or not metadata["start_date"]:
            # 尝试从已有数据获取日期
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
        """获取项目元数据（确保包含项目编号）"""
        meta = self.project_metadata.get(project_name, {})
        # 确保元数据中包含项目编号
        if "project_code" not in meta:
            # 尝试从Milvus中获取项目编号
            try:
                expr = f"project_name == '{project_name}'"
                res = self.collection.query(
                    expr=expr, 
                    output_fields=["project_code"],
                    limit=1
                )
                if res and "project_code" in res[0]:#########################################
                    meta["project_code"] = res[0]["project_code"]
            except Exception:
                meta["project_code"] = "未知编号"
        return meta

    def text_to_vector(self, text: str) -> List[float]:
        """使用ModelScopeEmbeddings进行文本向量化"""
        try:
            # 直接调用ModelScopeEmbeddings
            return self.embeddings.embed_query(text)
        except Exception as e:
            logger.error(f"向量化失败: {type(e).__name__} - {str(e)}")
            raise

    def generate_answer(self, prompt: str) -> str:
        """使用ModelScope LLM生成回答"""
        return self.llm.generate(prompt)

    def _initialize(self):
        """初始化并验证 API"""
        logger.info("初始化ModelScope组件...")
        
        for attempt in range(self.max_retries + 1):
            try:
                # 1. 测试嵌入模型
                test_vector = self.text_to_vector("test")
                self.dim = len(test_vector)
                logger.info(f"嵌入模型测试成功，向量维度: {self.dim}")
                
                # 2. 测试问答模型
                test_response = self.generate_answer("你好")
                logger.info(f"问答模型测试响应: {test_response[:50]}...")
                
                # 3. 连接Milvus
                self._connect_milvus()
                logger.info("系统初始化完成")
                return
                
            except Exception as e:
                logger.error(f"初始化尝试 {attempt+1}/{self.max_retries} 失败: {str(e)}")
                if attempt < self.max_retries:
                    wait_time = min(2 ** attempt, 30)  # 指数退避
                    logger.warning(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.critical("所有初始化尝试均失败")
                    raise RuntimeError("系统无法初始化") from e

    def _connect_milvus(self):
        """连接Milvus数据库"""
        try:
            from pymilvus import connections
            logger.info("正在连接Milvus数据库...")
            connections.connect("default", host="localhost", port="19530")
            logger.info("Milvus连接成功")
            self._create_collection_if_not_exists()
        except ImportError:
            logger.warning("pymilvus未安装，跳过数据库连接")
        except Exception as e:
            logger.error(f"Milvus连接失败: {str(e)}")
            # 非关键错误，允许继续运行
    
    def _create_collection_if_not_exists(self):
        """如果不存在则创建Milvus集合"""
        if not utility.has_collection(self.collection_name):
            # 定义字段
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.dim),
                FieldSchema(name="file_hash", dtype=DataType.VARCHAR, max_length=256),
                FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=256),
                FieldSchema(name="project_name", dtype=DataType.VARCHAR, max_length=256),  # 新增项目名字段
                FieldSchema(name="project_code", dtype=DataType.VARCHAR, max_length=50),  # 新增项目编号字段
                FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=300),
                FieldSchema(name="subsection_title", dtype=DataType.VARCHAR, max_length=500),            
                FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535, nullable=True),
                FieldSchema(name="date", dtype=DataType.VARCHAR, max_length=20),  # 新增日期字段
                FieldSchema(name="metadata", dtype=DataType.JSON)
            ]

            # 创建集合模式
            schema = CollectionSchema(fields, "Multi-project text search collection")
            
            # 创建集合
            self.collection = Collection(name=self.collection_name, schema=schema)
            
            # 创建索引
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {"M": 24, "efConstruction": 300}
            }

            self.collection.create_index(field_name="embedding", index_params=index_params)
            self.collection.load()
            logger.info(f"Collection '{self.collection_name}' created with project_name field.")
        else:
            self.collection = Collection(name=self.collection_name)
            self.collection.load()
            logger.info(f"Collection '{self.collection_name}' loaded successfully.")

    def close(self):
        """关闭资源"""
        if hasattr(self, 'client') and self.client:
            try:
                self.client.close()
                logger.info("OpenAI客户端已关闭")
            except:
                pass

    
    def load_models(self, model_type='default'):
        """由于使用 API 模式，无需加载本地模型"""
        logger.info("当前使用 ModelScope API 模式，跳过本地模型加载")
        return
    
    def parse_document_structure(self, sections: List[Dict]) -> List[Dict]:
        """重构的文档结构解析（精确匹配章节层级）"""
        chapters = []
        current_chapter = {"title": "未命名章节", "content": [], "subsections": []}
        current_subsection = None
        content_buffer = []  # 内容缓冲队列
        chapter_counter = 0  # 章节计数器

        for section in sections:
            try:
                # === 主章节处理 ===
                if section["type"] == "main_chapter":
                    # 提交缓冲内容到当前结构
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    # 保存当前章节（非初始状态时）
                    if current_chapter["title"] != "未命名章节":
                        chapters.append(current_chapter)
                        chapter_counter += 1
                    
                    # 重置当前子章节
                    current_subsection = None
                    
                    # 新建章节
                    current_chapter = {
                        "title": section["text"],
                        "content": [],
                        "subsections": []
                    }
                    continue

                # === 子章节处理（一级子章节） ===
                if section["type"] == "sub_chapter" and section["level"] == 1:
                    # 提交缓冲内容
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    # 创建新子章节
                    current_subsection = {
                        "title": section["text"],
                        "content": [section["text"]],  # 标题作为首行
                        "is_merged": False,
                        "level": 1
                    }
                    current_chapter["subsections"].append(current_subsection)
                    continue

                # === 二级子章节处理 ===
                if section["type"] == "sub_chapter" and section["level"] >= 2:
                    # 提交缓冲内容
                    if content_buffer:
                        self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
                        content_buffer = []
                    
                    # 创建二级子章节（归属于当前一级子章节）
                    if current_subsection:
                        # 如果当前没有一级子章节，则创建占位的一级子章节
                        if "subsections" not in current_subsection:
                            current_subsection["subsections"] = []
                            
                        # 添加二级子章节
                        current_subsection["subsections"].append({
                            "title": section["text"],
                            "content": [section["text"]],
                            "is_merged": False,
                            "level": section["level"]
                        })
                    else:
                        # 没有一级子章节时直接创建一级子章节
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

                # === 普通内容处理 ===
                content_buffer.append(section["text"])

            except Exception as e:
                print(f"结构解析出错（跳过）：{e}")
                continue

        # === 最终处理 ===
        # 1. 提交剩余缓冲内容
        if content_buffer:
            self._commit_content_buffer(content_buffer, current_chapter, current_subsection)
        
        # 2. 保存最后一个章节
        if current_chapter["title"] != "未命名章节":
            chapters.append(current_chapter)
        
        # 3. 后处理：修复孤立内容
        for chapter in chapters:
            # 情况1：有主内容但无子章节 -> 转换为子章节
            if chapter["content"] and not chapter["subsections"]:
                chapter["subsections"].append({
                    "title": chapter['title'],
                    "content": chapter["content"],
                    "is_merged": False,
                    "level": 1
                })
                chapter["content"] = []
        
        # 4. 重构的合并算法（仅合并一级子章节）
        MIN_SUBSECTION_LENGTH = 500  # 最小字数阈值
        
        for chapter in chapters:
            # 只处理一级子章节
            top_level_subsections = [sub for sub in chapter["subsections"] if sub.get("level") == 1]
            
            merged_subsections = []
            current_merge = None  # 当前合并组
            
            for sub in top_level_subsections:
                # 合并二级子章节内容到一级子章节
                if "subsections" in sub:
                    for child_sub in sub.get("subsections", []):
                        sub["content"].extend(child_sub["content"])
                
                content_length = sum(len(text) for text in sub["content"])
                
                # 当前子章节需要合并
                if content_length < MIN_SUBSECTION_LENGTH:
                    if current_merge is None:
                        # 开始新的合并组
                        current_merge = {
                            "titles": [sub["title"]],
                            "content": sub["content"].copy(),
                            "level": 1
                        }
                    else:
                        # 添加到现有合并组
                        current_merge["titles"].append(sub["title"])
                        current_merge["content"].extend(sub["content"])
                else:
                    # 提交当前合并组（如果有）
                    if current_merge is not None:
                        merged_subsections.append({
                            "title": " · ".join(current_merge["titles"]),
                            "content": current_merge["content"],
                            "is_merged": True,
                            "level": 1
                        })
                        current_merge = None
                    
                    # 添加合格子章节
                    merged_subsections.append({
                        "title": sub["title"],
                        "content": sub["content"],
                        "is_merged": False,
                        "level": 1
                    })
            
            # 处理章节末尾的合并组
            if current_merge is not None:
                merged_subsections.append({
                    "title": " · ".join(current_merge["titles"]),
                    "content": current_merge["content"],
                    "is_merged": True,
                    "level": 1
                })
            
            # 保留二级子章节信息
            for sub in merged_subsections:
                # 从原始子章节中恢复二级子章节
                original_sub = next((s for s in top_level_subsections if s["title"] in sub["title"]), None)
                if original_sub and "subsections" in original_sub:
                    sub["subsections"] = original_sub["subsections"]
            
            chapter["subsections"] = merged_subsections
        
        return chapters
    
    def _commit_content_buffer(self, buffer: List[str], chapter: Dict, current_subsection: Optional[Dict] = None):
        """安全提交缓冲内容到当前结构（修复内容归属问题）
        
        参数:
            buffer: 待提交的内容列表
            chapter: 当前章节
            current_subsection: 当前活动的子章节（可选）
        """
        if not buffer:
            return
        
        # 优先提交到当前子章节（如果存在）
        if current_subsection is not None:
            current_subsection["content"].extend(buffer)
        # 其次提交到最后一个子章节
        elif chapter["subsections"]:
            last_sub = chapter["subsections"][-1]
            last_sub["content"].extend(buffer)
        else:
            # 没有子章节则提交到章节内容
            chapter["content"].extend(buffer)
    
    def determine_section_type(self, text: str, style_name: str, list_value: str) -> str:
        """增强版段落类型判断函数
        
        改进点：
        1. 支持更多中文编号格式（如"一、", "(二)", "第三节"）
        2. 兼容Word自动编号和手动编号
        3. 增强标题特征检测
        4. 添加详细的调试日志

        Args:
            text: 段落文本内容
            style_name: Word样式名称
            list_value: Word自动编号值（如"1."）

        Returns:
            str: 段落类型标识：
                - "main_chapter"   : 主章节标题（第X章）
                - "sub_chapter"    : 子章节标题
                - "heading"       : Word样式标记的标题
                - "auto_numbered"  : Word自动编号段落
                - "section_divider": 分隔线
                - "paragraph"      : 普通段落
        """
        # 调试信息
        print(f"参数接收: text={text}, style={style_name}, list={list_value}")
        debug_info = f"判断段落: '{text[:20]}'... | 样式: {style_name} | 编号: {list_value} -> "
        
        # 0. 空内容处理
        if not text.strip():
            logger.debug(debug_info + "EMPTY")
            return "paragraph"

        # 1. 主章节识别（支持【第X章】和独立章节标题）
        main_chapter_pattern = r'^(【第[一二三四五六七八九十零\d]+章[^】]*】|第[一二三四五六七八九十零\d]+章\s*.+)'
        if re.match(main_chapter_pattern, text):
            print(debug_info + "MAIN_CHAPTER")
            return "main_chapter"

        # 2. 子章节识别（增强的编号格式支持）
        sub_chapter_pattern = r'^(\s*' + '|'.join([
            r'\d+[、.)]',                  # 数字编号：1、 1. 1)
            r'[(（][一二三四五六七八九十零\d]+[)）]',  # 中文括号：(一) （二）
            r'第?[一二三四五六七八九十零\d]+[节条项]',  # 中文编号：第一节 第二条
            r'[①②③④⑤⑥⑦⑧⑨⑩]',            # 圆圈数字
            r'[A-Za-z]\d?[、.)]',          # 字母编号：A、 B1.
            r'[IVX]+\.',                   # 罗马数字：I. II.
            r'【.+】',                      # 方括号标题：【引言】
            r'[▶♦●■]',                     # 特殊符号标题
        ]) + r')'
        
        if re.match(sub_chapter_pattern, text.strip()):
            print(debug_info + "SUB_CHAPTER")
            return "sub_chapter"

        # 3. Word样式识别（Heading样式优先于编号）
        if style_name and "heading" in style_name.lower():
            print(debug_info + "HEADING")
            return "heading"

        # 4. 自动编号段落（Word自动生成的编号）
        if list_value and list_value.strip():
            print(debug_info + "AUTO_NUMBERED")
            return "auto_numbered"

        # 5. 分隔线检测（至少10个连续-或=）
        if re.match(r'^[-=]{10,}$', text.strip()):
            print(debug_info + "SECTION_DIVIDER")
            return "section_divider"

        # 6. 默认作为普通段落
        print(debug_info + "PARAGRAPH")
        return "paragraph"


    def read_docx_with_win32com(self, filepath: str) -> List[Dict]:
        """增强版Word文档解析（解决final_text未定义问题）"""
        pythoncom.CoInitialize()
        word = None
        doc = None
        sections = []

        try:
            word = win32.Dispatch("Word.Application")
            word.Visible = False
            word.DisplayAlerts = False

            doc = word.Documents.Open(
                FileName=os.path.abspath(filepath),
                ReadOnly=True,
                ConfirmConversions=False,
                AddToRecentFiles=False
            )

            for para in doc.Paragraphs:
                try:
                    # === 基础文本提取 ===
                    raw_text = para.Range.Text.strip()
                    if not raw_text:
                        continue

                    # === 编号检测 ===
                    list_value = ""
                    list_level = 0
                    list_levels = []
                    try:
                        if para.Range.ListFormat.ListType != 0:
                            list_level = para.Range.ListFormat.ListLevelNumber
                            for level in range(1, list_level + 1):
                                para.Range.ListFormat.ListLevelNumber = level
                                list_levels.append(para.Range.ListFormat.ListString)
                            list_value = ".".join(list_levels)
                    except Exception as list_err:
                        print(f"获取编号时出错：{list_err}")

                    # === 样式检测 ===
                    style_name = ""
                    try:
                        style_name = para.Style.NameLocal
                    except:
                        pass

                    # === 智能文本生成 ===
                    # 初始化final_text为raw_text
                    final_text = raw_text
                    
                    # 检测手动编号（优先级高于自动编号）
                    manual_number_match = re.match(
                        r'^(\d+[、.)]|[(（][一二三四五六七八九十零\d]+[)）]|第?[一二三四五六七八九十零\d]+[章节条项])', 
                        raw_text
                    )
                    
                    if manual_number_match:
                        # 情况1：存在手动编号 -> 直接使用原始文本
                        final_text = raw_text
                        list_value = ""  # 清空自动编号
                    elif list_value:
                        # 情况2：只有自动编号 -> 添加缩进
                        indent = "\t" * (len(list_levels) - 1)
                        final_text = f"{indent}{list_value} {raw_text}"
                    # 情况3：无编号 -> 保持原样

                    # === 类型判断 ===
                    section_type = self.determine_section_type(
                        text=final_text,
                        style_name=style_name,
                        list_value=list_value
                    )

                    # === 记录段落 ===
                    sections.append({
                        "type": section_type,
                        "text": final_text,
                        "number": list_value,
                        "level": list_level,
                        "style": style_name,
                        "raw_text": raw_text  # 原始文本备份
                    })

                except Exception as para_error:
                    print(f"段落处理出错（跳过）：{para_error}")
                    continue

            return sections

        except Exception as e:
            print(f"文档解析失败：{e}")
            raise
        finally:
            # 确保资源释放
            try:
                if doc:
                    doc.Close(SaveChanges=False)
            except:
                pass
            try:
                if word:
                    word.Quit()
            except:
                pass
            pythoncom.CoUninitialize()

    def _calculate_segment_hash(self, content: str, filename: str, chapter_index: int, subsection_index: int) -> str:
        """为每个片段计算唯一的哈希值
        
        参数:
            content: 文本内容
            filename: 文件名
            chapter_index: 章节索引
            subsection_index: 子章节索引
            
        返回:
            16进制哈希字符串
        """
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
        """生成格式优化的解析日志（显示层级结构）"""
        try:
            os.makedirs(log_dir, exist_ok=True)
            base_name = os.path.splitext(os.path.basename(filename))[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(log_dir, f"{base_name}_parsed_{timestamp}.txt")
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # 文件头信息
                f.write("=" * 80 + "\n")
                f.write(f"文档解析日志：{filename}\n")
                f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总章节数：{len(chapters)}\n")
                f.write("=" * 80 + "\n\n")
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    # 章节标题
                    chap_title = chapter.get('title', f'第{chap_idx}章')
                    f.write("=" * 80 + "\n")
                    f.write(f"【{chap_title}】\n")
                    f.write("=" * 80 + "\n\n")
                    
                    # 章节内容
                    chap_content = '\n'.join(chapter.get('content', []))
                    if chap_content:
                        f.write("[章节主内容]\n")
                        f.write(chap_content + "\n\n")
                    
                    # 一级子章节处理
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        sub_title = subsection.get('title', f'子章节{sub_idx}')
                        level = subsection.get('level', 1)
                        merge_flag = " [合并]" if subsection.get('is_merged', False) else ""
                        f.write("-" * 60 + "\n")
                        f.write(f"* L{level}: {sub_title}{merge_flag}\n")
                        f.write("-" * 60 + "\n")
                        
                        # 一级子章节内容
                        sub_content = '\n'.join(subsection.get('content', []))
                        f.write(sub_content + "\n\n")
                        
                        # 二级子章节处理
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
        """增强版字段处理（特别处理子章节标题）"""
        handler = self.field_handlers.get(field_name, {})
        max_len = handler.get('max_len', float('inf'))
        
        # 特别处理子章节标题
        if field_name == 'subsection_title':
            # 提取真正的标题部分（通常在第一行或前100个字符内）
            true_title = self._extract_true_title(value)
            if len(true_title) <= max_len:
                return true_title
            
            # 如果仍然超长，使用哈希后缀
            suffix = hashlib.md5(true_title.encode()).hexdigest()[:8]
            return f"{true_title[:max_len-9]}...{suffix}"[:max_len]
        
        if len(value) <= max_len:
            return value
            
        # 特殊处理subsection_title
        if field_name == 'subsection_title':
            suffix = hashlib.md5(value.encode()).hexdigest()[:8]
            return f"{value[:200]}...{suffix}"[:max_len]
            
        # 其他字段处理
        if handler['truncate'] == 'hash':
            keep_len = max(0, max_len - 9)  # 保留8位哈希
            suffix = hashlib.md5(value.encode()).hexdigest()[:8]
            return f"{value[:keep_len]}...{suffix}"
        elif handler['truncate'] == 'simple':
            return value[:max_len]
        else:
            raise ValueError(f"Field '{field_name}' exceeds max length ({len(value)} > {max_len})")
        
    def _extract_true_title(self, text: str) -> str:
        """从可能包含额外内容的文本中提取真正的标题"""
        # 情况1：标题在第一行
        first_line = text.split('\n')[0].strip()
        if len(first_line) <= 100:  # 假设真实标题不会超过100字符
            return first_line
        
        # 情况2：标题在开头部分
        possible_title = text[:100]
        if '。' in possible_title or '；' in possible_title:
            # 如果有标点符号，取第一个标点前的部分
            for sep in ['。', '；', '\n', '.', ';']:
                if sep in possible_title:
                    return possible_title.split(sep)[0]
        
        # 情况3：无法确定，返回前100字符
        return possible_title

    def _commit_content_buffer(self, buffer: List[str], chapter: Dict, current_subsection: Optional[Dict] = None):
        """安全提交缓冲内容到当前结构（修复内容归属问题）
        
        参数:
            buffer: 待提交的内容列表
            chapter: 当前章节
            current_subsection: 当前活动的子章节（可选）
        """
        if not buffer:
            return
        
        # 优先提交到当前子章节（如果存在）
        if current_subsection is not None:
            current_subsection["content"].extend(buffer)
        # 其次提交到最后一个子章节
        elif chapter["subsections"]:
            last_sub = chapter["subsections"][-1]
            last_sub["content"].extend(buffer)
        else:
            # 没有子章节则提交到章节内容
            chapter["content"].extend(buffer)

    def insert_documents(self, file_dir: str, project_name: str = None, project_date: str = None, project_code: str = None):
        """重构的数据插入方法（支持项目和项目元数据）"""
        success_count = 0
        project_files = {}
        
        # 自动从目录结构推断项目名
        if project_name is None:
            project_name = os.path.basename(os.path.normpath(file_dir))
            logger.info(f"Auto-detected project name: {project_name}")
        else:
            # 确保项目名称不超过最大长度
            project_name = project_name[:256]

        # 验证项目名称
        if not project_name or not isinstance(project_name, str):
            raise ValueError("Invalid project name") 
        
        if not project_date:
            project_date = datetime.now().strftime("%Y-%m-%d")
        elif len(project_date) == 4:  # 只有年份
            project_date = f"{project_date}-01-01"    
        # 验证项目编号
        if not project_code:
            project_code = "NO_CODE"
            logger.warning(f"未提供项目编号，使用默认值: {project_code}")                        
        
        # 关键修复：在整个项目处理过程中使用同一个哈希集合
        processed_hashes = set()
        
        for filename in os.listdir(file_dir):
            if not filename.endswith(".docx"):
                continue
                
            filepath = os.path.join(file_dir, filename)
            try:
                pythoncom.CoInitialize()
                sections = self.read_docx_with_win32com(filepath)
                chapters = self.parse_document_structure(sections)
                
                # 生成解析日志文件
                log_path = self.log_subsections_to_file(filename, chapters)
                logger.info(f"文档解析完成：{filename}，日志路径：{log_path}")
                
                # 记录项目文件关系
                if project_name not in project_files:
                    project_files[project_name] = []
                project_files[project_name].append(filename)
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    # 处理一级子章节
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        # 合并所有内容：一级子章节内容 + 其下的二级子章节内容
                        all_content = subsection['content'][:]
                        
                        # 添加二级子章节内容
                        if 'subsections' in subsection:
                            for child_sub in subsection['subsections']:
                                all_content.extend(child_sub['content'])
                        
                        subsection_content = '\n'.join(all_content)
                        
                        # 计算内容哈希，检查是否已处理（关键修复：使用全局哈希集合）
                        content_hash = hashlib.sha256(subsection_content.encode()).hexdigest()
                        if content_hash in processed_hashes:
                            logger.debug(f"跳过重复内容: {filename} {chap_idx}.{sub_idx}")
                            continue
                        processed_hashes.add(content_hash)
                        
                        try:
                            # 使用一级子章节标题
                            true_subsection_title = subsection['title']
                            
                            # 确保子章节标题与章节标题不同
                            if true_subsection_title == chapter['title']:
                                true_subsection_title = f"{chapter['title']} - 子章节{sub_idx}"
                            
                            sub_hash = self._calculate_segment_hash(subsection_content, filename, chap_idx, sub_idx)
                            
                            # 元数据记录层级信息
                            metadata = {
                                "section_type": "subsection",
                                "original_title": subsection['title'],
                                "parent_chapter": chapter['title'],
                                "is_merged": subsection.get('is_merged', False),
                                "level": subsection.get('level', 1),
                                "log_path": log_path,
                                "start_date": project_date  # 使用相同的项目日期
                            }
                            
                            # 添加二级子章节信息
                            if 'subsections' in subsection:
                                metadata["child_subsections"] = [
                                    {"title": child['title'], "level": child.get('level', 2)} 
                                    for child in subsection['subsections']
                                ]
                            
                            # 关键修复：添加 project_code 字段
                            doc = {
                                "embedding": self.text_to_vector(subsection_content),
                                "file_hash": sub_hash,
                                "filename": self._process_field('filename', filename),
                                "project_name": project_name,
                                "chapter_title": self._process_field('chapter_title', chapter['title']),
                                "subsection_title": self._process_field('subsection_title', true_subsection_title),
                                "content": subsection_content[:65535],
                                "project_code": project_code,  # 修复：添加项目编号字段
                                "date": project_date,  # 添加日期字段
                                "metadata": metadata
                            }
                            self.collection.insert([doc])
                            success_count += 1
                            logger.debug(f"插入子章节: {filename} {chap_idx}.{sub_idx} - {true_subsection_title[:30]}...")
                        except Exception as e:
                            logger.error(f"子章节插入失败: {filename} {chap_idx}.{sub_idx} - {str(e)}")
                            
            except Exception as e:
                logger.error(f"文件处理错误（跳过）: {filename} - {str(e)}")
            finally:
                pythoncom.CoUninitialize()

        # 保存项目元数据（确保start_date被正确设置）
        if project_files:
            # 使用传入的project_date，如果没有则使用当前日期
            if not project_date:
                project_date = datetime.now().strftime("%Y-%m-%d")
                logger.warning(f"未提供项目日期，使用当前日期: {project_date}")
                
            for project, files in project_files.items():
                self.add_project_metadata(project, {
                    "file_count": len(files),
                    "files": files,
                    "last_updated": datetime.now().isoformat(),
                    "project_code": project_code,  # 存储项目编号
                    "start_date": project_date  # 确保设置start_date
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
                    rerank: bool = False) -> Dict:  # 添加 rerank 参数
        """
        统一的搜索接口，整合原answer_question、search和hybrid_search功能
        添加 rerank 参数控制是否启用重排序
        """
        # 更新上下文约束
        if project_names:
            self.update_context("project_name", project_names)
        if time_range:
            self.update_context("time_range", time_range)
        if feature_filter:
            self.update_context("feature", feature_filter)
        
        # 获取当前项目集合（基于上下文约束）
        project_set = self.get_current_project_set()
        if not project_set:
            return {
                "answer": "没有符合当前约束条件的项目",
                "type": "error",
                "context_info": self._get_context_info(),
                "results": []
            }
        
        # 向量搜索
        query_vector = self.text_to_vector(query_text)
        search_params = {"metric_type": "COSINE", "params": {"ef": 50}}
        candidate_k = top_k * 3 if rerank else top_k  # 重排序时多取一些候选结果

        raw_results = self.collection.search(
            data=[query_vector], 
            anns_field="embedding", 
            param=search_params, 
            limit=candidate_k,  # 使用动态候选数量
            expr=f"project_name in {json.dumps(list(project_set))}",  # 添加上下文过滤
            output_fields=["id", "file_hash", "filename", "project_name", 
                        "chapter_title", "subsection_title", "content","date","project_code"]
        )

        candidates = []
        for hits in raw_results:
            for hit in hits:
                candidates.append({
                    "id": hit.id,
                    "file_hash": hit.entity.get("file_hash"),
                    "filename": hit.entity.get("filename"),
                    "project_name": hit.entity.get("project_name"),
                    "project_code": hit.entity.get("project_code"),  # 添加项目编号
                    "chapter_title": hit.entity.get("chapter_title"),
                    "subsection_title": hit.entity.get("subsection_title"),
                    "content": hit.entity.get("content"),
                    "date": hit.entity.get("date"),  # 添加日期字段
                    "distance": hit.distance  # 余弦距离（越小越相似）
                })
        
        # 处理重排序（新增逻辑）
        if rerank and len(candidates) > top_k:
            try:
                from sentence_transformers import CrossEncoder
                
                # 初始化重排序模型（如果未初始化）
                if not hasattr(self, 'reranker_model') or self.reranker_model is None:
                    self.reranker_model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")  # 通用重排序模型
                
                # 准备重排序输入（查询+文档内容）
                pairs = [(query_text, doc['content']) for doc in candidates]
                
                # 计算重排序分数（分数越高越相关）
                rerank_scores = self.reranker_model.predict(pairs)
                
                # 为候选结果添加重排序分数
                for doc, score in zip(candidates, rerank_scores):
                    doc['rerank_score'] = float(score)
                
                # 按重排序分数降序排列
                candidates.sort(key=lambda x: x['rerank_score'], reverse=True)
            
            except ImportError:
                logger.warning("未安装sentence_transformers，无法重排序，使用原始结果")
            except Exception as e:
                logger.error(f"重排序失败: {str(e)}，使用原始结果")
        
        # 应用关键词过滤
        if keyword_filter:
            keyword = keyword_filter.lower()
            candidates = [
                r for r in candidates
                if (keyword in r['content'].lower() or
                    keyword in r.get('chapter_title', '').lower() or
                    keyword in r.get('subsection_title', '').lower())
            ]
        
        # 截取最终结果
        final_results = candidates[:top_k]
        
        # 使用LLM生成统一答案
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
        
    def _generate_unified_answer(self, query: str, results: List[Dict]) -> str:
        """
        基于多个检索结果，使用LLM生成统一的回答
        
        参数:
            query: 用户查询
            results: 检索结果列表
        """
        # 构建LLM提示
        context = "\n\n".join([
            f"来源文档: {res['filename']} | 项目: {res['project_name']} | 章节: {res['chapter_title']} - {res['subsection_title']}\n"
            f"内容: {res['content']}"  # 移除内容长度限制
            for res in results  # 移除结果数量限制
        ])
        
        prompt = f"""
        你是一个专业的技术文档助手，请基于以下上下文回答问题：
        
        用户问题: {query}
        
        上下文信息:
        {context}
        
        请提供一个整合所有相关信息的专业回答，重点突出技术细节和关键点。
        如果上下文不足，指出需要哪些额外信息。
        """
        
        try:
            return self.llm.generate(prompt)
        except Exception as e:
            logger.error(f"LLM生成答案失败: {str(e)}")
            return "抱歉，无法生成答案。请尝试调整查询内容。"        
    
    def calculate_statistics(self, feature: str) -> dict:
        """
        计算当前项目集合中具有特定特征的项目占比
        
        参数:
            feature: 要统计的特征
            
        返回:
            {
                "total_projects": 总项目数,
                "featured_projects": 具有特征的项目数,
                "percentage": 占比百分比
            }
        """
        # 获取当前项目集合
        project_set = self.get_current_project_set()
        
        # 统计总项目数
        total_count = len(project_set)
        
        if total_count == 0:
            return {
                "total_projects": 0,
                "featured_projects": 0,
                "percentage": 0.0
            }
        
        # 搜索具有特征的项目
        feature_query = f"具有特征'{feature}'的项目"
        results = self.execute_search(feature_query, top_k=1000, rerank=False)
        
        # 提取唯一项目
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

        # 新增跨项目统计方法
    def get_project_summary(self) -> Dict[str, Dict]:
        """获取所有项目的统计信息"""
        # 从集合中获取所有唯一项目名
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
                # 获取项目编号
                project_code = metadata.get("project_code", "未知编号")                
                project_stats[project] = {
                    "document_count": count,
                    "last_updated": metadata.get("last_updated", "unknown"),
                    "file_count": metadata.get("file_count", 0),
                    "files": metadata.get("files", []),
                    "project_code": project_code  # 添加项目编号
                }
            return project_stats
        except Exception as e:
            logger.error(f"Failed to get project summary: {e}")
            return {}
    
    # 新增跨项目搜索方法
    def cross_project_search(self, query_text: str, top_k_per_project: int = 3, 
                            rerank: bool = True) -> Dict[str, List[Dict]]:
        """
        跨项目搜索，返回每个项目的top结果
        
        参数:
            query_text: 查询文本
            top_k_per_project: 每个项目返回的结果数
            rerank: 是否使用重排序
            
        返回:
            字典: {project_name: [result1, result2, ...]}
        """
        # 获取所有项目
        all_projects = list(self.project_metadata.keys())
        
        if not all_projects:
            # 如果没有项目元数据，从集合中获取
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
                # 添加项目编号到结果
                for result in results.get("results", []):
                    result["project_code"] = self.get_project_metadata(project).get("project_code", "")                
                results_by_project[project] = results
            except Exception as e:
                logger.error(f"Project {project} search failed: {str(e)}")
                results_by_project[project] = []
        
        return results_by_project
    
    def get_all_projects(self) -> List[str]:
        """获取所有项目名称列表（增强安全性和错误处理）"""
        try:
            # 优先使用项目元数据中的键
            if self.project_metadata:
                return list(self.project_metadata.keys())
            
            # 如果元数据为空，从集合中获取
            if not self.collection:
                logger.warning("集合未初始化，无法获取项目列表")
                return []
                
            # 执行查询获取所有项目名称
            res = self.collection.query(
                expr="",  # 空表达式表示所有文档
                output_fields=["project_name"],
                limit=10000  # 假设项目不超过10000个
            )
            
            # 提取并去重项目名称
            project_names = set()
            for item in res:
                project_name = item.get("project_name")
                if project_name:
                    project_names.add(project_name)
                    
            return list(project_names)
            
        except Exception as e:
            logger.error(f"获取所有项目失败: {str(e)}", exc_info=True)
            return []  # 返回空列表保证前端不崩溃

    def get_project_description(self, project_name: str) -> str:
        """
        获取项目描述/特点
        1. 首先尝试从元数据获取
        2. 如果没有，则使用LLM生成项目摘要
        """
        # 检查元数据中是否有描述（优先使用缓存）
        if project_name in self.project_metadata and "description" in self.project_metadata[project_name]:
            return self.project_metadata[project_name]["description"]
        
        # 使用LLM生成项目描述（处理搜索结果结构和异常）
        try:
            # 1. 获取项目中的代表性文档（使用统一搜索接口）
            search_result = self.execute_search(
                query_text="项目概述",
                top_k=3,
                rerank=True,
                project_names=[project_name],  # 限制只搜索当前项目
                use_llm=False,  # 不需要LLM处理每个结果
                # 不请求不存在的字段（避免date字段错误）
                output_fields=["content", "filename", "project_name"]
            )
            
            # 2. 从搜索结果中提取文档列表（关键修复：正确解析execute_search的返回结构）
            doc_results = search_result.get("results", [])  # execute_search返回字典，结果在"results"键中
            if not doc_results:
                return "该项目暂无可用描述信息"
            
            # 3. 构建LLM提示（确保只使用存在的字段）
            context_snippets = []
            for idx, doc in enumerate(doc_results[:3]):  # 最多取3条结果
                # 避免依赖不存在的字段（如date），只使用content
                content = doc.get("content", "")[:500]  # 限制单条内容长度，避免提示词过长
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
            
            # 4. 调用LLM生成描述，并缓存结果到元数据
            llm_description = self.generate_answer(PROMPT)
            
            # 缓存生成的描述到元数据，避免重复生成
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
        """
        查找具有特定特点的项目
        参数:
            feature: 要查找的特点（如"高层建筑"）
            top_k: 返回的最大项目数量
        """
        # 使用跨项目搜索查找相关项目
        project_results = self.cross_project_search(
            query_text=feature,
            top_k_per_project=3,  # 每个项目取3个结果
            rerank=True
        )
        
        # 对项目进行评分排序
        scored_projects = []
        for project, results in project_results.items():
            if not results:
                continue
                
            # 计算项目相关性分数（取最高分结果）
            best_score = max(
                result.get('rerank_score', 1 - result['distance']) 
                for result in results
            )
            scored_projects.append((project, best_score))
        
        # 按分数排序并取top_k
        scored_projects.sort(key=lambda x: x[1], reverse=True)
        return [{"project": p, "score": s} for p, s in scored_projects[:top_k]]
    
    def _llm_parse_time_range(self, question: str) -> Tuple[Optional[str], Optional[str]]:
        """
        使用大语言模型解析时间范围，返回标准化日期格式
        
        参数:
            question: 包含时间信息的用户问题
            
        返回:
            tuple: (start_date, end_date) 格式为 'YYYY-MM-DD'，解析失败时返回 (None, None)
        """
        def _normalize_date(date_str: Optional[str]) -> Optional[str]:
            """内部函数：标准化日期格式为YYYY-MM-DD"""
            if not date_str:
                return None
                
            # 常见中文日期格式处理
            date_str = date_str.replace("年", "-").replace("月", "-").replace("日", "").replace("/", "-")
            
            # 尝试多种日期格式解析
            formats = [
                "%Y-%m-%d",    # 标准格式
                "%Y-%m",       # 只有年月
                "%Y",          # 只有年份
                "%m-%d-%Y",    # 美式格式
                "%d-%m-%Y"     # 欧式格式
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    # 根据格式补全日期
                    if fmt == "%Y-%m":
                        return dt.strftime("%Y-%m-01")
                    elif fmt == "%Y":
                        return dt.strftime("%Y-01-01")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None

        # 1. 设计LLM提示模板（严格约束输出格式）
        prompt = f"""
        ## 时间解析任务说明
        请从以下用户问题中提取时间范围，并严格按JSON格式返回结果:
        
        ### 输入问题:
        {question}
        
        ### 输出要求:
        1. 必须包含start_date和end_date字段
        2. 日期格式优先级:
        - 完整日期: YYYY-MM-DD (如"2023-05-15")
        - 年月: YYYY-MM-01 (如"2023-05-01")
        - 年份: YYYY-01-01 (如"2023-01-01")
        3. 特殊处理:
        - "近三年" → 当前年份减3到当前年份
        - "2023年以后" → start_date="2023-01-01", end_date="2100-12-31"
        - "2020年之前" → start_date="1900-01-01", end_date="2020-12-31"
        - "疫情期间" → start_date="2020-01-01", end_date="2023-12-31"
        
        ### 示例输出:
        {{
            "start_date": "2023-01-01",
            "end_date": "2023-12-31",
            "comment": "从'2023年'解析得到"
        }}
        
        请直接返回JSON对象，不要包含任何额外解释！
        """

        try:
            # 2. 调用LLM获取响应
            llm_response = self.llm.generate(prompt)
            logger.debug(f"LLM原始响应: {llm_response}")

            # 3. 提取JSON部分（处理可能的非规范响应）
            json_str = llm_response[llm_response.find('{'):llm_response.rfind('}')+1]
            time_info = json.loads(json_str)

            # 4. 验证并标准化日期
            start_date = _normalize_date(time_info.get("start_date"))
            end_date = _normalize_date(time_info.get("end_date"))

            # 5. 逻辑校验
            if start_date and end_date:
                if start_date > end_date:
                    logger.warning(f"时间范围无效: {start_date} > {end_date}")
                    return (None, None)

            # 6. 默认值处理
            if not start_date and not end_date:
                logger.info("未解析到有效时间范围")
            elif not start_date:
                start_date = "1900-01-01"  # 默认最小值
            elif not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")  # 默认当前日期

            return (start_date, end_date)

        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}\n响应内容: {llm_response[:200]}...")
            return (None, None)
        except Exception as e:
            logger.error(f"时间解析异常: {str(e)}")
            return (None, None)

    def _hybrid_parse_time_range(self, question: str) -> tuple:
        """
        混合解析策略：先尝试正则快速解析，失败则调用LLM
        
        返回:
            (start_date, end_date) 或 (None, None)
        """
        # 1. 先用正则解析简单情况（效率高）
        start_date, end_date = self._parse_time_range(question)
        if start_date and end_date:
            # 确保返回标准格式
            return (f"{start_date}-01-01", f"{end_date}-12-31")
        
        # 2. 复杂情况调用LLM
        logger.info(f"正则解析失败，使用LLM解析时间范围: {question}")
        return self._llm_parse_time_range(question)    

    # 简化的answer_question方法，仅处理任务类型识别
    def answer_question(self, question: str) -> Dict:
        """
        识别问题类型并调用相应处理逻辑
        """
        # 提取约束条件
        project_names = self._extract_project_names(question)
        time_range = self._hybrid_parse_time_range(question)
        feature = self._extract_feature(question)
        
        # 执行搜索
        return self.execute_search(
            query_text=question,
            top_k=10,
            use_llm=True,
            project_names=project_names,
            time_range=time_range,
            feature_filter=feature
        )
    
    def _extract_project_names(self, question: str) -> List[str]:
        """从问题中提取项目名称"""
        # 简单实现，实际应使用更复杂的命名实体识别
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
        """获取当前上下文状态信息（仅显示非空约束）"""
        project_set = self.get_current_project_set()
        
        # 格式化约束条件（仅显示实际添加的约束）
        formatted_constraints = []
        for constraint in self.context_manager["constraints"]:
            constraint_type = constraint["type"]
            value = constraint["value"]
            
            # 处理时间范围显示
            if constraint_type == "time_range":
                start_year, end_year = value
                if start_year and end_year:
                    formatted_value = f"{start_year}-{end_year}年"
                elif start_year:
                    formatted_value = f"{start_year}年以后"
                elif end_year:
                    formatted_value = f"{end_year}年以前"
                else:
                    continue  # 跳过空时间约束
            else:
                formatted_value = value
            
            formatted_constraints.append({
                "type": constraint_type,
                "value": formatted_value,
                "timestamp": constraint["timestamp"]
            })
        
        # 仅在有约束时显示，空约束时前端显示"无筛选条件"
        return {
            "project_count": len(project_set),
            "constraints": formatted_constraints,  # 空列表时前端不显示
            "session_id": self.context_manager.get("session_id", str(uuid.uuid4()))
        }


