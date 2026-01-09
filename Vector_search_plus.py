from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
import logging
#import pythoncom
#import win32com.client as win32
from docx import Document  # 新增：用于处理Word文档的跨平台库
from datetime import datetime
import requests
import json
import time
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed, retry_if_exception_type
import ssl
import httpx 
from langchain_community.embeddings import ModelScopeEmbeddings
from typing import Set, List, Dict, Optional, Tuple, Any
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
        #'model_id': 'deepseek-ai/DeepSeek-R1-Distill-Qwen-7B',
        'model_id': 'Qwen/Qwen3-Next-80B-A3B-Instruct',
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
    def __init__(self, collection_name="text_searchLC", max_retries=5):
        self.collection_name = collection_name
        self.dim = 1024
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

        # 项目元数据持久化
        self.metadata_file = "project_metadata.json"
        self.project_metadata = self._load_persistent_metadata()

        # ModelScope 组件初始化
        self.embeddings = ModelScopeEmbeddings(
            model_id=MODEL_CONFIG['embedding']['model_id']
        )
        self.llm = ModelScopeLLM(
            api_base=MODEL_CONFIG['llm']['api_base'],
            api_key=MODEL_CONFIG['llm']['api_key'],
            model_id=MODEL_CONFIG['llm']['model_id']
        )

        # 上下文状态管理
        self.context_manager = {
            "current_project_set": None,
            "constraints": [],
            "session_id": str(uuid.uuid4())
        }

        # 初始化集合
        self._initialize()

    def _load_persistent_metadata(self):
        """从文件加载持久化的项目元数据"""
        try:
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    logger.info(f"从文件加载了 {len(metadata)} 个项目元数据")
                    return metadata
        except Exception as e:
            logger.error(f"加载持久化元数据失败: {e}")
        return {}

    def _save_persistent_metadata(self):
        """保存项目元数据到文件"""
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.project_metadata, f, ensure_ascii=False, indent=2)
            logger.debug("项目元数据已保存到文件")
        except Exception as e:
            logger.error(f"保存持久化元数据失败: {e}")

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
                    wait_time = min(2 ** attempt, 30)
                    logger.warning(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.critical("所有初始化尝试均失败")
                    raise RuntimeError("系统无法初始化") from e
                
    def _recover_metadata_from_milvus(self):
        """从Milvus恢复项目元数据"""
        try:
            if not self.collection:
                logger.warning("集合未初始化，无法恢复元数据")
                return
            
            # 获取所有不重复的项目
            all_projects = self._get_distinct_projects_from_milvus()
            logger.info(f"从Milvus发现 {len(all_projects)} 个项目")
            
            # 为每个项目构建元数据
            for project_name in all_projects:
                if project_name not in self.project_metadata:
                    # 从Milvus获取项目基本信息
                    project_info = self._get_project_info_from_milvus(project_name)
                    
                    # 创建新的元数据记录
                    self.project_metadata[project_name] = {
                        "project_code": project_info.get("project_code", "未知编号"),
                        "file_count": project_info.get("file_count", 0),
                        "files": project_info.get("files", []),
                        "start_date": project_info.get("start_date", "未知日期"),
                        "last_updated": project_info.get("last_updated", datetime.now().isoformat()),
                        "description": project_info.get("description", "")
                    }
            
            # 保存恢复的元数据
            self._save_persistent_metadata()
            logger.info(f"从Milvus成功恢复 {len(all_projects)} 个项目元数据")
            
        except Exception as e:
            logger.error(f"从Milvus恢复元数据失败: {str(e)}")

    def _get_distinct_projects_from_milvus(self):
        """从Milvus获取所有不重复的项目名称"""
        try:
            results = self.collection.query(
                expr="",
                output_fields=["project_name"],
                limit=10000
            )
            
            project_names = set()
            for item in results:
                project_name = item.get("project_name")
                if project_name:
                    project_names.add(project_name)
            
            return list(project_names)
            
        except Exception as e:
            logger.error(f"获取项目列表失败: {e}")
            return []
        
    def _get_project_info_from_milvus(self, project_name):
        """从Milvus获取项目详细信息"""
        try:
            # 获取项目文档数量
            expr = f'project_name == "{project_name}"'
            doc_count = self.collection.query(expr=expr, count_only=True)
            
            # 获取项目文件列表
            files_expr = f'project_name == "{project_name}"'
            file_results = self.collection.query(
                expr=files_expr,
                output_fields=["filename"],
                limit=1000
            )
            
            files = list(set([item.get("filename", "") for item in file_results if item.get("filename")]))
            
            # 获取项目编号和日期
            info_expr = f'project_name == "{project_name}"'
            info_results = self.collection.query(
                expr=info_expr,
                output_fields=["project_code", "date"],
                limit=1
            )
            
            project_code = "未知编号"
            start_date = "未知日期"
            if info_results:
                project_code = info_results[0].get("project_code", "未知编号")
                start_date = info_results[0].get("date", "未知日期")
            
            return {
                "project_code": project_code,
                "file_count": len(files),
                "files": files,
                "start_date": start_date,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"获取项目 {project_name} 信息失败: {e}")
            return {}
        
    def get_all_projects_with_stats(self):
        """直接从 Milvus 查询所有项目信息"""
        try:
            # 查询所有向量，输出需要字段
            results = self.collection.query(
                expr="",  # 查询全部
                output_fields=["project_name", "project_code", "date", "metadata"],
                limit=10000  # 根据项目数量调整
            )

            projects = {}
            for item in results:
                name = item.get("project_name")
                if not name:
                    continue
                # metadata 里可以存文件数量、文档数量等
                meta = item.get("metadata") or {}
                projects[name] = {
                    "project_code": item.get("project_code", "无"),
                    "document_count": meta.get("document_count", 0),
                    "file_count": meta.get("file_count", 0),
                    "start_date": item.get("date", "未知"),
                    "last_updated": item.get("date", "未知")
                }

            return projects

        except Exception as e:
            logger.error(f"获取项目列表失败: {e}")
            return {}

    def _get_project_vector_count(self, project_name: str) -> int:
        """
        统计某个项目在 Milvus 中的向量条目数量。
        """
        try:
            expr = f'project == "{project_name}"'
            res = self.collection.query(expr=expr, output_fields=["project"])
            return len(res)
        except Exception as e:
            print(f"[ERROR] Failed to count vectors for project {project_name}: {e}")
            return 0
        
    def _get_project_doc_count(self, project_name):
        """获取项目文档数量"""
        try:
            expr = f'project_name == "{project_name}"'
            return self.collection.query(expr=expr, count_only=True)
        except Exception as e:
            logger.error(f"获取项目 {project_name} 文档数量失败: {e}")
            return 0
        
    def add_project_metadata(self, project_name: str, metadata: dict):
        """添加或更新项目元数据（自动保存到文件）"""
        # 确保包含必要字段
        if "project_code" not in metadata:
            metadata["project_code"] = "NO_CODE"
            
        if "start_date" not in metadata or not metadata["start_date"]:
            metadata["start_date"] = datetime.now().strftime("%Y-%m-%d")
        
        # 更新元数据
        if project_name not in self.project_metadata:
            self.project_metadata[project_name] = {}
        
        self.project_metadata[project_name].update(metadata)
        
        # 自动保存到文件
        self._save_persistent_metadata()
        
        logger.info(f"项目 '{project_name}' 元数据已更新")

    def get_project_metadata(self, project_name: str) -> Optional[dict]:
        """获取项目元数据（带fallback机制）"""
        # 优先从内存元数据获取
        meta = self.project_metadata.get(project_name, {})
        
        # 如果内存中没有，尝试从Milvus恢复
        if not meta and self.collection:
            try:
                project_info = self._get_project_info_from_milvus(project_name)
                if project_info:
                    self.project_metadata[project_name] = project_info
                    self._save_persistent_metadata()
                    return project_info
            except Exception as e:
                logger.error(f"从Milvus恢复项目 {project_name} 元数据失败: {e}")
        
        return meta
    
    def delete_projects(self, project_names: List[str]) -> Dict[str, Any]:
        if not project_names:
            return {"success": False, "message": "未提供项目名称"}

        try:
            expr = f"project_name in {json.dumps(project_names)}"

            count_result = self.collection.query(expr=expr, count_only=True)

            delete_result = self.collection.delete(expr)

            # 删除元数据
            for project in project_names:
                if project in self.project_metadata:
                    del self.project_metadata[project]

            self._save_persistent_metadata()

            # 清空上下文缓存
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

    def update_context(self, constraint_type: str, value: any):
        if self.context_manager is None:
            self.context_manager = {
                "current_project_set": None,
                "constraints": [],
                "session_id": str(uuid.uuid4())
            }

        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return

        # 时间范围解析
        if constraint_type == "time_range":
            parsed_value = None
            value_str = value.strip() if isinstance(value, str) else str(value)

            range_match = re.match(r"^(\d{4})\s*-\s*(\d{4})\s*年$", value_str)
            if range_match:
                y1, y2 = int(range_match.group(1)), int(range_match.group(2))
                if y1 <= y2:
                    parsed_value = (y1, y2)
                else:
                    return

            after_match = re.match(r"^(\d{4})\s*年\s*以后$", value_str)
            if after_match and not parsed_value:
                parsed_value = (int(after_match.group(1)), None)

            before_match = re.match(r"^(\d{4})\s*年\s*以前$", value_str)
            if before_match and not parsed_value:
                parsed_value = (None, int(before_match.group(1)))

            if not parsed_value:
                return

            value = parsed_value

        # 去重
        for ex in self.context_manager["constraints"]:
            if ex["type"] == constraint_type and ex["value"] == value:
                return

        # 避免全集约束
        if constraint_type == "project_name":
            all_projects = set(self.get_all_projects())
            target = set(value if isinstance(value, list) else [value])
            if target == all_projects:
                return

        self.context_manager["constraints"].append({
            "type": constraint_type,
            "value": value,
            "timestamp": datetime.now().isoformat()
        })

        self.context_manager["current_project_set"] = None

    # 2. 修复remove_constraint方法（增加安全校验）
    def remove_constraint(self, index: int):
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
                constraints.pop(index)
                self.context_manager["current_project_set"] = None
            else:
                raise IndexError(f"无效的约束索引: {index}")

            return self._get_context_info()

        except Exception as e:
            logger.error(f"移除约束失败: {str(e)}")
            raise   

    def clear_context(self):
        self.context_manager = {
            "current_project_set": None,
            "constraints": [],
            "session_id": str(uuid.uuid4())
        }
        logger.info("Context cleared")

    # 增强get_current_project_set方法，处理所有约束条件
    def get_current_project_set(self) -> Set[str]:
        try:
            # 使用缓存
            if self.context_manager["current_project_set"] is not None:
                return self.context_manager["current_project_set"] or set()

            # 获取所有项目初始集合
            all_projects = self._get_distinct_projects_from_milvus()
            current_set = set(all_projects) if all_projects else set()

            # ---------------------------
            # 1️⃣ 先处理 project_name / project_code 并集
            # ---------------------------
            project_union = set()
            for c in self.context_manager["constraints"]:
                if c["type"] == "project_name":
                    target_list = c["value"] if isinstance(c["value"], list) else [c["value"]]
                    matched = set()

                    # 构建 project -> code 映射
                    mapping = {}
                    reverse = {}
                    for p in current_set:
                        meta = self.get_project_metadata(p)
                        if meta and "project_code" in meta:
                            mapping[p] = meta["project_code"]
                            reverse[meta["project_code"]] = p

                    for tname in target_list:
                        # 完全匹配 name
                        if tname in current_set:
                            matched.add(tname)
                        # 完全匹配 code
                        elif tname in reverse:
                            matched.add(reverse[tname])
                        else:
                            # 包含匹配 name 或 code
                            for p in current_set:
                                code = mapping.get(p, "")
                                if tname in p or tname in code:
                                    matched.add(p)

                    # 并集累加
                    project_union.update(matched)

            # 如果有 project_name / project_code 筛选，使用并集结果
            if project_union:
                current_set = project_union

            # ---------------------------
            # 2️⃣ 处理时间范围筛选（交集）
            # ---------------------------
            for c in self.context_manager["constraints"]:
                if c["type"] == "time_range":
                    start_year, end_year = c["value"]
                    filtered = set()

                    project_dates = self._get_project_dates_from_milvus(current_set)

                    for p in current_set:
                        d = project_dates.get(p)
                        if not d:
                            continue
                        try:
                            py = int(d.split("-")[0])
                        except:
                            continue

                        in_range = True
                        if start_year is not None and py < start_year:
                            in_range = False
                        if end_year is not None and py > end_year:
                            in_range = False

                        if in_range:
                            filtered.add(p)

                    current_set = filtered

            # ---------------------------
            # 3️⃣ 处理特征语义筛选（交集）
            # ---------------------------
            for c in self.context_manager["constraints"]:
                if c["type"] == "feature":
                    try:
                        vec = self.text_to_vector(c["value"])
                        params = {"metric_type": "COSINE", "params": {"ef": 50}}

                        res = self.collection.search(
                            data=[vec],
                            anns_field="embedding",
                            param=params,
                            limit=1000,
                            output_fields=["project_name"]
                        )

                        matched = set()
                        for hits in res:
                            for hit in hits:
                                pn = hit.entity.get("project_name")
                                if pn:
                                    matched.add(pn)

                        current_set = current_set.intersection(matched)

                    except:
                        pass

            # 缓存结果
            self.context_manager["current_project_set"] = current_set or set()
            return current_set or set()

        except Exception as e:
            logger.error(f"获取当前项目集合失败: {str(e)}", exc_info=True)
            return set()
        
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

    def _connect_milvus(self):
        """连接Milvus数据库"""
        try:
            import os
            from pymilvus import connections

            milvus_host = os.getenv("MILVUS_HOST", "localhost")
            milvus_port = os.getenv("MILVUS_PORT", "19530")

            logger.info(f"正在连接 Milvus: {milvus_host}:{milvus_port}")

            connections.connect(
                alias="default",
                host=milvus_host,
                port=milvus_port
            )

            logger.info("Milvus 连接成功")
            self._create_collection_if_not_exists()

            if self.collection is None:
                raise RuntimeError("Milvus 已连接，但 collection 未初始化")

        except ImportError:
            logger.warning("pymilvus 未安装，跳过数据库连接")
        except Exception as e:
            logger.error(f"Milvus 连接失败: {e}")
            raise   # ⚠️ 这里建议直接抛出，别继续跑
    
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


    #def read_docx_with_win32com(self, filepath: str) -> List[Dict]:
    def read_docx_with_python_docx(self, filepath: str) -> List[Dict]:
        """使用 python-docx 解析 Word 文档（生产级跨平台方案）"""
        sections = []

        try:
            doc = Document(filepath)

            for para in doc.paragraphs:
                raw_text = para.text.strip()
                if not raw_text:
                    continue

                style_name = para.style.name if para.style else ""

                list_value = ""
                list_level = 0

                # === 1. 手动编号（最高优先级）===
                manual_number_match = re.match(
                    r'^(\d+[、.)]|[(（][一二三四五六七八九十零\d]+[)）]|第?[一二三四五六七八九十零\d]+[章节条项])',
                    raw_text
                )

                if manual_number_match:
                    list_value = manual_number_match.group(1)
                    final_text = raw_text

                else:
                    # === 2. Word 自动列表（仅用于层级，不强求编号）===
                    if para._p.pPr is not None and para._p.pPr.numPr is not None:
                        list_level = 1
                        try:
                            indent = para.paragraph_format.left_indent
                            if indent is not None:
                                list_level = max(1, int(indent.pt / 36) + 1)
                        except:
                            pass

                    # === 3. 根据层级做缩进 ===
                    if list_level > 0:
                        final_text = "\t" * (list_level - 1) + raw_text
                    else:
                        final_text = raw_text

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

            return sections

        except Exception as e:
            print(f"文档解析失败：{e}")
            raise

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

    def insert_documents(
        self,
        file_dir: str,
        project_name: str = None,
        project_date: str = None,
        project_code: str = None
    ):
        """跨平台版本的数据插入方法（不依赖 win32 / pythoncom）"""

        success_count = 0
        project_files = {}

        # === 1. 项目名推断 ===
        if project_name is None:
            project_name = os.path.basename(os.path.normpath(file_dir))
            logger.info(f"Auto-detected project name: {project_name}")
        else:
            project_name = project_name[:256]

        if not project_name or not isinstance(project_name, str):
            raise ValueError("Invalid project name")

        # === 2. 项目日期处理 ===
        if not project_date:
            project_date = datetime.now().strftime("%Y-%m-%d")
        elif len(project_date) == 4:
            project_date = f"{project_date}-01-01"

        # === 3. 项目编号处理 ===
        if not project_code:
            project_code = "NO_CODE"
            logger.warning(f"未提供项目编号，使用默认值: {project_code}")

        # === 4. 全局内容哈希去重（项目级）===
        processed_hashes = set()

        for filename in os.listdir(file_dir):
            if not filename.lower().endswith(".docx"):
                continue

            filepath = os.path.join(file_dir, filename)

            try:
                # ★★ 关键修改点：使用 python-docx 解析 ★★
                sections = self.read_docx_with_python_docx(filepath)

                chapters = self.parse_document_structure(sections)

                log_path = self.log_subsections_to_file(filename, chapters)
                logger.info(f"文档解析完成：{filename}，日志路径：{log_path}")

                project_files.setdefault(project_name, []).append(filename)

                for chap_idx, chapter in enumerate(chapters, 1):
                    for sub_idx, subsection in enumerate(chapter.get("subsections", []), 1):

                        # === 合并一级 + 二级子章节内容 ===
                        all_content = list(subsection.get("content", []))
                        for child in subsection.get("subsections", []):
                            all_content.extend(child.get("content", []))

                        subsection_content = "\n".join(all_content).strip()
                        if not subsection_content:
                            continue

                        # === 项目级内容去重 ===
                        content_hash = hashlib.sha256(subsection_content.encode("utf-8")).hexdigest()
                        if content_hash in processed_hashes:
                            logger.debug(f"跳过重复内容: {filename} {chap_idx}.{sub_idx}")
                            continue
                        processed_hashes.add(content_hash)

                        try:
                            true_subsection_title = subsection.get("title") or f"子章节{sub_idx}"

                            if true_subsection_title == chapter.get("title"):
                                true_subsection_title = f"{chapter['title']} - 子章节{sub_idx}"

                            sub_hash = self._calculate_segment_hash(
                                subsection_content, filename, chap_idx, sub_idx
                            )

                            metadata = {
                                "section_type": "subsection",
                                "original_title": subsection.get("title"),
                                "parent_chapter": chapter.get("title"),
                                "is_merged": subsection.get("is_merged", False),
                                "level": subsection.get("level", 1),
                                "log_path": log_path,
                                "start_date": project_date
                            }

                            if subsection.get("subsections"):
                                metadata["child_subsections"] = [
                                    {
                                        "title": c.get("title"),
                                        "level": c.get("level", 2)
                                    }
                                    for c in subsection["subsections"]
                                ]

                            doc = {
                                "embedding": self.text_to_vector(subsection_content),
                                "file_hash": sub_hash,
                                "filename": self._process_field("filename", filename),
                                "project_name": project_name,
                                "chapter_title": self._process_field(
                                    "chapter_title", chapter.get("title")
                                ),
                                "subsection_title": self._process_field(
                                    "subsection_title", true_subsection_title
                                ),
                                "content": subsection_content[:65535],
                                "project_code": project_code,
                                "date": project_date,
                                "metadata": metadata
                            }

                            self.collection.insert([doc])
                            success_count += 1

                        except Exception as e:
                            logger.error(
                                f"子章节插入失败: {filename} {chap_idx}.{sub_idx} - {str(e)}",
                                exc_info=True
                            )

            except Exception as e:
                logger.error(f"文件处理错误（跳过）: {filename} - {str(e)}", exc_info=True)

        # === 5. 项目元数据写入 ===
        if project_files:
            for project, files in project_files.items():
                self.add_project_metadata(project, {
                    "file_count": len(files),
                    "files": files,
                    "last_updated": datetime.now().isoformat(),
                    "project_code": project_code,
                    "start_date": project_date
                })
                logger.info(f"项目 '{project}' 元数据已更新")

        self.collection.load()
        logger.info(f"项目 '{project_name}' 处理完成: 成功插入 {success_count} 个片段")

        return success_count
    
    def execute_search(self, query_text: str, top_k: int = 10,
                    use_llm: bool = True, keyword_filter: str = None,
                    project_names: Optional[List[str]] = None,
                    time_range: Optional[Tuple[str, str]] = None,
                    feature_filter: str = None,
                    rerank: bool = False) -> Dict:

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

        query_vec = self.text_to_vector(query_text)
        params = {"metric_type": "COSINE", "params": {"ef": 50}}
        candidate_k = top_k * 3 if rerank else top_k

        raw = self.collection.search(
            data=[query_vec],
            anns_field="embedding",
            param=params,
            limit=candidate_k,
            expr=f"project_name in {json.dumps(list(project_set))}",
            output_fields=[
                "id", "file_hash", "filename", "project_name",
                "chapter_title", "subsection_title", "content",
                "date", "project_code"
            ]
        )

        candidates = []
        for hits in raw:
            for h in hits:
                candidates.append({
                    "id": h.id,
                    "file_hash": h.entity.get("file_hash"),
                    "filename": h.entity.get("filename"),
                    "project_name": h.entity.get("project_name"),
                    "project_code": h.entity.get("project_code"),
                    "chapter_title": h.entity.get("chapter_title"),
                    "subsection_title": h.entity.get("subsection_title"),
                    "content": h.entity.get("content"),
                    "date": h.entity.get("date"),
                    "distance": h.distance
                })

        # ======= 重排序 =======
        if rerank and len(candidates) > top_k:
            try:
                from sentence_transformers import CrossEncoder

                if self.reranker_model is None:
                    self.reranker_model = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

                pairs = [(query_text, c["content"]) for c in candidates]
                scores = self.reranker_model.predict(pairs)

                for c, s in zip(candidates, scores):
                    c["rerank_score"] = float(s)

                candidates.sort(key=lambda x: x["rerank_score"], reverse=True)

            except Exception as e:
                logger.error(f"重排序失败: {str(e)}")

        # 关键词过滤
        if keyword_filter:
            kw = keyword_filter.lower()
            candidates = [
                r for r in candidates
                if kw in r["content"].lower()
                or kw in (r.get("chapter_title") or "").lower()
                or kw in (r.get("subsection_title") or "").lower()
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
    
    def get_all_projects(self):
        """Always fetch distinct project names from Milvus."""
        return self._get_distinct_projects_from_milvus()


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
        project_set = self.get_current_project_set()

        formatted = []
        for c in self.context_manager["constraints"]:
            t = c["type"]
            v = c["value"]

            if t == "time_range":
                a, b = v
                if a and b:
                    text = f"{a}-{b}年"
                elif a and not b:
                    text = f"{a}年以后"
                elif b and not a:
                    text = f"{b}年以前"
                else:
                    text = "时间范围未知"
            else:
                text = v

            formatted.append({
                "type": t,
                "value": text,
                "timestamp": c["timestamp"]
            })

        return {
            "projects": list(project_set),
            "constraints": formatted
        }
    
    def stream_search(self, query_text, use_llm=True, rerank=False):
        """流式搜索方法，返回生成器"""
        # 1. 获取搜索结果
        results = self.execute_search(
            query_text=query_text,
            use_llm=use_llm,
            rerank=rerank,
            stream=True  # 添加流式标志
        )
        
        # 2. 流式生成响应
        for chunk in self._generate_stream_chunks(results):
            yield f"data: {json.dumps({'text': chunk})}\n\n"
    
    def _generate_stream_chunks(self, results):
        """将搜索结果分块生成"""
        # 这里需要根据你的实际实现来分块
        # 示例：将AI回答分成单词流式输出
        if 'answer' in results:
            answer = results['answer']
            words = answer.split()
            for word in words:
                yield word + " "
                time.sleep(0.05)  # 模拟流式延迟
        else:
            yield "抱歉，未能生成回答"


