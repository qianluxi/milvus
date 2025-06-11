from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
import logging
from typing import List, Dict, Optional
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
        
    def generate(self, prompt, **kwargs):
        response = self.client.chat.completions.create(
            model=self.model_id,
            messages=[{"role": "user", "content": prompt}],
            stream=False,
            max_tokens=kwargs.get("max_tokens", 512),
            temperature=kwargs.get("temperature", 0.2)
        )
        return response.choices[0].message.content

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
        
        # 初始化ModelScope组件
        self.embeddings = ModelScopeEmbeddings(
            model_id=MODEL_CONFIG['embedding']['model_id']
        )
        self.llm = ModelScopeLLM(
            api_base=MODEL_CONFIG['llm']['api_base'],
            api_key=MODEL_CONFIG['llm']['api_key'],
            model_id=MODEL_CONFIG['llm']['model_id']
        )
        
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
                FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=300),
                FieldSchema(name="subsection_title", dtype=DataType.VARCHAR, max_length=500),            
                FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535, nullable=True),
                FieldSchema(name="metadata", dtype=DataType.JSON)
            ]

            # 创建集合模式
            schema = CollectionSchema(fields, "Enhanced text search collection")

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
            logger.info(f"Collection '{self.collection_name}' created and loaded successfully.")
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

    def check_if_file_exists(file_hash: str) -> bool:
        """检查集合中是否已存在相同哈希值的文件"""
        expr = f"file_hash == '{file_hash}'"
        try:
            results = collection.query(expr=expr, output_fields=["id"])
            return len(results) > 0
        except Exception as e:
            logging.warning(f"查询数据库失败（忽略去重）: {e}")
            return False  # 查询失败时不阻止插入            
       
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

    def insert_documents(self, file_dir: str, app=None):
        """重构的数据插入方法（支持多级章节）"""
        success_count = 0
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
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    # 处理章节内容
                    if chapter.get('content'):
                        chapter_content = '\n'.join(chapter['content'])
                        if chapter_content.strip():
                            try:
                                chap_hash = self._calculate_segment_hash(chapter_content, filename, chap_idx, 0)
                                doc = {
                                    "embedding": self.text_to_vector(chapter_content),
                                    "file_hash": chap_hash,
                                    "filename": self._process_field('filename', filename),
                                    "chapter_title": self._process_field('chapter_title', chapter['title']),
                                    "subsection_title": self._process_field('subsection_title', chapter['title']),
                                    "content": chapter_content[:65535],
                                    "metadata": {
                                        "section_type": "chapter",
                                        "original_title": chapter['title'],
                                        "log_path": log_path
                                    }
                                }
                                self.collection.insert([doc])
                                success_count += 1
                            except Exception as e:
                                logger.error(f"章节插入失败: {filename} Chap{chap_idx} - {str(e)}")

                    # 处理一级子章节
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        # 合并所有内容：一级子章节内容 + 其下的二级子章节内容
                        all_content = subsection['content'][:]
                        
                        # 添加二级子章节内容
                        if 'subsections' in subsection:
                            for child_sub in subsection['subsections']:
                                all_content.extend(child_sub['content'])
                        
                        subsection_content = '\n'.join(all_content)
                        
                        try:
                            # 使用一级子章节标题
                            true_subsection_title = subsection['title']
                            
                            sub_hash = self._calculate_segment_hash(subsection_content, filename, chap_idx, sub_idx)
                            
                            # 元数据记录层级信息
                            metadata = {
                                "section_type": "subsection",
                                "original_title": subsection['title'],
                                "parent_chapter": chapter['title'],
                                "is_merged": subsection.get('is_merged', False),
                                "level": subsection.get('level', 1),
                                "log_path": log_path
                            }
                            
                            # 添加二级子章节信息
                            if 'subsections' in subsection:
                                metadata["child_subsections"] = [
                                    {"title": child['title'], "level": child.get('level', 2)} 
                                    for child in subsection['subsections']
                                ]
                            
                            doc = {
                                "embedding": self.text_to_vector(subsection_content),
                                "file_hash": sub_hash,
                                "filename": self._process_field('filename', filename),
                                "chapter_title": self._process_field('chapter_title', chapter['title']),
                                "subsection_title": self._process_field('subsection_title', true_subsection_title),
                                "content": subsection_content[:65535],
                                "metadata": metadata
                            }
                            self.collection.insert([doc])
                            success_count += 1
                        except Exception as e:
                            logger.error(f"子章节插入失败: {filename} {chap_idx}.{sub_idx} - {str(e)}")
                            
            except Exception as e:
                logger.error(f"文件处理错误（跳过）: {filename} - {str(e)}")
            finally:
                pythoncom.CoUninitialize()

        self.collection.load()
        logger.info(f"文档处理完成: 成功插入 {success_count} 个片段")
        return success_count
    
    def search(self, query_text: str, top_k: int = 10, rerank: bool = True) -> List[Dict]:
        """
        增强版搜索功能，支持重排序
        
        参数:
            query_text: 查询文本
            top_k: 返回结果数量
            rerank: 是否使用重排序
            
        返回:
            相似文档列表，按相关性排序
        """
        self.load_models()
        
        # 第一步：向量相似度搜索
        query_vector = self.text_to_vector(query_text)
        search_params = {
            "metric_type": "COSINE",
            "params": {"ef": 50}  # 更高的搜索参数
        }

        # 先获取更多候选结果用于重排序
        candidate_k = top_k * 3 if rerank else top_k
        
        raw_results = self.collection.search(
            data=[query_vector], 
            anns_field="embedding", 
            param=search_params, 
            limit=candidate_k, 
            output_fields=["id", "file_hash", "filename", "chapter_title", "subsection_title", "content"]
        )

        candidates = []
        for hits in raw_results:
            for hit in hits:
                candidates.append({
                    "id": hit.id,
                    "file_hash": hit.entity.get("file_hash"),
                    "filename": hit.entity.get("filename"),
                    "chapter_title": hit.entity.get("chapter_title"),
                    "subsection_title": hit.entity.get("subsection_title"),
                    "content": hit.entity.get("content"),
                    "distance": hit.distance
                })
        
        # 如果没有重排序或候选结果不足，直接返回
        if not rerank or len(candidates) <= top_k:
            return candidates[:top_k]
        
        # 第二步：重排序
        # 使用更强大的交叉编码器重新排序结果
        # 注意：这部分需要安装sentence-transformers的CrossEncoder
        try:
            from sentence_transformers import CrossEncoder
            
            if self.reranker_model is None:
                self.reranker_model = CrossEncoder(MODEL_CONFIG['reranker'])
            
            # 准备重排序对
            pairs = [(query_text, doc['content']) for doc in candidates]
            
            # 计算重排序分数
            rerank_scores = self.reranker_model.predict(pairs)
            
            # 合并分数
            for doc, score in zip(candidates, rerank_scores):
                doc['rerank_score'] = float(score)
            
            # 按重排序分数排序
            candidates.sort(key=lambda x: x['rerank_score'], reverse=True)
            
            # 返回带重排序分数的结果
            return candidates[:top_k]
            
        except ImportError:
            logger.warning("CrossEncoder not available, skipping reranking")
            return candidates[:top_k]
        except Exception as e:
            logger.error(f"Reranking failed: {str(e)}")
            return candidates[:top_k]
    
    def hybrid_search(self, query_text: str, keyword: str = None, top_k: int = 10):
        """
        混合搜索：结合向量搜索和关键词过滤
        
        参数:
            query_text: 查询文本（用于向量搜索）
            keyword: 关键词（用于过滤）
            top_k: 返回结果数量
        """
        # 向量搜索
        vector_results = self.search(query_text, top_k=top_k, rerank=False)
        
        # 如果没有关键词过滤，直接返回向量结果
        if not keyword:
            return vector_results
        
        # 关键词过滤
        keyword_filtered = [
            doc for doc in vector_results 
            if keyword.lower() in doc['content'].lower() or 
               keyword.lower() in doc['chapter_title'].lower() or
               keyword.lower() in doc['subsection_title'].lower()
        ]
        
        # 如果关键词过滤后结果不足，补充向量结果
        if len(keyword_filtered) < top_k:
            additional = [doc for doc in vector_results if doc not in keyword_filtered]
            keyword_filtered.extend(additional[:top_k - len(keyword_filtered)])
        
        return keyword_filtered[:top_k]

    def generate_parse_report(chapters: List[Dict]) -> Dict:
        """生成结构化解析报告"""
        report = {
            "total_chapters": len(chapters),
            "total_subsections": sum(len(chap["subsections"]) for chap in chapters),
            "merged_subsections": sum(1 for chap in chapters for sub in chap["subsections"] if sub.get("is_merged")),
            "chapter_details": [
                {
                    "title": chap["title"],
                    "subsection_count": len(chap["subsections"]),
                    "content_length": sum(len(text) for text in chap["content"]),
                    "example_subsections": [sub["title"] for sub in chap["subsections"][:2]]
                }
                for chap in chapters
            ]
        }
        return report