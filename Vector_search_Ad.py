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

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 模型配置
MODEL_CONFIG = {
    'embedding': {
        'default': 'BAAI/bge-large-zh-v1.5',  # 中文嵌入模型
        'en': 'BAAI/bge-large-en-v1.5',      # 英文嵌入模型
        'small': 'paraphrase-MiniLM-L6-v2'    # 轻量级模型
    },
    'reranker': 'BAAI/bge-reranker-large'     # 重排序模型
}

class VectorSearchSystem:
    def __init__(self, collection_name="text_search"):
        self.collection_name = collection_name
        self.dim = 1024  # bge-large模型的维度
        self.collection = None
        self.embedding_model = None
        self.reranker_model = None
        self.metadata_dict = {}
        self.field_handlers = {
            'filename': {'max_len': 256, 'truncate': 'hash'},
            'chapter_title': {'max_len': 300, 'truncate': 'hash'},
            'subsection_title': {'max_len': 500, 'truncate': 'simple'},  # 特殊处理
            'content': {'max_len': 65535, 'truncate': 'reject'}
        }

        # 初始化连接和集合
        self._initialize()
        # 确保模型在初始化时就加载
        self.load_models()  # 新增这行
    
    def _initialize(self):
        """初始化系统"""
        # 连接Milvus
        connections.connect("default", host="localhost", port="19530")
        
        # 创建集合
        self._create_collection_if_not_exists()
        
        # 加载模型（按需加载）
    
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
                FieldSchema(name="metadata", dtype=DataType.JSON)  # 新增字段存储额外元数据
            ]

            # 创建集合模式
            schema = CollectionSchema(fields, "Enhanced text search collection")

            # 创建集合
            self.collection = Collection(name=self.collection_name, schema=schema)

            # 创建索引
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {"M": 24, "efConstruction": 300}  # 更强大的索引参数
            }

            self.collection.create_index(field_name="embedding", index_params=index_params)
            self.collection.load()
            logger.info(f"Collection '{self.collection_name}' created and loaded successfully.")
        else:
            self.collection = Collection(name=self.collection_name)
            self.collection.load()
            logger.info(f"Collection '{self.collection_name}' loaded successfully.")
    
    def load_models(self, model_type='default'):
        """按需加载模型"""
        if self.embedding_model is None:
            model_name = MODEL_CONFIG['embedding'].get(model_type, MODEL_CONFIG['embedding']['default'])
            logger.info(f"Loading embedding model: {model_name}")
            try:
                self.embedding_model = SentenceTransformer(model_name)
                self.embedding_model_name = model_name  # 显式存储模型名称
                self.dim = self.embedding_model.get_sentence_embedding_dimension()
                logger.info(f"模型加载成功，维度: {self.dim}")
            except Exception as e:
                logger.error(f"模型加载失败: {str(e)}")
                raise RuntimeError(f"无法加载嵌入模型: {str(e)}")
        
        # 重排序模型按需加载
        # if self.reranker_model is None:
        #     logger.info(f"Loading reranker model: {MODEL_CONFIG['reranker']}")
        #     self.reranker_model = CrossEncoder(MODEL_CONFIG['reranker'])
    
    def parse_document_structure(self, sections: List[Dict]) -> List[Dict]:
        """精确的文档结构解析（解决内容归属问题）
        
        改进点：
        1. 缓冲队列确保内容正确归属
        2. 主章节/子章节的严格区分
        3. 自动修复孤立内容
        """
        chapters = []
        current_chapter = {"title": "未命名章节", "content": [], "subsections": []}
        current_subsection = None
        content_buffer = []  # 内容缓冲队列

        for section in sections:
            try:
                # === 主章节处理 ===
                if section["type"] == "main_chapter":
                    # 提交缓冲内容到当前结构
                    if content_buffer:
                        if current_subsection:
                            current_subsection["content"].extend(content_buffer)
                        else:
                            current_chapter["content"].extend(content_buffer)
                        content_buffer = []

                    # 保存当前章节（非初始状态时）
                    if current_chapter["title"] != "未命名章节":
                        chapters.append(current_chapter)

                    # 新建章节
                    current_chapter = {
                        "title": section["text"],
                        "content": [],
                        "subsections": []
                    }
                    current_subsection = None
                    continue

                # === 子章节处理 ===
                if section["type"] == "sub_chapter":
                    # 提交缓冲内容
                    if content_buffer:
                        if current_subsection:
                            current_subsection["content"].extend(content_buffer)
                        else:
                            current_chapter["content"].extend(content_buffer)
                        content_buffer = []

                    # 新建子章节（标题作为首行内容）
                    current_subsection = {
                        "title": section["text"],
                        "content": [section["text"]]  # 标题作为首行
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
            if current_subsection:
                current_subsection["content"].extend(content_buffer)
            else:
                current_chapter["content"].extend(content_buffer)

        # 2. 保存最后一个章节
        if current_chapter["title"] != "未命名章节":
            chapters.append(current_chapter)

        # 3. 后处理：修复孤立内容
        for chapter in chapters:
            # 情况1：有主内容但无子章节 -> 转换为子章节
            if chapter["content"] and not chapter["subsections"]:
                chapter["subsections"].append({
                    "title": f"{chapter['title']}-概要",
                    "content": chapter["content"]
                })
                chapter["content"] = []

            # 情况2：子章节内容为空 -> 用标题填充
            for subsection in chapter["subsections"]:
                if not subsection["content"]:
                    subsection["content"] = [subsection["title"]]

        return chapters
    
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
        """增强版Word文档解析（精确保留编号和结构）
        
        改进点：
        1. 智能处理自动编号与手动编号冲突
        2. 保留原始缩进和段落结构
        3. 增强的错误恢复机制
        """
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
                    list_levels = []
                    try:
                        if para.Range.ListFormat.ListType != 0:
                            # 获取完整多级编号（如"1.1.2"）
                            for level in range(1, para.Range.ListFormat.ListLevelNumber + 1):
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
                    else:
                        # 情况3：无编号
                        final_text = raw_text

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
                        "level": len(list_levels),
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
    
    def text_to_vector(self, text: str) -> List[float]:
        """将文本转换为向量"""
        try:
            # 确保模型已加载
            if self.embedding_model is None:
                self.load_models()  # 如果未加载则尝试加载
            
            # 如果仍然为None，抛出明确错误
            if self.embedding_model is None:
                raise RuntimeError("嵌入模型未能正确初始化")
            
            # 中文模型添加指令
            if self.embedding_model_name and any(
                keyword in self.embedding_model_name.lower() 
                for keyword in ['zh', 'chinese', 'm3e', 'bge']
            ):
                text = "为这个句子生成表示以用于检索相关文章：" + text
                
            vector = self.embedding_model.encode(text, normalize_embeddings=True)
            return vector.tolist()
        except Exception as e:
            logger.error(f"文本向量化失败 - 模型状态: {self.embedding_model is not None}, 错误: {e}")
            raise RuntimeError(f"无法生成文本向量: {str(e)}")
        
    @staticmethod
    def log_subsections_to_file(filename: str, chapters: List[Dict], log_dir: str = "./uploads"):
        """生成格式优化的解析日志（处理重复内容+改进分隔符）"""
        try:
            os.makedirs(log_dir, exist_ok=True)
            output_path = os.path.join(log_dir, f"{os.path.splitext(filename)[0]}_parsed.txt")
            
            seen_content = set()  # 用于检测重复内容
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # 文件头信息
                f.write("="*80 + "\n")
                f.write(f"文档解析日志：{filename}\n")
                f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    # 章节标题
                    chap_title = chapter.get('title', f'第{chap_idx}章')
                    f.write("="*80 + "\n")
                    f.write(f"【{chap_title}】\n")
                    f.write("="*80 + "\n\n")
                    
                    # 章节内容（去重处理）
                    chap_content = '\n'.join(chapter.get('content', []))
                    if chap_content and chap_content not in seen_content:
                        f.write(chap_content + "\n\n")
                        seen_content.add(chap_content)
                    
                    # 子章节处理
                    for sub_idx, subsection in enumerate(chapter.get('subsections', []), 1):
                        sub_title = subsection.get('title', f'子章节{sub_idx}')
                        f.write("-"*60 + "\n")
                        f.write(f"* {sub_title}\n")
                        f.write("-"*60 + "\n")
                        
                        # 子章节内容（去重处理）
                        sub_content = '\n'.join(subsection.get('content', []))
                        if sub_content and sub_content not in seen_content:
                            f.write(sub_content + "\n\n")
                            seen_content.add(sub_content)
            
            logger.info(f"日志文件已生成：{output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"生成日志失败：{str(e)}")
            return None

    def _process_field(self, field_name: str, value: str) -> str:
        """智能字段处理（确保subsection_title永不失败）"""
        handler = self.field_handlers.get(field_name, {})
        max_len = handler.get('max_len', float('inf'))
        
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

    def insert_documents(self, file_dir: str, app=None):
        """确保100%数据插入的终极版本"""
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
                self.log_subsections_to_file(filename, chapters)                
                
                for chap_idx, chapter in enumerate(chapters, 1):
                    # 处理章节
                    chapter_content = '\n'.join(chapter['content']) or chapter['title']
                    if chapter_content.strip():
                        try:
                            chap_hash = self._calculate_segment_hash(chapter_content, filename, chap_idx, 0)
                            doc = {
                                "embedding": self.text_to_vector(chapter_content),
                                "file_hash": chap_hash,
                                "filename": self._process_field('filename', filename),
                                "chapter_title": self._process_field('chapter_title', chapter['title']),
                                "subsection_title": self._process_field('subsection_title', chapter['title']),
                                "content": chapter_content[:65535],  # 简单截断
                                "metadata": {
                                    "section_type": "chapter",
                                    "original_title": chapter['title']
                                }
                            }
                            self.collection.insert([doc])
                            success_count += 1
                        except Exception as e:
                            logger.error(f"章节插入失败（已简化标题）: {filename} Chap{chap_idx} - {str(e)}")
                            # 终极回退：仅插入必要字段
                            self.collection.insert([{
                                "embedding": doc["embedding"],
                                "file_hash": doc["file_hash"],
                                "content": doc["content"]
                            }])

                    # 处理子章节（强制插入）
                    for sub_idx, subsection in enumerate(chapter['subsections'], 1):
                        subsection_content = '\n'.join(subsection['content']) or subsection['title']
                        try:
                            sub_hash = self._calculate_segment_hash(subsection_content, filename, chap_idx, sub_idx)
                            doc = {
                                "embedding": self.text_to_vector(subsection_content),
                                "file_hash": sub_hash,
                                "filename": self._process_field('filename', filename),
                                "chapter_title": self._process_field('chapter_title', chapter['title']),
                                "subsection_title": self._process_field('subsection_title', subsection['title']),
                                "content": subsection_content[:65535],
                                "metadata": {
                                    "section_type": "subsection",
                                    "original_title": subsection['title'],
                                    "parent_chapter": chapter['title']
                                }
                            }
                            self.collection.insert([doc])
                            success_count += 1
                        except Exception as e:
                            logger.error(f"子章节插入失败（已简化标题）: {filename} {chap_idx}.{sub_idx} - {str(e)}")
                            # 保底插入最小数据集
                            self.collection.insert([{
                                "embedding": doc["embedding"],
                                "file_hash": doc["file_hash"],
                                "content": doc["content"],
                                "subsection_title": f"Section-{chap_idx}.{sub_idx}"
                            }])
                            
            except Exception as e:
                logger.error(f"文件处理错误（跳过）: {filename} - {str(e)}")
            finally:
                pythoncom.CoUninitialize()

        self.collection.load()
        logger.info(f"文档处理完成，成功插入 {success_count} 个片段")
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