from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
from docx import Document
from pathlib import Path
import pythoncom
import win32com.client as win32
from typing import List, Dict, Optional


# 初始化SentenceTransformer模型
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# 设置集合名称和维度
collection_name = "text_search"
dim = 384

# 全局变量
collection = None
metadata_dict = {}  # 用于存储文件元数据，键为file_hash

def create_collection_if_not_exists():
    """如果不存在，则创建Milvus集合"""
    global collection

    if not utility.has_collection(collection_name):
        # 定义字段
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="file_hash", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=256),
            FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=300),
            FieldSchema(name="subsection_title", dtype=DataType.VARCHAR, max_length=500),            
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535, nullable=True)
        ]

        # 创建集合模式
        schema = CollectionSchema(fields, "Text search collection")

        # 创建集合
        collection = Collection(name=collection_name, schema=schema)

        # 创建索引
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {"M": 16, "efConstruction": 200}
        }

        collection.create_index(field_name="embedding", index_params=index_params)
        collection.load()
        print(f"Collection '{collection_name}' created and loaded successfully.")
    else:
        collection = Collection(name=collection_name)

        # 检查是否已有索引，没有则创建
        if not collection.has_index():
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {"M": 16, "efConstruction": 200}
            }
            collection.create_index(field_name="embedding", index_params=index_params)

        collection.load()
        print(f"Collection '{collection_name}' loaded successfully.")
    
    return collection

def get_collection():
    """获取Milvus集合对象"""
    global collection
    if collection is None:
        collection = create_collection_if_not_exists()
    return collection

def parse_document_structure(sections: List[Dict]) -> List[Dict]:
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


def determine_section_type(text: str, style_name: str, list_value: str) -> str:
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


def read_docx_with_win32com(filepath: str) -> List[Dict]:
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
                section_type = determine_section_type(
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

def calculate_segment_hash(content: str, filename: str, chapter_index: int, subsection_index: int) -> str:
    """为每个片段计算唯一的哈希值"""
    segment_identifier = f"{filename}_chap{chapter_index + 1}_sec{subsection_index + 1}"
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

def text_to_vector(text: str) -> List[float]:
    """将文本转换为向量并归一化"""
    vector = model.encode(text)
    
    if isinstance(vector, list):
        vector = np.array(vector)
    
    if len(vector.shape) > 1:
        vector = vector[0]
    
    normalized_vector = vector / np.linalg.norm(vector)
    return normalized_vector.tolist()

def log_subsections_to_file(filename: str, chapters: List[Dict]):
    """将子章节内容记录到指定文件"""
    uploads_dir = "./uploads"
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)
    
    output_filename = os.path.join(uploads_dir, os.path.splitext(os.path.basename(filename))[0] + "_parsed.txt")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        for chapter_idx, chapter in enumerate(chapters, 1):
            f.write(f"=== 第{chapter_idx}章: {chapter['title']} ===\n\n")
            
            if chapter['content']:
                f.write("【章节主内容】\n")
                f.write('\n'.join(chapter['content']) + "\n\n")
            
            for sub_idx, subsection in enumerate(chapter['subsections'], 1):
                f.write(f"--- 子章节 {sub_idx}: {subsection['title']} ---\n")
                if subsection['content']:
                    f.write('\n'.join(subsection['content']) + "\n")
                f.write("\n")
                f.write("-"*100 + "\n\n")
            
            f.write("="*200 + "\n\n")

def insert_text_files(file_dir: str, app):
    vectors = []
    
    for filename in os.listdir(file_dir):
        if not filename.endswith(".docx"):
            continue
            
        filepath = os.path.join(file_dir, filename)
        try:
            pythoncom.CoInitialize()
            sections = read_docx_with_win32com(filepath)
            chapters = parse_document_structure(sections)
            log_subsections_to_file(filename, chapters)

            app.logger.info(f"开始处理文件: {filename}，共识别到 {len(chapters)} 个章节")
            
            for chap_idx, chapter in enumerate(chapters, 1):
                # 处理章节主内容（使用标题作为兜底）
                chapter_content = '\n'.join(chapter['content']) or chapter['title']
                
                # 生成章节向量（即使内容为空）
                if chapter_content.strip():
                    chap_hash = calculate_segment_hash(chapter_content, filename, chap_idx, 0)
                    vectors.append({
                        "embedding": text_to_vector(chapter_content),
                        "file_hash": chap_hash,
                        "filename": f"{filename}_chap{chap_idx}",
                        "chapter_title": chapter['title'],
                        "subsection_title": chapter['title'][:100],                        
                        "content": chapter_content
                    })

                # 处理子章节（保留原始编号）
                for sub_idx, subsection in enumerate(chapter['subsections'], 1):
                    # 拼接编号和内容（保留原始段落结构）
                    subsection_content = '\n'.join([f"{i+1}.\t{line}" for i, line in enumerate(subsection['content'])])
                    
                    # 如果没有实际内容，使用子章节标题
                    if not subsection_content.strip():
                        subsection_content = subsection['title']
                    
                    sub_hash = calculate_segment_hash(subsection_content, filename, chap_idx, sub_idx)
                    
                    vectors.append({
                        "embedding": text_to_vector(subsection_content),
                        "file_hash": sub_hash,
                        "filename": f"{filename}_chap{chap_idx}_sec{sub_idx}",
                        "chapter_title": chapter['title'],
                        "subsection_title": subsection['title'][:100],                            
                        "content": subsection_content
                    })
                        
        except Exception as e:
            app.logger.error(f"处理文件 {filename} 失败: {str(e)}", exc_info=True)
        finally:
            pythoncom.CoUninitialize()
    
    # 批量插入数据
    if vectors:
        try:
            collection = get_collection()
            insert_result = collection.insert(vectors)
            collection.load()
            app.logger.info(f"成功插入 {len(vectors)} 条向量数据")
            return insert_result
        except Exception as e:
            app.logger.error(f"数据库插入失败: {str(e)}", exc_info=True)
            raise
    else:
        app.logger.info("没有需要插入的新数据")

def search_similar_texts(query_text: str, top_k: int = 10) -> List[Dict]:
    """在Milvus中搜索最相似的文档"""
    query_vector = text_to_vector(query_text)
    search_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10}
    }

    results = collection.search(
        data=[query_vector], 
        anns_field="embedding", 
        param=search_params, 
        limit=top_k, 
        output_fields=["id", "file_hash", "filename", "chapter_title", "subsection_title", "content"]
    )

    similar_docs = []
    for hits in results:
        for hit in hits:
            similar_docs.append({
                "id": hit.id,
                "file_hash": hit.entity.get("file_hash"),
                "filename": hit.entity.get("filename"),
                "chapter_title": hit.entity.get("chapter_title"),
                "subsection_title": hit.entity.get("subsection_title")[:100],
                "content": hit.entity.get("content"),
                "distance": hit.distance
            })
    return similar_docs