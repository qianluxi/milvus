from sentence_transformers import SentenceTransformer
import numpy as np
from pymilvus import Collection, connections, FieldSchema, CollectionSchema, DataType, utility, Index
import os
import re
import hashlib
from docx import Document
from pathlib import Path

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
            FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=300), # 新增字段
            FieldSchema(name="subsection_title", dtype=DataType.VARCHAR, max_length=500), # 新增字段            
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535, nullable=True)  # 设置 content 为可空            
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

        collection.load()  # 加载集合
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

def read_docx(filepath):
    """读取.docx文件内容"""
    try:
        document = Document(filepath)
        full_text = []
        for para in document.paragraphs:
            full_text.append(para.text)
        return '\n'.join(full_text)
    except Exception as e:
        print(f"Failed to read file {filepath}: {e}")
        return ""

def calculate_segment_hash(content, filename, chapter_index, subsection_index):
    """为每个片段计算唯一的哈希值"""
    segment_identifier = f"{filename}_chap{chapter_index + 1}_sec{subsection_index + 1}"
    content_with_identifier = content + segment_identifier  # 将片段标识符与内容结合
    return hashlib.sha256(content_with_identifier.encode()).hexdigest()

def check_if_file_exists(file_hash):
    """检查集合中是否已存在相同哈希值的文件"""
    expr = f"file_hash == '{file_hash}'"
    results = collection.query(expr=expr, output_fields=["id"])
    return len(results) > 0

def text_to_vector(text):
    """将文本转换为向量并归一化"""
    vector = model.encode(text)  # 获取文本的向量表示
    
    # 确认 vector 是一维数组
    if isinstance(vector, list):
        vector = np.array(vector)
    
    if len(vector.shape) > 1:
        # 如果是二维数组，取第一个维度
        vector = vector[0]
    
    normalized_vector = vector / np.linalg.norm(vector)  # 归一化
    return normalized_vector.tolist()  # 转换为Python列表，确保所有元素都是浮点数

def save_content_to_disk(content, file_hash):
        return None
    
def split_text_into_structured_parts(content):
    parts = []
    current_chapter = {'title': '', 'content': [], 'subsections': []}
    current_subsection = None
    
    # 章节标题模式（如【第一章 标题】）
    title_pattern = re.compile(r'^【第[零一二三四五六七八九十百千]+章\s+.*】\s*$')
    # 子标题模式（如 "1. 标题" 或 "一、标题"）
    subtitle_pattern = re.compile(r'^\s*([一二三四五六七八九十]+|\d+)[、\.\s].*$')

    for line in content.split('\n'):
        line = line.rstrip()  # 保留行尾空格
        
        # 1. 检查是否是章节标题
        if title_pattern.match(line):
            if current_chapter['title'] or current_chapter['content'] or current_chapter['subsections']:
                parts.append(current_chapter)
            
            current_chapter = {
                'title': line.strip(),
                'content': [],
                'subsections': []
            }
            current_subsection = None
            continue
            
        # 2. 检查是否是子标题
        if subtitle_pattern.match(line):
            if current_subsection and (current_subsection['title'] or current_subsection['content']):
                current_chapter['subsections'].append(current_subsection)
            
            current_subsection = {'title': line.strip(), 'content': []}
            continue
            
        # 3. 普通内容处理
        if current_subsection is not None:
            current_subsection['content'].append(line)
        else:
            current_chapter['content'].append(line)

    # 处理最后一个子章节和章节
    if current_subsection and (current_subsection['title'] or current_subsection['content']):
        current_chapter['subsections'].append(current_subsection)
    if current_chapter['title'] or current_chapter['content'] or current_chapter['subsections']:
        parts.append(current_chapter)
    
    # **调试输出，查看分割结果**
    print("\n=== 解析结果 ===")
    for chapter in parts:
        print(f"章节: {chapter['title']}")
        print(f"内容: {chapter['content']}")
        for sub in chapter['subsections']:
            print(f"  子章节: {sub['title']}")
            print(f"  内容: {sub['content']}")

    # 处理换行合并，防止内容丢失
    for chapter in parts:
        chapter['content'] = '\n'.join(chapter['content']).strip()
        for sub in chapter['subsections']:
            sub['content'] = '\n'.join(sub['content']).strip()
    
    return parts


def log_subsections_to_file(filename, chapters):
    """将子章节内容记录到指定文件"""
    uploads_dir = "./uploads"
    if not os.path.exists(uploads_dir):
        os.makedirs(uploads_dir)
    
    output_filename = os.path.join(uploads_dir, os.path.splitext(os.path.basename(filename))[0] + "_parsed.txt")
    
    with open(output_filename, 'w', encoding='utf-8') as f:
        for chapter_idx, chapter in enumerate(chapters, 1):
            # Write chapter title
            f.write(f"=== 第{chapter_idx}章: {chapter['title']} ===\n\n")
            
            # Write chapter main content if exists
            if chapter['content']:
                f.write("【章节主内容】\n")
                f.write(chapter['content'] + "\n\n")
            
            # Write subsections
            for sub_idx, subsection in enumerate(chapter['subsections'], 1):
                f.write(f"--- 子章节 {sub_idx}: {subsection['title']} ---\n")
                if subsection['content']:
                    f.write(subsection['content'] + "\n")
                f.write("\n")  # Add extra newline between subsections
            
            f.write("="*80 + "\n\n")  # Add separator between chapters

def insert_text_files(file_dir, app):
    vectors = []
    
    for filename in os.listdir(file_dir):
        if not filename.endswith(".docx"):
            continue
            
        filepath = os.path.join(file_dir, filename)
        try:
            content = read_docx(filepath)
            if not content:
                app.logger.warning(f"空文件: {filename}")
                continue
                
            chapters = split_text_into_structured_parts(content)
            log_subsections_to_file(filename, chapters)  # 调试输出
            
            # 处理章节主内容
            for chap_idx, chapter in enumerate(chapters, 1):
                # 处理章节主内容（如果有）
                if chapter['content']:
                    chap_hash = hashlib.sha256(
                        f"{filename}_chap{chap_idx}_main".encode() + 
                        chapter['content'].encode()
                    ).hexdigest()
                    
                    if not check_if_file_exists(chap_hash):
                        vectors.append({
                            "embedding": text_to_vector(chapter['content']),
                            "file_hash": chap_hash,
                            "filename": f"{filename}_chap{chap_idx}",
                            "chapter_title": chapter['title'],
                            "subsection_title": chapter['title'][:100],                        
                            "content": chapter['content']
                        })
                
                # 处理子章节
                for sub_idx, subsection in enumerate(chapter['subsections'], 1):
                    if not subsection['content']:
                        continue
                        
                    sub_hash = hashlib.sha256(
                        f"{filename}_chap{chap_idx}_sec{sub_idx}".encode() + 
                        subsection['content'].encode()
                    ).hexdigest()
                    
                    if not check_if_file_exists(sub_hash):
                        vectors.append({
                            "embedding": text_to_vector(subsection['content']),
                            "file_hash": sub_hash,
                            "filename": f"{filename}_chap{chap_idx}_sec{sub_idx}",
                            "chapter_title": chapter['title'],
                            "subsection_title": subsection['title'][:100],                            
                            "content": subsection['content']
                        })
                        
        except Exception as e:
            app.logger.error(f"处理文件 {filename} 失败: {str(e)}")
            continue
    
    # 批量插入数据
    if vectors:
        try:
            collection = get_collection()
            insert_result = collection.insert(vectors)
            collection.load()
            app.logger.info(f"成功插入 {len(vectors)} 条向量数据")
            return insert_result
        except Exception as e:
            app.logger.error(f"数据库插入失败: {str(e)}")
            raise

def search_similar_texts(query_text, top_k=10):
    """在Milvus中搜索最相似的文档并返回完整内容"""
    query_vector = text_to_vector(query_text)
    search_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10}
    }

    # 执行搜索（不再请求content_file_path字段）
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
            # 直接使用Milvus中存储的content（不再检查文件路径）
            similar_docs.append({
                "id": hit.id,
                "file_hash": hit.entity.get("file_hash"),
                "filename": hit.entity.get("filename"),
                "chapter_title": hit.entity.get("chapter_title"),
                "subsection_title": hit.entity.get("subsection_title")[:100],
                "content": hit.entity.get("content"),  # 直接从Milvus获取
                "distance": hit.distance
            })
    return similar_docs


