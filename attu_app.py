from flask import Flask, request, jsonify, render_template, redirect, url_for
import os
import logging
from pymilvus import connections
from vector_search import insert_text_files, search_similar_texts, get_collection

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 配置上传文件夹
UPLOAD_FOLDER = './uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route('/')
def index():
    """重定向到主页"""
    return redirect(url_for('home'))

@app.route('/home')
def home():
    """渲染主页"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        # 处理文件上传
        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({"error": "No files were uploaded."}), 400
        
        for file in files:
            filepath = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(filepath)  # 保存文件到服务器上的指定目录
        
        # 调用处理函数
        insert_text_files(UPLOAD_FOLDER,app)
        
        return jsonify({"message": "Files have been uploaded and processed."}), 200
    except Exception as e:
        app.logger.error(f"Failed to process uploaded files: {e}")
        return jsonify({"error": f"Failed to process uploaded files: {str(e)}"}), 500

@app.route('/process_directory', methods=['POST'])
def process_directory():
    try:
        data = request.get_json()
        file_dir = data.get('directory')
        
        if not os.path.isdir(file_dir):
            return jsonify({"error": "Invalid directory path."}), 400
        
        # 插入文本文件到Milvus
        insert_text_files(file_dir)
        
        return jsonify({"message": "Files have been processed successfully."}), 200
    except Exception as e:
        app.logger.error(f"Failed to process directory: {e}")
        return jsonify({"error": f"Failed to process directory: {str(e)}"}), 500

@app.route('/search', methods=['POST'])
def search():
    try:
        data = request.form
        query_text = data.get('query', '')
        print(f"Received query: {query_text}")  # 打印接收到的查询文本
        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        results = search_similar_texts(query_text, top_k=10)
        print(f"Returned results: {results}")  # 打印返回的搜索结果
        return jsonify(results), 200
    except Exception as e:
        app.logger.error(f"Search error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    try:
        # 连接到 Milvus
        connections.connect("default", host="127.0.0.1", port="19530")
        
        # 获取或创建集合
        collection = get_collection()
        
        # 运行 Flask 应用
        app.run(debug=True)
    except Exception as e:
        print(f"启动失败: {e}")
    finally:
        # 断开连接（可选）
        connections.disconnect("default")