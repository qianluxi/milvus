from flask import Flask, request, jsonify, render_template, redirect, url_for
import os
import logging
from Vector_search_Ad import VectorSearchSystem

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 配置上传文件夹
UPLOAD_FOLDER = './uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 初始化搜索系统
search_system = VectorSearchSystem()

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
        
        # 保存文件到上传目录
        for file in files:
            filepath = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(filepath)
        
        # 使用搜索系统处理文档
        search_system.insert_documents(UPLOAD_FOLDER, app)
        
        return jsonify({"message": "Files have been uploaded and processed successfully."}), 200
    except Exception as e:
        app.logger.error(f"Failed to process uploaded files: {e}", exc_info=True)
        return jsonify({"error": f"Failed to process files: {str(e)}"}), 500

@app.route('/process_directory', methods=['POST'])
def process_directory():
    try:
        data = request.get_json()
        file_dir = data.get('directory')
        
        if not file_dir or not os.path.isdir(file_dir):
            return jsonify({"error": "Invalid directory path."}), 400
        
        # 使用搜索系统处理目录
        search_system.insert_documents(file_dir, app)
        
        return jsonify({"message": "Directory processed successfully."}), 200
    except Exception as e:
        app.logger.error(f"Failed to process directory: {e}", exc_info=True)
        return jsonify({"error": f"Failed to process directory: {str(e)}"}), 500

@app.route('/search', methods=['POST'])
def search():
    try:
        query_text = request.form.get('query', '').strip()
        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        # 获取搜索参数
        top_k = int(request.form.get('top_k', 10))
        rerank = request.form.get('rerank', 'true').lower() == 'true'
        keyword = request.form.get('keyword', '').strip()

        # 执行搜索
        if keyword:
            results = search_system.hybrid_search(query_text, keyword=keyword, top_k=top_k)
        else:
            results = search_system.search(query_text, top_k=top_k, rerank=rerank)
        
        return jsonify(results), 200
    except Exception as e:
        app.logger.error(f"Search error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/hybrid_search', methods=['POST'])
def hybrid_search():
    try:
        data = request.get_json()
        query_text = data.get('query', '').strip()
        keyword = data.get('keyword', '').strip()
        top_k = int(data.get('top_k', 10))
        
        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        # 执行混合搜索
        results = search_system.hybrid_search(query_text, keyword=keyword, top_k=top_k)
        return jsonify(results), 200
    except Exception as e:
        app.logger.error(f"Hybrid search error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    try:
        # 运行Flask应用
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        raise