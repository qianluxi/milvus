from flask import Flask, request, jsonify, render_template, redirect, url_for
import os
import logging
from Vector_search_plus import VectorSearchSystem

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
    # 获取项目列表用于前端显示
    projects = search_system.get_project_summary()
    return render_template('index.html', projects=projects)

@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        # 获取项目名称（新增）
        project_name = request.form.get('project_name', '').strip()
        if not project_name:
            return jsonify({"error": "Project name is required."}), 400
        
        # 处理文件上传
        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({"error": "No files were uploaded."}), 400
        
        # 创建项目子目录
        project_dir = os.path.join(UPLOAD_FOLDER, project_name)
        os.makedirs(project_dir, exist_ok=True)
        
        # 保存文件到项目目录
        for file in files:
            if not file.filename.lower().endswith('.docx'):
                continue
            filepath = os.path.join(project_dir, file.filename)
            file.save(filepath)
        
        # 使用搜索系统处理文档（传入项目名称）
        result = search_system.insert_documents(project_dir, project_name=project_name)
        
        return jsonify({
            "message": f"{result} documents have been processed successfully.",
            "project_name": project_name
        }), 200
    except Exception as e:
        app.logger.error(f"Failed to process uploaded files: {e}", exc_info=True)
        return jsonify({"error": f"Failed to process files: {str(e)}"}), 500

@app.route('/process_directory', methods=['POST'])
def process_directory():
    try:
        data = request.get_json()
        file_dir = data.get('directory')
        project_name = data.get('project_name', os.path.basename(os.path.normpath(file_dir)))
        
        if not file_dir or not os.path.isdir(file_dir):
            return jsonify({"error": "Invalid directory path."}), 400
        
        # 使用搜索系统处理目录（传入项目名称）
        search_system.insert_documents(file_dir, project_name=project_name)
        
        return jsonify({
            "message": "Directory processed successfully.",
            "project_name": project_name
        }), 200
    except Exception as e:
        app.logger.error(f"Failed to process directory: {e}", exc_info=True)
        return jsonify({"error": f"Failed to process directory: {str(e)}"}), 500

@app.route('/search', methods=['POST'])
def search():
    try:
        query_text = request.form.get('query', '').strip()
        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        # 获取搜索参数（新增项目过滤）
        top_k = int(request.form.get('top_k', 10))
        rerank = request.form.get('rerank', 'true').lower() == 'true'
        keyword = request.form.get('keyword', '').strip()
        project_names = request.form.getlist('project_names')  # 多项目选择
        
        # 如果未指定项目，搜索所有项目
        if not project_names:
            project_names = None

        # 执行搜索（新增项目过滤参数）
        if keyword:
            results = search_system.hybrid_search(
                query_text, 
                keyword=keyword, 
                top_k=top_k,
                project_names=project_names
            )
        else:
            results = search_system.search(
                query_text, 
                top_k=top_k, 
                rerank=rerank,
                project_names=project_names
            )
        
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
        project_names = data.get('project_names', [])  # 多项目选择
        
        if not query_text:
            return jsonify({"error": "Query text is required."}), 400

        # 执行混合搜索（新增项目过滤参数）
        results = search_system.hybrid_search(
            query_text, 
            keyword=keyword, 
            top_k=top_k,
            project_names=project_names if project_names else None
        )
        return jsonify(results), 200
    except Exception as e:
        app.logger.error(f"Hybrid search error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/projects', methods=['GET'])
def get_projects():
    """获取所有项目信息"""
    try:
        projects = search_system.get_project_summary()
        return jsonify(projects), 200
    except Exception as e:
        app.logger.error(f"Failed to get projects: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/project/<name>', methods=['GET'])
def get_project(name):
    """获取特定项目详情"""
    try:
        project_info = search_system.get_project_metadata(name)
        if not project_info:
            return jsonify({"error": "Project not found."}), 404
            
        # 获取项目中的文档数量
        expr = f"project_name == '{name}'"
        count = search_system.collection.query(expr=expr, count_only=True)
        
        project_info['document_count'] = count
        return jsonify(project_info), 200
    except Exception as e:
        app.logger.error(f"Failed to get project: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    try:
        # 运行Flask应用
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        raise