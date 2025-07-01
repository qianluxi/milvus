from flask import Flask, request, jsonify, render_template, redirect, url_for
import os
import logging
from Vector_search_plus import VectorSearchSystem
import json

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
        # 获取项目名称
        project_name = request.form.get('project_name', '').strip()
        if not project_name:
            return jsonify({"success": False, "error": "项目名称不能为空"}), 400
        
        # 处理文件上传
        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({"success": False, "error": "没有上传任何文件"}), 400
        
        # 创建项目子目录
        project_dir = os.path.join(UPLOAD_FOLDER, project_name)
        os.makedirs(project_dir, exist_ok=True)
        
        # 保存文件到项目目录
        saved_files = []
        for file in files:
            if not file.filename.lower().endswith('.docx'):
                continue
            filepath = os.path.join(project_dir, file.filename)
            file.save(filepath)
            saved_files.append(file.filename)
        
        # 使用搜索系统处理文档
        result = search_system.insert_documents(project_dir, project_name=project_name)
        
        # 返回 JSON 响应而不是重定向
        return jsonify({
            "success": True,
            "message": f"成功处理 {result} 个文档",
            "project_name": project_name,
            "files": saved_files
        }), 200
    except Exception as e:
        app.logger.error(f"处理上传文件失败: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"文件处理失败: {str(e)}"
        }), 500

@app.route('/process_directory', methods=['POST'])
def process_directory():
    try:
        data = request.get_json()
        file_dir = data.get('directory')
        project_name = data.get('project_name', os.path.basename(os.path.normpath(file_dir)))
        
        if not file_dir or not os.path.isdir(file_dir):
            return jsonify({"success": False, "error": "无效的目录路径"}), 400
        
        # 使用搜索系统处理目录
        result = search_system.insert_documents(file_dir, project_name=project_name)
        
        return jsonify({
            "success": True,
            "message": f"成功处理 {result} 个文档",
            "project_name": project_name
        }), 200
    except Exception as e:
        app.logger.error(f"处理目录失败: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"目录处理失败: {str(e)}"
        }), 500

@app.route('/search', methods=['POST'])
def search():
    try:
        # 获取表单数据
        query_text = request.form.get('query', '').strip()
        if not query_text:
            return jsonify({"success": False, "error": "搜索内容不能为空"}), 400

        # 获取所有搜索参数
        top_k = int(request.form.get('top_k', 10))
        rerank = request.form.get('rerank', 'false').lower() == 'true'
        use_llm = request.form.get('use_llm', 'false').lower() == 'true'
        keyword = request.form.get('keyword', '').strip()
        project_names = request.form.getlist('project_names')  # 多项目选择
        
        # 如果未指定项目，设为None（搜索所有项目）
        if not project_names:
            project_names = None

        # 根据是否有关键词决定搜索方式
        if keyword:
            # 混合搜索（关键词过滤+向量搜索）
            results = search_system.hybrid_search(
                query_text, 
                keyword=keyword, 
                top_k=top_k,
                project_names=project_names,
                rerank=rerank,
                use_llm=use_llm
            )
        else:
            # 纯向量搜索
            results = search_system.search(
                query_text, 
                top_k=top_k, 
                rerank=rerank,
                project_names=project_names,
                use_llm=use_llm
            )
        
        # 确保每个结果都有summary字段（即使为空）
        for result in results:
            if 'summary' not in result:
                result['summary'] = None
        
        return jsonify({
            "success": True,
            "results": results,
            "stats": {
                "count": len(results),
                "rerank_enabled": rerank,
                "llm_processed": use_llm
            }
        }), 200
        
    except Exception as e:
        app.logger.error(f"搜索错误: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "搜索过程中发生错误"
        }), 500

@app.route('/hybrid_search', methods=['POST'])
def hybrid_search():
    try:
        # 从JSON body获取参数
        data = request.get_json()
        query_text = data.get('query', '').strip()
        keyword = data.get('keyword', '').strip()
        top_k = int(data.get('top_k', 10))
        rerank = data.get('rerank', False)
        use_llm = data.get('use_llm', False)
        project_names = data.get('project_names', [])
        
        if not query_text:
            return jsonify({
                "success": False,
                "error": "搜索内容不能为空"
            }), 400

        # 执行混合搜索
        results = search_system.hybrid_search(
            query_text, 
            keyword=keyword, 
            top_k=top_k,
            project_names=project_names if project_names else None,
            rerank=rerank,
            use_llm=use_llm
        )
        
        # 确保每个结果都有summary字段（即使为空）
        for result in results:
            if 'summary' not in result:
                result['summary'] = None
        
        return jsonify({
            "success": True,
            "results": results,
            "stats": {
                "count": len(results),
                "rerank_enabled": rerank,
                "llm_processed": use_llm
            }
        }), 200
        
    except Exception as e:
        app.logger.error(f"混合搜索错误: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "混合搜索过程中发生错误"
        }), 500

@app.route('/projects', methods=['GET'])
def get_projects():
    """获取所有项目信息（前端需要的格式）"""
    try:
        projects = search_system.get_project_summary()
        
        # 确保返回的是数组格式
        if isinstance(projects, dict):
            # 如果是字典格式，转换为数组
            formatted_projects = [
                {
                    "name": name,
                    "document_count": details.get("document_count", 0),
                    "file_count": details.get("file_count", 0),
                    "last_updated": details.get("last_updated", "未知时间")
                }
                for name, details in projects.items()
            ]
        elif isinstance(projects, list):
            # 如果已经是数组格式，直接使用
            formatted_projects = projects
        else:
            # 未知格式，返回空数组
            formatted_projects = []
        
        return jsonify(formatted_projects), 200
    except Exception as e:
        app.logger.error(f"获取项目列表失败: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/project/<name>', methods=['GET'])
def get_project(name):
    """获取特定项目详情"""
    try:
        project_info = search_system.get_project_metadata(name)
        if not project_info:
            return jsonify({"success": False, "error": "项目不存在"}), 404
            
        # 获取项目中的文档数量
        expr = f"project_name == '{name}'"
        count = search_system.collection.query(expr=expr, count_only=True)
        
        # 获取项目中的文件列表
        file_list = []
        project_dir = os.path.join(UPLOAD_FOLDER, name)
        if os.path.exists(project_dir):
            file_list = [
                f for f in os.listdir(project_dir) 
                if os.path.isfile(os.path.join(project_dir, f))
            ]
        
        # 构建完整响应
        response = {
            "name": name,
            "document_count": count,
            "file_count": len(file_list),
            "last_updated": project_info.get("last_updated", "未知时间"),
            "files": file_list
        }
        
        return jsonify(response), 200
    except Exception as e:
        app.logger.error(f"获取项目详情失败: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == '__main__':
    try:
        # 运行Flask应用
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logging.error(f"应用启动失败: {e}")
        raise