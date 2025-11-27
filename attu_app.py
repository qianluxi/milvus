from flask import Flask, request, jsonify, render_template, redirect, url_for, Response
from flask_cors import CORS
import os
import logging
from Vector_search_plus import VectorSearchSystem
import json
from datetime import datetime
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 配置CORS - 允许所有来源的请求
CORS(app, resources={r"/*": {"origins": "*"}})

# 配置上传文件夹
UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# 初始化搜索系统
search_system = VectorSearchSystem()

# 健康检查
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "message": "API服务正常运行",
        "timestamp": datetime.now().isoformat()
    })

# 非流式问答
@app.route('/ask', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({"success": False, "error": "问题不能为空"}), 400
        
        result = search_system.execute_search(
            query_text=question,
            use_llm=True,
            rerank=False
        )
        
        return jsonify({
            "success": True,
            "answer": result.get('answer', '抱歉，未能生成回答'),
            "results": result.get('results', [])
        }), 200
        
    except Exception as e:
        app.logger.error(f"问答处理失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 流式问答
@app.route('/ask_stream', methods=['POST'])
def ask_question_stream():
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({"success": False, "error": "问题不能为空"}), 400
        
        result = search_system.execute_search(
            query_text=question,
            use_llm=True,
            rerank=False
        )
        
        answer = result.get('answer', '')

        def generate():
            for word in answer.split():
                yield f"data: {json.dumps({'content': word + ' ', 'completed': False})}\n\n"
                time.sleep(0.05)
            
            yield f"data: {json.dumps({'completed': True})}\n\n"
            yield f"data: {json.dumps({'results': result.get('results', [])})}\n\n"
            yield "data: {}\n\n"

        return Response(
            generate(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'
            }
        )

    except Exception as e:
        app.logger.error(f"流式问答失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 首页
@app.route('/')
def index():
    return render_template('index.html')

# 文件上传
@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        project_name = request.form.get('project_name', '').strip()
        project_code = request.form.get('project_code', '').strip()
        project_date = request.form.get('project_date', '')

        if not project_name:
            return jsonify({"success": False, "error": "项目名称不能为空"}), 400
        if not project_code:
            return jsonify({"success": False, "error": "项目编号不能为空"}), 400

        if project_date:
            try:
                datetime.strptime(project_date, "%Y-%m-%d")
            except:
                return jsonify({"success": False, "error": "日期格式无效"}), 400

        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({"success": False, "error": "请选择至少一个文件"}), 400

        project_dir = os.path.join(UPLOAD_FOLDER, project_name)
        os.makedirs(project_dir, exist_ok=True)

        saved_files = []
        for file in files:
            if file.filename.lower().endswith('.docx'):
                filepath = os.path.join(project_dir, file.filename)
                file.save(filepath)
                saved_files.append(file.filename)

        inserted_count = search_system.insert_documents(
            project_dir,
            project_name=project_name,
            project_code=project_code,
            project_date=project_date
        )

        return jsonify({
            "success": True,
            "message": f"成功处理 {inserted_count} 个文档片段",
            "project_name": project_name,
            "project_code": project_code,
            "files": saved_files
        }), 200

    except Exception as e:
        app.logger.error(f"上传失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 过滤条件添加
@app.route('/context/add', methods=['POST'])
def add_constraint():
    try:
        data = request.get_json()
        constraint_type = data.get('constraint_type')
        value = data.get('value')

        search_system.update_context(constraint_type, value)

        return jsonify({
            "success": True,
            "context": search_system._get_context_info()
        }), 200

    except Exception as e:
        app.logger.error(f"添加筛选条件失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 过滤条件移除
@app.route('/context/remove/<int:index>', methods=['POST'])
def remove_constraint(index):
    try:
        context = search_system.remove_constraint(index)
        return jsonify({"success": True, "context": context}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# 清空过滤条件
@app.route('/context/clear', methods=['POST'])
def clear_constraints():
    try:
        search_system.clear_context()
        return jsonify({
            "success": True,
            "message": "已清除所有筛选条件",
            "context": search_system._get_context_info()
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# 获取当前筛选条件
@app.route('/context', methods=['GET'])
def get_context():
    try:
        return jsonify({
            "success": True,
            "context": search_system._get_context_info()
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# -------------------------
# ★★ 关键修改点：项目列表永远为全量 ★★
# -------------------------
@app.route('/projects', methods=['GET'])
def get_projects():
    try:
        project_stats = search_system.get_all_projects_with_stats()
        return jsonify({
            "success": True,
            "projects": project_stats
        }), 200

    except Exception as e:
        app.logger.error(f"获取项目失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 删除项目
@app.route('/projects/delete', methods=['POST'])
def delete_projects():
    try:
        data = request.get_json()
        project_names = data.get('project_names', [])

        result = search_system.delete_projects(project_names)
        return jsonify(result), (200 if result["success"] else 500)

    except Exception as e:
        app.logger.error(f"删除失败: {str(e)}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

# 项目详情
@app.route('/project/<name>', methods=['GET'])
def get_project_details(name):
    try:
        metadata = search_system.get_project_metadata(name)
        if not metadata:
            return jsonify({"success": False, "error": "项目不存在"}), 404
        
        expr = f"project_name == '{name}'"
        doc_count = search_system.collection.query(expr=expr, count_only=True)

        return jsonify({
            "name": name,
            "description": search_system.get_project_description(name),
            "document_count": doc_count,
            "file_count": metadata.get("file_count", 0),
            "files": metadata.get("files", []),
            "last_updated": metadata.get("last_updated", "未知"),
            "start_date": metadata.get("start_date", "未知")
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# -------------------------
# ★★ 仅返回符合筛选条件的项目（当前筛选条件区用）★★
# -------------------------
@app.route('/projects/filtered', methods=['GET'])
def get_filtered_projects():
    try:
        filtered_project_names = search_system.get_current_project_set()
        
        filtered_projects = []
        for name in filtered_project_names:
            meta = search_system.get_project_metadata(name) or {}
            project_code = meta.get("project_code", "NO_CODE")
            display_name = f"{name}_{project_code}"

            filtered_projects.append({
                "name": name,
                "display_name": display_name,
                "project_code": project_code
            })

        filtered_projects.sort(key=lambda x: x["display_name"].lower())

        return jsonify(filtered_projects), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# 重定向
@app.route('/home')
def home_redirect():
    return redirect(url_for('index'))

if __name__ == '__main__':
    try:
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    except Exception as e:
        logging.error(f"启动失败: {e}")
        raise
