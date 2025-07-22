from flask import Flask, request, jsonify, render_template, redirect, url_for, Response  # 添加 Response
import os
import logging
from Vector_search_plus import VectorSearchSystem
import json
import uuid
from datetime import datetime
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# 配置上传文件夹
UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# 初始化搜索系统
search_system = VectorSearchSystem()


@app.route('/')
def index():
    """渲染主页"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """处理文件上传（支持多文件，与前端上传表单对应）"""
    try:
        app.logger.info("收到上传请求")
        app.logger.info(f"表单数据: {request.form}")
        app.logger.info(f"文件数量: {len(request.files.getlist('files'))}")        
        # 获取项目信息（添加 project_code 获取）
        project_name = request.form.get('project_name', '').strip()
        project_code = request.form.get('project_code', '').strip()  # 新增项目编号
        project_date = request.form.get('project_date', '')
        
        # 验证必填字段
        if not project_name:
            return jsonify({"success": False, "error": "项目名称不能为空"}), 400
        
        if not project_code:  # 项目编号验证
            return jsonify({"success": False, "error": "项目编号不能为空"}), 400
        
        # 验证日期格式
        if project_date:
            try:
                datetime.strptime(project_date, "%Y-%m-%d")
            except ValueError:
                return jsonify({"success": False, "error": "日期格式无效，请使用YYYY-MM-DD"}), 400
        
        # 处理上传文件
        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({"success": False, "error": "请选择至少一个文件"}), 400
        
        # 创建项目目录
        project_dir = os.path.join(UPLOAD_FOLDER, project_name)
        os.makedirs(project_dir, exist_ok=True)
        
        # 保存文件
        saved_files = []
        for file in files:
            if file.filename.lower().endswith('.docx'):
                filepath = os.path.join(project_dir, file.filename)
                file.save(filepath)
                saved_files.append(file.filename)
        
        # 插入文档到向量库（添加 project_code 参数）
        inserted_count = search_system.insert_documents(
            project_dir,
            project_name=project_name,
            project_date=project_date,
            project_code=project_code  # 添加项目编号
        )
        
        return jsonify({
            "success": True,
            "message": f"成功处理 {inserted_count} 个文档片段",
            "project_name": project_name,
            "project_code": project_code,  # 返回项目编号
            "files": saved_files
        }), 200
        
    except Exception as e:
        app.logger.error(f"文件上传失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"处理失败: {str(e)}"
        }), 500


@app.route('/query/stream', methods=['POST'])
def handle_stream_query():
    """流式查询接口（处理流式问答请求）"""
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        rerank = data.get('rerank', False)
        use_llm = data.get('use_llm', True)
        
        if not question:
            return jsonify({"success": False, "error": "问题不能为空"}), 400
        
        # 调用流式搜索方法
        def generate():
            try:
                # 执行搜索获取完整结果
                result = search_system.execute_search(
                    query_text=question,
                    use_llm=use_llm,
                    rerank=rerank
                )
                
                # 获取AI回答内容
                answer = result.get('answer', '抱歉，未能生成回答')
                
                # 将回答分成单词流式输出
                words = answer.split()
                for word in words:
                    # 发送SSE格式的数据
                    yield f"data: {json.dumps({'text': word + ' '})}\n\n"
                    time.sleep(0.05)  # 控制输出速度
                
                # 发送搜索结果
                yield f"data: {json.dumps({'results': result.get('results', [])})}\n\n"
                
                # 流结束时发送结束信号
                yield "data: {}\n\n"  # 空数据表示结束
                
            except Exception as e:
                logging.error(f"流式生成失败: {str(e)}")
                yield f"data: {json.dumps({'error': '流式输出失败'})}\n\n"
        
        # 构建流式响应
        return Response(generate(), mimetype='text/event-stream')
        
    except Exception as e:
        app.logger.error(f"流式查询处理失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"流式查询失败: {str(e)}"
        }), 500

@app.route('/context/add', methods=['POST'])
def add_constraint():
    """添加筛选条件（与前端添加筛选功能对应）"""
    try:
        data = request.get_json()
        constraint_type = data.get('constraint_type')
        value = data.get('value')
        
        if not constraint_type or value is None:
            return jsonify({"success": False, "error": "缺少约束类型或值"}), 400
        
        # 调用后端添加约束方法
        search_system.update_context(constraint_type, value)
        
        # 返回更新后的上下文
        return jsonify({
            "success": True,
            "context": search_system._get_context_info()
        }), 200
        
    except Exception as e:
        app.logger.error(f"添加约束失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"添加筛选条件失败: {str(e)}"
        }), 500


@app.route('/context/remove/<int:index>', methods=['POST'])
def remove_constraint(index):
    try:
        context = search_system.remove_constraint(index)
        return jsonify({
            "success": True,
            "context": context
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"移除失败: {str(e)}"
        }), 400


@app.route('/context/clear', methods=['POST'])
def clear_constraints():
    """清除所有筛选条件（与前端清除功能对应）"""
    try:
        search_system.clear_context()
        return jsonify({
            "success": True,
            "message": "已清除所有筛选条件",
            "context": search_system._get_context_info()
        }), 200
        
    except Exception as e:
        app.logger.error(f"清除约束失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"清除失败: {str(e)}"
        }), 500


@app.route('/context', methods=['GET'])
def get_context():
    """获取当前筛选条件（供前端显示当前约束）"""
    try:
        context_info = search_system._get_context_info()
        return jsonify({
            "success": True,
            "context": context_info
        }), 200
        
    except Exception as e:
        app.logger.error(f"获取上下文失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"获取筛选条件失败: {str(e)}"
        }), 500


@app.route('/projects', methods=['GET'])
def get_projects():
    """获取所有项目列表（与前端项目列表功能对应）"""
    try:
        project_summary = search_system.get_project_summary()
        
        # 格式化项目信息
        projects = []
        for name, info in project_summary.items():
            projects.append({
                "name": name,
                "document_count": info.get("document_count", 0),
                "file_count": info.get("file_count", 0),
                "last_updated": info.get("last_updated", "未知"),
                "description": search_system.get_project_description(name),  # 项目描述
                "features": info.get("features", [])
            })
        
        return jsonify(projects), 200
        
    except Exception as e:
        app.logger.error(f"获取项目列表失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"获取项目失败: {str(e)}"
        }), 500


@app.route('/project/<name>', methods=['GET'])
def get_project_details(name):
    """获取项目详情（与前端项目详情功能对应）"""
    try:
        # 获取项目元数据
        metadata = search_system.get_project_metadata(name)
        if not metadata:
            return jsonify({"success": False, "error": "项目不存在"}), 404
        
        # 获取项目描述
        description = search_system.get_project_description(name)
        
        # 获取项目文档数量
        expr = f"project_name == '{name}'"
        doc_count = search_system.collection.query(expr=expr, count_only=True)
        
        return jsonify({
            "name": name,
            "description": description,
            "document_count": doc_count,
            "file_count": metadata.get("file_count", 0),
            "files": metadata.get("files", []),
            "last_updated": metadata.get("last_updated", "未知"),
            "start_date": metadata.get("start_date", "未知")
        }), 200
        
    except Exception as e:
        app.logger.error(f"获取项目详情失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"获取详情失败: {str(e)}"
        }), 500

@app.route('/projects/filtered', methods=['GET'])
def get_filtered_projects():
    """获取符合当前筛选条件的项目列表（返回项目名和编号）"""
    try:
        # 1. 获取符合当前约束的项目集合
        filtered_project_names = search_system.get_current_project_set()
        
        # 2. 构造包含名称和编号的项目列表
        filtered_projects = []
        for name in filtered_project_names:
            # 获取项目元数据
            meta = search_system.get_project_metadata(name) or {}
            project_code = meta.get("project_code", "NO_CODE")
            
            # 创建显示名称（项目名_项目编号）
            display_name = f"{name}_{project_code}"
            
            filtered_projects.append({
                "name": name,  # 原始项目名
                "display_name": display_name,  # 显示名称
                "project_code": project_code  # 单独的项目编号
            })
        
        # 3. 按显示名称排序（A-Z）
        filtered_projects.sort(key=lambda x: x["display_name"].lower())
        
        return jsonify(filtered_projects), 200
        
    except Exception as e:
        app.logger.error(f"获取筛选项目列表失败: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": f"获取筛选项目失败: {str(e)}"
        }), 500
    
@app.route('/project/<name>/dates', methods=['GET'])
def get_project_dates(name):
    """获取项目的日期信息（调试用）"""
    try:
        # 获取项目日期
        dates = search_system._get_project_dates_from_milvus({name})
        return jsonify(dates), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/home')
def home_redirect():
    """重定向/home到根路由/"""
    return redirect(url_for('index'))
# 移除冗余路由（原hybrid_search已整合到handle_query）
# 移除原search路由（功能已整合到handle_query）

if __name__ == '__main__':
    try:
        # 运行Flask应用
        app.run(host='0.0.0.0', port=5000, debug=True)
    except Exception as e:
        logging.error(f"应用启动失败: {e}")
        raise