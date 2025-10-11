import os
import logging
import json
import uuid
from datetime import datetime
import time
from Vector_search_plus import VectorSearchSystem
from config import MODEL_CONFIG, ZILLIZ_CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SearchBackend:
    def __init__(self):
        # 配置上传文件夹
        self.UPLOAD_FOLDER = "/tmp/uploads"
        os.makedirs(self.UPLOAD_FOLDER, exist_ok=True)
        
        # 初始化搜索系统
        self.search_system = VectorSearchSystem(
            collection_name="text_searchMC",
            max_retries=5,
            model_config=MODEL_CONFIG,
            zilliz_config=ZILLIZ_CONFIG
        )
    
    # -------- 健康检查相关 --------
    def health_check(self):
        return {
            "status": "healthy",
            "message": "服务正常运行",
            "timestamp": datetime.now().isoformat()
        }
    
    def check_zilliz_connection(self):
        """检查 Zilliz Cloud 连接状态"""
        try:
            is_connected = self.search_system.check_connection()
            return {
                "connected": is_connected,
                "message": "Zilliz Cloud 连接正常" if is_connected else "Zilliz Cloud 连接异常"
            }
        except Exception as e:
            return {
                "connected": False,
                "message": f"连接检查失败: {str(e)}"
            }
    
    def reconnect_zilliz(self):
        """重新连接 Zilliz Cloud"""
        try:
            success = self.search_system.reconnect()
            return {
                "success": success,
                "message": "重新连接成功" if success else "重新连接失败"
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"重新连接失败: {str(e)}"
            }
    
    # -------- 问答相关 --------
    def ask_question(self, question):
        """非流式问答"""
        try:
            if not question:
                return {"success": False, "error": "问题不能为空"}
            
            # 执行搜索
            result = self.search_system.execute_search(
                query_text=question,
                use_llm=True,
                rerank=False
            )
            
            return {
                "success": True,
                "answer": result.get('answer', '抱歉，未能生成回答'),
                "results": result.get('results', [])
            }
            
        except Exception as e:
            logger.error(f"问答处理失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"处理失败: {str(e)}"
            }
    
    def ask_question_stream(self, question):
        """流式问答"""
        try:
            if not question:
                return {"success": False, "error": "问题不能为空"}
            
            # 执行搜索获取完整结果
            result = self.search_system.execute_search(
                query_text=question,
                use_llm=True,
                rerank=False
            )
            
            return {
                "success": True,
                "answer": result.get('answer', '抱歉，未能生成回答'),
                "results": result.get('results', [])
            }
            
        except Exception as e:
            logger.error(f"流式问答处理失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"流式问答失败: {str(e)}"
            }
    
    def handle_stream_query(self, question, rerank=False, use_llm=True):
        """流式查询处理"""
        try:
            if not question:
                return {"success": False, "error": "问题不能为空"}
            
            # 执行搜索获取完整结果
            result = self.search_system.execute_search(
                query_text=question,
                use_llm=use_llm,
                rerank=rerank
            )
            
            return {
                "success": True,
                "answer": result.get('answer', '抱歉，未能生成回答'),
                "results": result.get('results', [])
            }
            
        except Exception as e:
            logger.error(f"流式查询处理失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"流式查询失败: {str(e)}"
            }
    
    # -------- 配置信息 --------
    def get_config(self):
        """获取配置信息"""
        return {
            "success": True,
            "data": {
                "model_config": MODEL_CONFIG,
                "zilliz_config": ZILLIZ_CONFIG,
                "endpoints": {
                    "upload": "/upload",
                    "query": "/query/stream",
                    "health": "/health",
                    "projects": "/projects"
                }
            }
        }
    
    # -------- 文件上传相关 --------
    def upload_files(self, files, project_name, project_code, project_date):
        """处理文件上传"""
        try:
            logger.info("收到上传请求")
            logger.info(f"项目名称: {project_name}, 项目编号: {project_code}, 项目日期: {project_date}")
            logger.info(f"文件数量: {len(files)}")
            
            # 验证必填字段
            if not project_name:
                return {"success": False, "error": "项目名称不能为空"}
            
            if not project_code:
                return {"success": False, "error": "项目编号不能为空"}
            
            # 验证日期格式
            if project_date:
                try:
                    datetime.strptime(project_date, "%Y-%m-%d")
                except ValueError:
                    return {"success": False, "error": "日期格式无效，请使用YYYY-MM-DD"}
            
            # 验证文件
            if not files or all(file.name == '' for file in files):
                return {"success": False, "error": "请选择至少一个文件"}
            
            # 创建项目目录
            project_dir = os.path.join(self.UPLOAD_FOLDER, project_name)
            os.makedirs(project_dir, exist_ok=True)
            
            # 保存文件
            saved_files = []
            for file in files:
                if file.name.lower().endswith('.docx'):
                    filepath = os.path.join(project_dir, file.name)
                    # 在Streamlit中，文件对象有read()方法，我们需要写入文件
                    with open(filepath, "wb") as f:
                        f.write(file.getvalue())
                    saved_files.append(file.name)
            
            # 插入文档到向量库
            inserted_count = self.search_system.insert_documents(
                project_dir,
                project_name=project_name,
                project_date=project_date,
                project_code=project_code
            )
            
            return {
                "success": True,
                "message": f"成功处理 {inserted_count} 个文档片段",
                "project_name": project_name,
                "project_code": project_code,
                "files": saved_files
            }
            
        except Exception as e:
            logger.error(f"文件上传失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"处理失败: {str(e)}"
            }
    
    # -------- 筛选条件管理 --------
    def add_constraint(self, constraint_type, value):
        """添加筛选条件"""
        try:
            if not constraint_type or value is None:
                return {"success": False, "error": "缺少约束类型或值"}
            
            # 调用后端添加约束方法
            self.search_system.update_context(constraint_type, value)
            
            # 返回更新后的上下文
            return {
                "success": True,
                "context": self.search_system._get_context_info()
            }
            
        except Exception as e:
            logger.error(f"添加约束失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"添加筛选条件失败: {str(e)}"
            }
    
    def remove_constraint(self, index):
        """移除筛选条件"""
        try:
            context = self.search_system.remove_constraint(index)
            return {
                "success": True,
                "context": context
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"移除失败: {str(e)}"
            }
    
    def clear_constraints(self):
        """清除所有筛选条件"""
        try:
            self.search_system.clear_context()
            return {
                "success": True,
                "message": "已清除所有筛选条件",
                "context": self.search_system._get_context_info()
            }
            
        except Exception as e:
            logger.error(f"清除约束失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"清除失败: {str(e)}"
            }
    
    def get_context(self):
        """获取当前筛选条件"""
        try:
            context_info = self.search_system._get_context_info()
            return {
                "success": True,
                "context": context_info
            }
            
        except Exception as e:
            logger.error(f"获取上下文失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"获取筛选条件失败: {str(e)}"
            }
    
    # -------- 项目管理 --------
    def get_projects(self):
        """获取所有项目及其统计信息"""
        try:
            project_stats = self.search_system.get_all_projects_with_stats()
            return {
                "success": True,
                "projects": project_stats
            }
            
        except Exception as e:
            logger.error(f"获取项目列表失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"获取项目失败: {str(e)}"
            }
    
    def delete_projects(self, project_names):
        """删除指定项目"""
        try:
            if not project_names:
                return {"success": False, "error": "未选择项目"}
            
            # 执行删除
            result = self.search_system.delete_projects(project_names)
            return result
            
        except Exception as e:
            logger.error(f"删除项目失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"删除失败: {str(e)}"
            }
    
    def get_project_details(self, name):
        """获取项目详情"""
        try:
            # 获取项目元数据
            metadata = self.search_system.get_project_metadata(name)
            if not metadata:
                return {"success": False, "error": "项目不存在"}
            
            # 获取项目描述
            description = self.search_system.get_project_description(name)
            
            # 获取项目文档数量
            expr = f"project_name == '{name}'"
            doc_count = self.search_system.collection.query(expr=expr, count_only=True)
            
            return {
                "name": name,
                "description": description,
                "document_count": doc_count,
                "file_count": metadata.get("file_count", 0),
                "files": metadata.get("files", []),
                "last_updated": metadata.get("last_updated", "未知"),
                "start_date": metadata.get("start_date", "未知")
            }
            
        except Exception as e:
            logger.error(f"获取项目详情失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"获取详情失败: {str(e)}"
            }
    
    def get_filtered_projects(self):
        """获取符合当前筛选条件的项目列表"""
        try:
            # 获取符合当前约束的项目集合
            filtered_project_names = self.search_system.get_current_project_set()
            
            # 构造包含名称和编号的项目列表
            filtered_projects = []
            for name in filtered_project_names:
                # 获取项目元数据
                meta = self.search_system.get_project_metadata(name) or {}
                project_code = meta.get("project_code", "NO_CODE")
                
                # 创建显示名称（项目名_项目编号）
                display_name = f"{name}_{project_code}"
                
                filtered_projects.append({
                    "name": name,
                    "display_name": display_name,
                    "project_code": project_code
                })
            
            # 按显示名称排序（A-Z）
            filtered_projects.sort(key=lambda x: x["display_name"].lower())
            
            return filtered_projects
            
        except Exception as e:
            logger.error(f"获取筛选项目列表失败: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"获取筛选项目失败: {str(e)}"
            }
    
    def get_project_dates(self, name):
        """获取项目的日期信息"""
        try:
            # 获取项目日期
            dates = self.search_system._get_project_dates_from_milvus({name})
            return dates
        except Exception as e:
            return {"error": str(e)}