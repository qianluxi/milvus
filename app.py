import streamlit as st
import time
import json
from datetime import datetime
from search_backend import SearchBackend

import site, sys
# 确保 /home/studio_service/.local 优先于系统 site-packages
sys.path.insert(0, site.USER_SITE)

import modelscope


print(">>>> 看真实版本modelscope version:", modelscope.__version__)
# 页面配置
st.set_page_config(
    page_title="向量搜索系统",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化后端
@st.cache_resource
def get_backend():
    return SearchBackend()

backend = get_backend()

# 初始化session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_constraints' not in st.session_state:
    st.session_state.current_constraints = []

# 侧边栏
with st.sidebar:
    st.title("🔍 向量搜索系统")
    
    # 健康状态检查
    st.subheader("系统状态")
    if st.button("检查连接状态"):
        health_status = backend.health_check()
        zilliz_status = backend.check_zilliz_connection()
        
        st.success(f"API服务: {health_status['status']}")
        if zilliz_status['connected']:
            st.success(f"Zilliz: {zilliz_status['message']}")
        else:
            st.error(f"Zilliz: {zilliz_status['message']}")
    
    # 筛选条件管理
    st.subheader("筛选条件")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("清除筛选"):
            result = backend.clear_constraints()
            if result['success']:
                st.success("已清除所有筛选条件")
                st.session_state.current_constraints = result.get('context', {}).get('constraints', [])
            else:
                st.error(f"清除失败: {result['error']}")
    
    with col2:
        if st.button("刷新条件"):
            result = backend.get_context()
            if result['success']:
                st.session_state.current_constraints = result.get('context', {}).get('constraints', [])
                st.success("已刷新筛选条件")
    
    # 显示当前筛选条件
    if st.session_state.current_constraints:
        st.write("当前筛选条件:")
        for i, constraint in enumerate(st.session_state.current_constraints):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.text(f"{constraint['type']}: {constraint['value']}")
            with col2:
                if st.button("删除", key=f"del_{i}"):
                    result = backend.remove_constraint(i)
                    if result['success']:
                        st.session_state.current_constraints = result.get('context', {}).get('constraints', [])
                        st.rerun()
    
    # 添加新筛选条件
    st.subheader("添加筛选")
    constraint_type = st.selectbox("条件类型", ["project_name", "project_date", "project_code"])
    constraint_value = st.text_input("条件值")
    if st.button("添加筛选条件"):
        if constraint_value:
            result = backend.add_constraint(constraint_type, constraint_value)
            if result['success']:
                st.session_state.current_constraints = result.get('context', {}).get('constraints', [])
                st.success("筛选条件添加成功")
                st.rerun()
            else:
                st.error(f"添加失败: {result['error']}")
        else:
            st.warning("请输入条件值")

# 主界面
st.title("🔍 智能文档搜索系统")

# 创建标签页
tab1, tab2, tab3, tab4 = st.tabs(["💬 智能问答", "📤 文档上传", "📂 项目管理", "⚙️ 系统配置"])

# 标签页1: 智能问答
with tab1:
    st.header("智能问答")
    
    # 问题输入
    question = st.text_area("请输入您的问题:", height=100, placeholder="在这里输入您的问题...")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        use_stream = st.checkbox("使用流式输出", value=True)
    with col2:
        use_llm = st.checkbox("使用AI回答", value=True)
    with col3:
        rerank = st.checkbox("启用重排序", value=False)
    
    if st.button("提交问题", type="primary"):
        if question:
            with st.spinner("正在思考中..."):
                if use_stream:
                    # 流式输出模拟
                    result = backend.handle_stream_query(question, rerank, use_llm)
                    if result['success']:
                        # 模拟流式输出
                        answer = result.get('answer', '')
                        answer_placeholder = st.empty()
                        
                        full_answer = ""
                        words = answer.split()
                        for word in words:
                            full_answer += word + " "
                            answer_placeholder.markdown(full_answer)
                            time.sleep(0.05)
                        
                        # 添加到聊天历史
                        st.session_state.chat_history.append({
                            "question": question,
                            "answer": answer,
                            "timestamp": datetime.now().strftime("%H:%M:%S")
                        })
                        
                        # 显示搜索结果
                        results = result.get('results', [])
                        if results:
                            st.subheader("相关文档:")
                            for i, doc in enumerate(results[:5]):  # 显示前5个结果
                                with st.expander(f"文档 {i+1} (相似度: {doc.get('similarity', 0):.3f})"):
                                    st.write(f"**项目:** {doc.get('project_name', '未知')}")
                                    st.write(f"**内容:** {doc.get('text', '')}")
                    else:
                        st.error(f"查询失败: {result['error']}")
                else:
                    # 非流式输出
                    result = backend.ask_question(question)
                    if result['success']:
                        st.subheader("回答:")
                        st.write(result.get('answer', ''))
                        
                        # 添加到聊天历史
                        st.session_state.chat_history.append({
                            "question": question,
                            "answer": result.get('answer', ''),
                            "timestamp": datetime.now().strftime("%H:%M:%S")
                        })
                        
                        # 显示搜索结果
                        results = result.get('results', [])
                        if results:
                            st.subheader("相关文档:")
                            for i, doc in enumerate(results[:5]):
                                with st.expander(f"文档 {i+1} (相似度: {doc.get('similarity', 0):.3f})"):
                                    st.write(f"**项目:** {doc.get('project_name', '未知')}")
                                    st.write(f"**内容:** {doc.get('text', '')}")
                    else:
                        st.error(f"查询失败: {result['error']}")
        else:
            st.warning("请输入问题")
    
    # 显示聊天历史
    if st.session_state.chat_history:
        st.subheader("对话历史")
        for chat in reversed(st.session_state.chat_history[-5:]):  # 显示最近5条
            with st.expander(f"Q: {chat['question'][:50]}... ({chat['timestamp']})"):
                st.write(f"**问题:** {chat['question']}")
                st.write(f"**回答:** {chat['answer']}")

# 标签页2: 文档上传
with tab2:
    st.header("文档上传")
    
    with st.form("upload_form"):
        col1, col2 = st.columns(2)
        with col1:
            project_name = st.text_input("项目名称*", placeholder="请输入项目名称")
            project_date = st.date_input("项目日期")
        with col2:
            project_code = st.text_input("项目编号*", placeholder="请输入项目编号")
        
        uploaded_files = st.file_uploader(
            "选择Word文档(.docx)",
            type=['docx'],
            accept_multiple_files=True,
            help="支持多个.docx文件同时上传"
        )
        
        submitted = st.form_submit_button("上传文档", type="primary")
        if submitted:
            if project_name and project_code and uploaded_files:
                with st.spinner("正在上传和处理文档..."):
                    result = backend.upload_files(
                        uploaded_files, 
                        project_name, 
                        project_code, 
                        project_date.strftime("%Y-%m-%d") if project_date else ""
                    )
                    
                    if result['success']:
                        st.success(f"上传成功! {result['message']}")
                        st.json(result)
                    else:
                        st.error(f"上传失败: {result['error']}")
            else:
                st.warning("请填写所有必填字段(*)并选择文件")

# 标签页3: 项目管理
with tab3:
    st.header("项目管理")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button("刷新项目列表"):
            st.rerun()
    with col2:
        if st.button("获取筛选项目"):
            result = backend.get_filtered_projects()
            if not isinstance(result, dict) or 'error' not in result:
                st.success(f"找到 {len(result)} 个符合筛选条件的项目")
                st.write(result)
            else:
                st.error(f"获取失败: {result.get('error', '未知错误')}")
    
    # 获取项目列表
    with st.spinner("加载项目列表中..."):
        projects_result = backend.get_projects()
    
    if projects_result['success']:
        projects = projects_result.get('projects', [])
        st.subheader(f"项目列表 (共{len(projects)}个项目)")
        
        # 项目操作
        if projects:
            selected_projects = []
            for project in projects:
                col1, col2, col3 = st.columns([1, 3, 2])
                with col1:
                    if st.checkbox("", key=f"select_{project['name']}"):
                        selected_projects.append(project['name'])
                with col2:
                    st.write(f"**{project['name']}**")
                    st.write(f"文档数: {project.get('document_count', 0)}")
                with col3:
                    if st.button("查看详情", key=f"detail_{project['name']}"):
                        detail_result = backend.get_project_details(project['name'])
                        if 'name' in detail_result:
                            st.subheader(f"项目详情: {detail_result['name']}")
                            st.json(detail_result)
                        else:
                            st.error(f"获取详情失败: {detail_result.get('error', '未知错误')}")
            
            # 批量删除
            if selected_projects:
                st.warning(f"已选择 {len(selected_projects)} 个项目进行删除")
                if st.button("删除选中项目", type="secondary"):
                    result = backend.delete_projects(selected_projects)
                    if result['success']:
                        st.success(f"删除成功: {result['message']}")
                        st.rerun()
                    else:
                        st.error(f"删除失败: {result['error']}")
        else:
            st.info("暂无项目")
    else:
        st.error(f"加载项目失败: {projects_result['error']}")

# 标签页4: 系统配置
with tab4:
    st.header("系统配置")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("连接状态")
        if st.button("检查Zilliz连接"):
            result = backend.check_zilliz_connection()
            if result['connected']:
                st.success(result['message'])
            else:
                st.error(result['message'])
        
        if st.button("重新连接Zilliz"):
            result = backend.reconnect_zilliz()
            if result['success']:
                st.success(result['message'])
            else:
                st.error(result['message'])
    
    with col2:
        st.subheader("系统信息")
        config_result = backend.get_config()
        if config_result['success']:
            config_data = config_result['data']
            st.write(f"模型: {config_data['model_config'].get('model_name', '未知')}")
            st.write(f"集合: {config_data['zilliz_config'].get('collection_name', '未知')}")
    
    # 显示完整配置
    with st.expander("查看完整配置"):
        st.json(config_result)

# 页脚
st.markdown("---")
st.caption("向量搜索系统 v1.0.0 | 基于 Streamlit 构建")