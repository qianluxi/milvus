import streamlit as st
import time
from datetime import datetime
from search_backend import SearchBackend

# ----------------------------
# 页面配置
# ----------------------------
st.set_page_config(
    page_title="智能向量检索系统",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ----------------------------
# 初始化后端
# ----------------------------
@st.cache_resource
def get_backend():
    return SearchBackend()

backend = get_backend()

# ----------------------------
# 初始化 Session 状态
# ----------------------------
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'current_constraints' not in st.session_state:
    st.session_state.current_constraints = []
if 'active_projects' not in st.session_state:
    st.session_state.active_projects = []

# ----------------------------
# 侧边栏
# ----------------------------
with st.sidebar:
    st.title("🔧 系统控制台")

    # 系统状态
    st.subheader("🩺 健康检查")
    if st.button("检查连接状态"):
        health_status = backend.health_check()
        zilliz_status = backend.check_zilliz_connection()
        st.success(f"API: {health_status['status']}")
        if zilliz_status['connected']:
            st.success(f"Zilliz: {zilliz_status['message']}")
        else:
            st.error(f"Zilliz: {zilliz_status['message']}")

    # ----------------------------
    # 筛选条件部分
    # ----------------------------
    st.subheader("📑 筛选条件管理")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("清除筛选"):
            result = backend.clear_constraints()
            if result['success']:
                st.session_state.current_constraints = []
                st.session_state.active_projects = []
                st.success("✅ 已清除所有筛选条件")
                st.rerun()
            else:
                st.error(result['error'])
    with col2:
        if st.button("刷新条件"):
            result = backend.get_context()
            if result['success']:
                st.session_state.current_constraints = result['context'].get('constraints', [])
                # 同时刷新当前激活项目
                filtered = backend.get_filtered_projects()
                if isinstance(filtered, list):
                    st.session_state.active_projects = [p["display_name"] for p in filtered]
                    st.info(f"当前激活项目：{', '.join(st.session_state.active_projects) or '无'}")
                st.success("🔄 已刷新筛选条件")

    # 显示当前筛选条件
    if st.session_state.current_constraints:
        st.write("当前筛选条件:")
        for i, c in enumerate(st.session_state.current_constraints):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.text(f"{c['type']}: {c['value']}")
            with col2:
                if st.button("❌ 删除", key=f"del_{i}"):
                    backend.remove_constraint(i)
                    st.rerun()

    # ----------------------------
    # 添加筛选条件
    # ----------------------------
    st.subheader("➕ 添加筛选")
    constraint_type = st.selectbox("条件类型", ["project_name", "project_date", "project_code"])
    constraint_value = st.text_input("条件值", placeholder="请输入筛选值")

    if st.button("添加筛选条件"):
        if constraint_value.strip():
            # ✅ 新逻辑：追加筛选而不是清除再添加
            backend.add_constraint(constraint_type, constraint_value)
            result = backend.get_context()
            if result['success']:
                st.session_state.current_constraints = result['context'].get('constraints', [])
                st.success("✅ 已添加筛选条件")
                st.rerun()
        else:
            st.warning("请输入筛选条件值")

# ----------------------------
# 主页面布局
# ----------------------------
st.title("🔍 智能文档向量检索系统")

tab1, tab2, tab3, tab4 = st.tabs(["💬 智能问答", "📤 文档上传", "📂 项目管理", "⚙️ 系统配置"])

# ===================================================
# 💬 智能问答
# ===================================================
with tab1:
    st.header("💬 智能问答")

    # 聊天历史（气泡样式）
    for chat in st.session_state.chat_history[-10:]:
        with st.container():
            if chat["role"] == "user":
                st.markdown(
                    f"""
                    <div style='text-align:right;background-color:#DCF8C6;color:#003366;
                    padding:8px 12px;border-radius:10px;margin:4px 0;'>
                    🧑‍💻 {chat['content']}
                    <div style='font-size:12px;color:#555555;'>{chat['timestamp']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"""
                    <div style='text-align:left;background-color:#F1F0F0;color:#333333;
                    padding:8px 12px;border-radius:10px;margin:4px 0;'>
                    🤖 {chat['content']}
                    <div style='font-size:12px;color:#666666;'>{chat['timestamp']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # 用户输入
    user_input = st.text_area("请输入您的问题：", height=100, placeholder="在这里输入您的问题...")
    col1, col2 = st.columns([1, 1])
    with col1:
        use_llm = st.checkbox("使用AI回答", value=True)
    with col2:
        rerank = st.checkbox("启用重排序", value=False)

    if st.button("🚀 发送问题"):
        if user_input.strip():
            st.session_state.chat_history.append({
                "role": "user",
                "content": user_input,
                "timestamp": datetime.now().strftime("%H:%M:%S")
            })

            with st.spinner("AI 正在思考中..."):
                result = backend.handle_stream_query(user_input, rerank, use_llm)

            if result['success']:
                answer = result.get('answer', '抱歉，未能生成回答')
                st.session_state.chat_history.append({
                    "role": "bot",
                    "content": answer,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                })
                st.rerun()
            else:
                st.error(f"查询失败: {result['error']}")
        else:
            st.warning("请输入您的问题！")

# ===================================================
# 📤 文档上传
# ===================================================
with tab2:
    st.header("📤 上传项目文档")

    with st.form("upload_form"):
        col1, col2 = st.columns(2)
        with col1:
            project_name = st.text_input("项目名称*", placeholder="请输入项目名称")
            # 使用文本输入代替日期选择器
            project_date_input = st.text_input(
                "项目日期*", 
                placeholder="YYYY-MM-DD",
                value=datetime.now().strftime("%Y-%m-%d")  # 默认今天
            )
        with col2:
            project_code = st.text_input("项目编号*", placeholder="请输入项目编号")

        uploaded_files = st.file_uploader("选择Word文档(.docx)", type=['docx'], accept_multiple_files=True)
        submitted = st.form_submit_button("上传")

        if submitted:
            if project_name and project_code and uploaded_files:
                # 验证日期格式
                if project_date_input:
                    try:
                        datetime.strptime(project_date_input, "%Y-%m-%d")
                    except ValueError:
                        st.error("❌ 日期格式无效，请使用 YYYY-MM-DD 格式")
                        st.stop()
                
                with st.spinner("正在上传并向Zilliz写入..."):
                    result = backend.upload_files(
                        uploaded_files,
                        project_name,
                        project_code,
                        project_date_input  # 直接使用文本输入
                    )
                if result['success']:
                    st.success(f"✅ 上传成功: {result['message']}")
                    # 可选：显示详细信息
                    with st.expander("查看上传详情"):
                        st.json(result)
                else:
                    st.error(result['error'])
            else:
                st.warning("请填写项目名称、编号并选择文件")

# ===================================================
# 📂 项目管理
# ===================================================
with tab3:
    st.header("📂 项目管理")
    
    # 刷新按钮
    if st.button("🔄 刷新项目列表"):
        st.rerun()

    with st.spinner("加载项目中..."):
        projects_result = backend.get_projects()

    if projects_result['success']:
        projects = projects_result['projects']
        st.write(f"共 {len(projects)} 个项目")
        
        # ----------------------------
        # 项目搜索和删除功能
        # ----------------------------
        st.subheader("🗑️ 删除项目")
        
        # 创建搜索选项
        search_options = []
        for p in projects:
            # 创建搜索字符串：项目名 + 项目编号
            search_string = f"{p['name']} | {p['project_code']}"
            search_options.append({
                "display": search_string,
                "name": p['name'],
                "code": p['project_code'],
                "date": p['project_date'],
                "doc_count": p['document_count']
            })
        
        # 搜索框
        search_term = st.text_input("🔍 搜索项目名或项目编号:", placeholder="输入项目名或编号进行搜索...")
        
        # 筛选项目
        filtered_projects = []
        if search_term:
            filtered_projects = [
                p for p in search_options 
                if search_term.lower() in p['name'].lower() or search_term.lower() in p['code'].lower()
            ]
        else:
            filtered_projects = search_options
        
        if filtered_projects:
            # 显示搜索结果
            st.write(f"找到 {len(filtered_projects)} 个项目:")
            
            # 创建多选列表
            project_options = {
                f"{p['display']} (日期: {p['date']}, 文档: {p['doc_count']})": p['name'] 
                for p in filtered_projects
            }
            
            selected_projects_for_deletion = st.multiselect(
                "选择要删除的项目:",
                options=list(project_options.keys()),
                help="⚠️ 注意：删除操作不可逆，将永久删除该项目在Zilliz中的所有数据"
            )
            
            # 确认删除
            if selected_projects_for_deletion:
                st.warning("⚠️ 即将删除以下项目，此操作不可逆！")
                for project_display in selected_projects_for_deletion:
                    st.write(f"- {project_display}")
                
                # 安全确认
                col1, col2 = st.columns(2)
                with col1:
                    confirm_delete = st.checkbox("我确认要删除这些项目")
                with col2:
                    if confirm_delete:
                        if st.button("🔥 确认删除", type="primary"):
                            with st.spinner("删除中..."):
                                # 获取实际的项目名称
                                project_names_to_delete = [project_options[p] for p in selected_projects_for_deletion]
                                delete_result = backend.delete_projects(project_names_to_delete)
                                
                            if delete_result['success']:
                                st.success(f"✅ 成功删除 {len(project_names_to_delete)} 个项目")
                                st.info(f"删除记录数: {delete_result.get('deleted_count', '未知')}")
                                # 刷新页面
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error(f"❌ 删除失败: {delete_result.get('error', delete_result.get('message', '未知错误'))}")
        else:
            if search_term:
                st.info("🔍 没有找到匹配的项目")
            else:
                st.info("📝 在搜索框中输入项目名或编号来搜索要删除的项目")
        
        # ----------------------------
        # 简化的项目列表展示
        # ----------------------------
        st.subheader("📋 项目列表")
        
        # 使用表格形式展示项目信息
        if projects:
            # 创建简化的数据表格
            project_data = []
            for p in projects:
                project_data.append({
                    "项目名称": p['name'],
                    "项目编号": p['project_code'],
                    "项目日期": p['project_date'],
                    "文档数量": p['document_count']
                })
            
            # 显示表格
            st.dataframe(
                project_data,
                use_container_width=True,
                hide_index=True
            )
            
            # 计算总文档数 - 修复类型错误
            try:
                total_docs = sum(p['document_count'] for p in projects)
                st.caption(f"📊 统计: 共 {len(projects)} 个项目，{total_docs} 个文档片段")
            except TypeError:
                # 如果还有类型错误，使用更安全的方法
                total_docs = 0
                for p in projects:
                    try:
                        total_docs += int(p['document_count'])
                    except (TypeError, ValueError):
                        continue
                st.caption(f"📊 统计: 共 {len(projects)} 个项目，约 {total_docs} 个文档片段")
            
        else:
            st.info("暂无项目数据")
            
    else:
        st.error(projects_result["error"])
# ===================================================
# ⚙️ 系统配置
# ===================================================
with tab4:
    st.header("⚙️ 系统配置")
    config_result = backend.get_config()
    if config_result['success']:
        st.json(config_result['data'])
    else:
        st.error(config_result['error'])

st.markdown("---")
st.caption("🧭 Vector Search System v2.0 | Streamlit 前端 by ModelScope")
