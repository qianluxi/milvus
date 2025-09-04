// chat-module.js
class ChatModule {
    constructor(options = {}) {
        this.containerId = options.containerId || 'chat-module-container';
        this.apiBase = options.apiBase || '';
        this.streamMode = options.streamMode || false;
        
        this.init();
    }
    
    init() {
        this.createChatInterface();
        this.bindEvents();
    }
    
    createChatInterface() {
        const container = document.getElementById(this.containerId);
        if (!container) {
            console.error(`Container with ID ${this.containerId} not found`);
            return;
        }
        
        container.innerHTML = `
            <div class="chat-controls">
                <span>流式输出: </span>
                <label class="switch">
                    <input type="checkbox" id="chat-stream-mode" ${this.streamMode ? 'checked' : ''}>
                    <span class="slider"></span>
                </label>
                <span id="chat-model-status"><i class="fas fa-circle" style="color: #4CAF50;"></i> 模型已就绪</span>
            </div>
            
            <div class="chat-container" id="chat-messages-container">
                <div class="chat-message bot-message">
                    <div class="message-header">
                        <div class="chat-avatar bot-avatar"><i class="fas fa-robot"></i></div>
                        AI助手
                    </div>
                    <div class="message-content">
                        <div class="rich-text">
                            <p>您好！我是AI助手，请问有什么可以帮您的？</p>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="chat-input-area">
                <input type="text" class="chat-input" id="chat-question-input" placeholder="请输入您的问题..." autocomplete="off">
                <button class="chat-send-button" id="chat-send-button">
                    <i class="fas fa-paper-plane"></i>
                </button>
            </div>
        `;
        
        this.messagesContainer = document.getElementById('chat-messages-container');
        this.questionInput = document.getElementById('chat-question-input');
        this.sendButton = document.getElementById('chat-send-button');
        this.streamModeCheckbox = document.getElementById('chat-stream-mode');
        
        // 绑定事件
        this.questionInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.askQuestion();
            }
        });
        
        this.sendButton.addEventListener('click', () => {
            this.askQuestion();
        });
    }
    
    bindEvents() {
        // 可以添加其他事件绑定
    }
    
    askQuestion() {
        const question = this.questionInput.value.trim();
        if (!question) return;
        
        // 添加用户消息到聊天窗口
        this.addMessage(question, 'user');
        this.questionInput.value = '';
        
        // 检查是否启用流式模式
        if (this.streamModeCheckbox.checked) {
            this.askQuestionStream(question);
        } else {
            this.askQuestionNormal(question);
        }
    }
    
    askQuestionNormal(question) {
        // 显示加载提示
        const loadingId = this.addTypingIndicator();
        
        fetch(`${this.apiBase}/ask`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({question: question})
        })
        .then(response => response.json())
        .then(data => {
            // 移除输入指示器
            const indicator = document.getElementById(loadingId);
            if (indicator) indicator.remove();
            
            if (data.error) {
                this.addMessage('错误: ' + data.error, 'bot');
            } else {
                // 格式化响应内容
                const formattedResponse = this.formatResponse(data.answer);
                this.addMessage(formattedResponse, 'bot');
            }
        })
        .catch(error => {
            // 移除输入指示器
            const indicator = document.getElementById(loadingId);
            if (indicator) indicator.remove();
            
            this.addMessage('请求失败: ' + error, 'bot');
        });
    }
    
    askQuestionStream(question) {
        // 显示加载提示
        const loadingId = this.addTypingIndicator();
        
        // 使用EventSource进行流式响应
        const eventSource = new EventSource(`${this.apiBase}/ask_stream?question=${encodeURIComponent(question)}`);
        let fullResponse = '';
        let responseElement = null;
        
        eventSource.onmessage = (event) => {
            if (event.data === '[DONE]') {
                eventSource.close();
                
                // 格式化完整的响应内容
                if (responseElement) {
                    const contentDiv = responseElement.querySelector('.message-content');
                    if (contentDiv) {
                        contentDiv.innerHTML = this.formatResponse(fullResponse);
                    }
                }
                return;
            }
            
            const data = JSON.parse(event.data);
            
            if (data.error) {
                // 移除输入指示器
                const indicator = document.getElementById(loadingId);
                if (indicator) indicator.remove();
                
                this.addMessage('错误: ' + data.error, 'bot');
                eventSource.close();
            } else if (data.content) {
                // 如果是第一次收到内容，移除输入指示器并创建新的消息元素
                if (fullResponse === '') {
                    const indicator = document.getElementById(loadingId);
                    if (indicator) indicator.remove();
                    
                    responseElement = this.addMessage('', 'bot', true);
                }
                
                fullResponse += data.content;
                
                // 更新消息内容
                if (responseElement) {
                    const contentDiv = responseElement.querySelector('.message-content');
                    if (contentDiv) {
                        contentDiv.innerHTML = this.formatResponse(fullResponse);
                    }
                }
                
                // 滚动到底部
                this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
            }
        };
        
        eventSource.onerror = () => {
            eventSource.close();
            const indicator = document.getElementById(loadingId);
            if (indicator) indicator.remove();
            
            if (!fullResponse) {
                this.addMessage('流式请求中断或失败', 'bot');
            }
        };
    }
    
    addMessage(content, type, returnElement = false) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `chat-message ${type}-message`;
        
        const messageHeader = document.createElement('div');
        messageHeader.className = 'message-header';
        
        const avatar = document.createElement('div');
        avatar.className = `chat-avatar ${type}-avatar`;
        
        const messageContent = document.createElement('div');
        messageContent.className = 'message-content';
        
        if (type === 'user') {
            avatar.innerHTML = '<i class="fas fa-user"></i>';
            messageHeader.appendChild(document.createTextNode('用户'));
            messageHeader.appendChild(avatar);
            messageContent.textContent = content;
        } else {
            avatar.innerHTML = '<i class="fas fa-robot"></i>';
            messageHeader.appendChild(avatar);
            messageHeader.appendChild(document.createTextNode('AI助手'));
            messageContent.innerHTML = content;
        }
        
        messageDiv.appendChild(messageHeader);
        messageDiv.appendChild(messageContent);
        
        this.messagesContainer.appendChild(messageDiv);
        this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
        
        return returnElement ? messageDiv : null;
    }
    
    addTypingIndicator() {
        const messageDiv = document.createElement('div');
        messageDiv.className = 'chat-message bot-message';
        messageDiv.id = 'typing-' + Date.now();
        
        const messageHeader = document.createElement('div');
        messageHeader.className = 'message-header';
        
        const avatar = document.createElement('div');
        avatar.className = 'chat-avatar bot-avatar';
        avatar.innerHTML = '<i class="fas fa-robot"></i>';
        
        messageHeader.appendChild(avatar);
        messageHeader.appendChild(document.createTextNode('AI助手'));
        
        const messageContent = document.createElement('div');
        messageContent.className = 'message-content';
        
        const typingDiv = document.createElement('div');
        typingDiv.className = 'typing-indicator';
        typingDiv.innerHTML = `
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
            <div class="typing-dot"></div>
        `;
        
        messageContent.appendChild(typingDiv);
        messageDiv.appendChild(messageHeader);
        messageDiv.appendChild(messageContent);
        
        this.messagesContainer.appendChild(messageDiv);
        this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
        
        return messageDiv.id;
    }
    
    formatResponse(text) {
        // 1. 处理标题
        text = text.replace(/### (.*?)(?=\n|$)/g, '<h3>$1</h3>');
        text = text.replace(/## (.*?)(?=\n|$)/g, '<h2>$1</h2>');
        text = text.replace(/# (.*?)(?=\n|$)/g, '<h1>$1</h1>');
        
        // 2. 处理粗体和斜体
        text = text.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
        text = text.replace(/\*(.*?)\*/g, '<em>$1</em>');
        
        // 3. 处理代码块
        text = text.replace(/```(\w+)?\s([\s\S]*?)```/g, function(match, lang, code) {
            return `<pre><code>${code}</code></pre>`;
        });
        
        // 4. 处理内联代码
        text = text.replace(/`([^`]+)`/g, '<code>$1</code>');
        
        // 5. 处理无序列表
        text = text.replace(/^[-*] (.*$)/gm, '<li>$1</li>');
        text = text.replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>');
        
        // 6. 处理有序列表
        text = text.replace(/^\d+\. (.*$)/gm, '<li>$1</li>');
        text = text.replace(/(<li>.*<\/li>)/s, '<ol>$1</ol>');
        
        // 7. 处理引用
        text = text.replace(/^> (.*$)/gm, '<blockquote>$1</blockquote>');
        
        // 8. 处理表格
        text = text.replace(/\|(.+)\|\n\|([\-:| ]+)+\|\n((?:\|.*\|\n)+)/g, function(match, header, align, rows) {
            let tableHtml = '<table><thead><tr>';
            
            // 处理表头
            header.split('|').forEach(cell => {
                if (cell.trim()) tableHtml += `<th>${cell.trim()}</th>`;
            });
            tableHtml += '</tr></thead><tbody>';
            
            // 处理行
            rows.split('\n').forEach(row => {
                if (row.trim()) {
                    tableHtml += '<tr>';
                    row.split('|').forEach(cell => {
                        if (cell.trim()) tableHtml += `<td>${cell.trim()}</td>`;
                    });
                    tableHtml += '</tr>';
                }
            });
            
            tableHtml += '</tbody></table>';
            return tableHtml;
        });
        
        // 9. 处理换行
        text = text.replace(/\n/g, '<br>');
        
        // 10. 添加序号格式化
        text = text.replace(/(\d+)\./g, '<span class="number">$1.</span>');
        
        return `<div class="rich-text">${text}</div>`;
    }
}

// 全局可用
window.ChatModule = ChatModule;