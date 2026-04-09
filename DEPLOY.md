# QC Dashboard — 部署到 Streamlit Cloud（免费公网访问）

## 同事使用方式
部署成功后，同事通过一个 **公网链接** 直接在浏览器打开，无需安装任何东西。

---

## 一、准备 GitHub 仓库

### 1. 创建仓库（如果还没有）
```bash
cd qc-dashboard

# 初始化 git（如果是新仓库）
git init
```

### 2. 确认以下文件都在
```
qc-dashboard/
├── streamlit_app.py        # 主应用
├── requirements.txt        # Python 依赖
├── config.yaml             # 队列配置
├── data/metrics.db         # 数据库 ⚠️ 必须提交
└── .streamlit/config.toml  # Streamlit 配置
```

### 3. 提交并推送
```bash
git add streamlit_app.py requirements.txt config.yaml data/metrics.db .streamlit/config.toml
git commit -m "feat: QC Dashboard v1 — 7队列质检数据看板"
git branch -M main
git remote add origin https://github.com/你的用户名/qc-dashboard.git
git push -u origin main
```

> ⚠️ **`data/metrics.db` 必须提交**——Streamlit Cloud 从 Git 拉代码时需要这个数据库文件。

---

## 二、连接 Streamlit Cloud

### 第1步：注册
1. 打开 [share.streamlit.io](https://share.streamlit.io)
2. 用 GitHub 账号登录（免费）

### 第2步：创建应用
1. 点击 **"Create app"**
2. 填写：

| 设置项 | 填写 |
|--------|------|
| Repository | 选择你的 `qc-dashboard` 仓库 |
| Branch | `main` |
| Main file path | `streamlit_app.py` |
| Python version | 推荐 **3.11** 或 3.12 |

3. 点击 **"Deploy!"**
4. 等待 1~2 分钟构建完成

### 第3步：拿到链接
部署成功后你会得到类似：
```
https://xxxxx-qc-dashboard-app-xxxxx.streamlit.app
```
👉 **这就是发给同事的链接**

---

## 三、日常更新数据

每次跑完 `refresh.sh` 后，更新线上看板只需两步：

```bash
cd qc-dashboard
git add data/metrics.db
git commit -m "data: update $(date +%Y-%m-%d)"
git push
```

推送后 Streamlit Cloud 会自动重新部署（约 1 分钟）。

> 💡 **进阶**：后续可以接 GitHub Actions 实现定时自动拉取+更新。

---

## 四、常见问题

| 问题 | 解决 |
|------|------|
| 页面空白/报错 | 检查 Logs → 通常缺文件或路径错误 |
| 数据没更新 | 确认 `metrics.db` 已 push 到 main 分支 |
| 访问太慢 | 免费版有冷启动（首次约 10 秒） |
| 想限制访问 | Streamlit Cloud 不支持密码 → 可用 nginx 反代加鉴权 |

---

## 架构说明

```
同事浏览器 ──→ Streamlit Cloud (免费) ──→ Git Repo ──→ streamlit_app.py + metrics.db
                                                              ↓
                                                         Plotly 图表渲染
```
