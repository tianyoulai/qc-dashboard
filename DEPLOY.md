# QC Dashboard — Streamlit Cloud 部署指南

## 📦 GitHub 仓库

**https://github.com/tianyoulai/qc-dashboard**

## 🚀 部署步骤（2分钟）

1. 打开 **[Streamlit Cloud](https://share.streamlit.io/)**
2. 点击 **"New App"**
3. 填写：
   - Repository: `tianouali / qc-dashboard`
   - Branch: `main`
   - Main file path: `streamlit_app.py`
4. 点 **Deploy** → 等待 30-60 秒

部署完成后你会得到一个公开 URL（如 `xxx.streamlit.app`），发给同事即可。

## 🔁 每日更新数据后

在本地跑：
```bash
cd qc-dashboard && ./refresh.sh
git add data/metrics.db dashboard/index.html
git commit -m "data: $(date +%Y-%m-%d) 更新"
git push
```

Streamlit Cloud 会自动重新拉取最新数据。如需立即刷新，在 Cloud 控制台点 **Reboot**。

## 📱 同事使用

- 浏览器打开链接，无需安装任何东西
- 支持手机/平板查看
