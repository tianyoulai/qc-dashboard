# 📊 质检数据统一看板 (QC Dashboard)

> **一句话说明**：把散落在 6 个企业微信文档里的质检数据，自动汇聚到一个可视化看板上。

## 项目身份（2026-04-17 校准）

- **当前线上地址**：`https://f4qv5p8uuurnb369isiudq.streamlit.app/`
- **项目定位**：这是 **TAO 看板** 当前本地项目。
- **本地定时任务**：`com.qc-dashboard.daily-push`（09:15 推送）、`com.qc-dashboard.auto-refresh`（18:25 刷新）
- **易混淆的兄弟项目**：评论质量看板在 `/Users/laitianyou/WorkBuddy/20260326191218/`，线上地址是 `https://quality-dashboard-2026.streamlit.app/`
- **特别说明**：本目录内的 `jobs/daily_report.py`、`scripts/push_comment.py`、`config/report_pmt.md` 属于评论质量日报残留文件，**不属于 TAO 的日常 push / refresh 任务链**。

---

## 🎯 这是什么？

你每天需要在多个企微文档里翻来覆去查看各队列的质检指标——漏过率、误伤率、准确率……  
**这个工具帮你把所有数据集中到一个页面上**，按日期筛选、按队列切换、看趋势图、导出数据，一键搞定。

### 能做什么？

| 功能 | 说明 |
|------|------|
| 🔄 **多队列统一查看** | 6 个队列的数据在一个页面切换查看 |
| 📅 **日期范围筛选** | 看任意时间段的数据（日/周/月一键切换） |
| 📈 **趋势图表** | 折线图、饼图、柱状图、雷达图，自动生成 |
| 📋 **数据明细表** | 可排序、可搜索、可导出 CSV |
| ⚡ **每日自动刷新** | 配置一次后每天自动拉取新数据 |
| 💾 **本地存储** | 数据保存在本地 SQLite，无需服务器 |

---

## 🚀 快速开始（3 步）

### 第 1 步：下载 & 安装

```bash
# 1. 下载项目到你的电脑
git clone <项目地址> qc-dashboard
cd qc-dashboard

# 2. 一键安装
sh install.sh
```

安装脚本会自动：
- ✅ 检查 Python 环境（需要 Python 3.8+）
- ✅ 安装必要的依赖库（openpyxl、pyyaml 等）
- ✅ 创建数据库和目录结构
- ✅ （可选）安装 wecom-cli 企业微信命令行工具

> **⚠️ 如果没有 Python？**
> macOS 自带。打开终端输入 `python3 --version` 查看版本。
> 版本低于 3.8 请运行 `brew install python` 升级。

### 第 2 步：配置凭证（⚠️ 最重要的一步）

编辑 `config.yaml` 文件，找到最顶部的 **凭证区域**：

```bash
# 用任何文本编辑器打开
vim config.yaml        # 或用 VS Code / 记事本 打开
```

你需要填写 **Bot ID** 和 **Secret** 两项：

```yaml
global:
  collector_mode: wcom-api          # 使用 API 全自动模式

  wcom_api:
    bot_id: ""      # ← 在这里填入你的机器人 ID（AgentId）
    secret: ""       # ← 在这里填入你的应用密钥（Secret）
```

> **如何获取？**
> 1. 登录企微管理后台 → https://work.weixin.qq.com/wadmin
> 2. 应用管理 → 找到或创建一个「**智能机器人**」类型的应用
> 3. 复制页面上显示的 **AgentId** 和 **Secret**
> 4. 粘贴到上面的 `bot_id` 和 `secret` 字段中
>
> 📖 详细教程：https://open.work.weixin.qq.com/help2/pc/cat?doc_id=21677

> **💡 同事拿到项目后怎么换凭证？**
> 只需修改上面这两行！把 `bot_id` 和 `secret` 换成自己的就行，其他都不用动。

### 第 3 步：初始化 & 拉取数据

```bash
# 1️⃣ 初始化 wecom-cli（首次必须运行一次）
python3 src/collector.py setup
# 按提示完成交互式认证（可能需要扫码）

# 2️⃣ 拉取所有队列的数据
python3 src/collector.py fetch

# 3️⃣ 打开看板
open dashboard/index.html        # macOS
# 或双击 dashboard/index.html     # Windows / Linux
```

---

## 📖 使用指南

### 每日操作流程

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────┐
│ 1. 导入数据 │ → │ 2. 打开看板查看  │ → │ 3. 分析/导出 │
│ import/fetch│    │ index.html       │    │ CSV/截图     │
└─────────────┘    └──────────────────┘    └──────────────┘
```

如果设置了定时任务，第 1 步可以省略——**每天 18:25 自动刷新数据，次日 09:15 独立推送日报**。

### 常用命令速查

| 命令 | 作用 |
|------|------|
| `python3 src/collector.py setup` | **首次必须**：初始化 wecom-cli 凭证认证 |
| `python3 src/collector.py fetch` | 通过 wecom-cli API 自动拉取所有队列数据 |
| `python3 src/collector.py fetch --queue q6_shangqiang` | 只拉取某个队列 |
| `python3 src/collector.py import` | 从 Excel 导入数据（备选方案） |
| `python3 src/collector.py live-sync --queue q3_erjian_4qi_gt` | 直读企微网页内置计算结果，适合 q3/q3b 这类公式队列兜底 |
| `python3 src/collector.py status` | 查看各队列数据状态 + wecom-cli 连通性 |
| `python3 src/collector.py export-json` | 导出全部数据为 JSON |
| `python3 src/collector.py export-html` | 导出带数据的独立 HTML 文件 |

### 看板操作

- **切换队列**：点击顶部的队列标签卡片（📢投放误漏 / 📋简单二审 ...）
- **日期筛选**：右上角选择日期范围
- **日/周/月切换**：快速选择最近一天 / 一周 / 一月
- **重置筛选**：点击 ↺ 按钮
- **表格排序**：点击表头排序
- **导出数据**：点击 📥 导出 CSV 按钮

---

## 📁 项目结构

```
qc-dashboard/
├── install.sh              ← 一键安装脚本（运行这个）
├── config.yaml             ← 队列配置（编辑这个）
├── README.md               ← 你正在看的说明书
├── dashboard/
│   └── index.html          ← 🎯 看板页面（双击打开）
├── src/
│   └── collector.py        ← 数据采集引擎（不需要改）
├── data/
│   ├── metrics.db          ← SQLite 数据库（自动创建）
│   ├── uploads/            ← 放 Excel 的目录
│   │   └── processed/      ← 已处理的 Excel 会移到这里
│   └── backups/            ← 数据备份
└── requirements.txt        ← Python 依赖列表
```

---

## ❓ 常见问题

### Q: 导入 Excel 后看板没变化？
1. 确认文件放在了 `data/uploads/` 目录
2. 确认文件名包含队列名（如"投放"、"上墙"等），方便自动匹配
3. 运行 `python3 src/collector.py status` 检查是否有数据入库

### Q: 我的数据不在 6 个预设队列里？
编辑 `config.yaml`，按照现有格式新增一个队列即可：
```yaml
queues:
  - id: "q7_my_queue"
    name: "我的队列"
    full_name: "【我的业务】XXX"
    doc_url: "https://doc.weixin.qq.com/sheet/..."
    ...
```

### Q: 如何设置每天自动刷新？
运行安装脚本时会提示你是否设置定时任务。
也可以手动添加：
```bash
# macOS / Linux（仅手动维护时使用；当前默认推荐 launchd）
crontab -e
# 加入这行（每天 18:25 刷新数据）:
25 18 * * * cd /你的路径/qc-dashboard && python3 src/collector.py fetch >> data/cron.log 2>&1
```

> 当前 TAO 默认调度口径：**18:25 刷新数据，次日 09:15 独立推送日报**。

### Q: 数据安全吗？
✅ 所有数据保存在你本地电脑的 SQLite 文件中  
✅ 不上传到任何服务器  
✅ 不需要联网（除非使用 API 或 CDP 模式）

### Q: 可以分享给别人吗？
可以！整个文件夹打包发给同事，对方只需：

1. 解压到任意目录
2. 运行 `sh install.sh`（一键安装依赖）
3. **编辑 `config.yaml`，把 `bot_id` 和 `secret` 改成自己的**（如果用自己的机器人）
4. 运行 `python3 src/collector.py setup`（完成一次认证）
5. 运行 `python3 src/collector.py fetch`（拉取数据）
6. 打开 `dashboard/index.html`

> **不想配置凭证？** 也可以用 Excel 导入模式：改 `collector_mode: excel` → 把企微导出的 xlsx 放进 `data/uploads/` → `python3 src/collector.py import`

---

## 🔧 进阶用法

### 切换数据采集模式

在 `config.yaml` 顶部修改 `global.collector_mode`：

| 模式 | 说明 | 需要什么 |
|------|------|---------|
| `wcom-api` | **通过 wecom-cli API 全自动拉取（推荐）** | Bot ID + Secret（一次性配置） |
| `excel` | 手动导 Excel → 自动解析 | 最简单，零门槛 |
| `cdp` | 浏览器自动化抓取 | Chrome 保持登录 |

### 自定义指标字段

每个队列的 `fields.metrics` 定义了要提取哪些列：
```yaml
metrics:
  - key: violation_rate     # 内部标识符（英文）
    label: "违规准确率"      # 显示名称
    col: "E"                # Excel/表格中的列号
```

想增加指标？直接加一行就行。

### 多人协作方案

如果团队多人使用：
1. 把 `config.yaml` 和 `data/metrics.db` 放在共享网盘（如 SVN/Git）
2. 每个人各自维护自己的 Excel 导入
3. 或者由一个人负责采集，其他人只读看板

---

## 📊 当前接入的队列

| # | 队列名 | 文档 | 子表 | 核心指标 | 状态 |
|---|--------|------|------|---------|------|
| 1 | 📢 投放误漏 | 供应商-投放误漏case | 量级登记 | 违规准确率、漏率 | 待接入 |
| 2 | 📋 简单二审 | 供应商-简单二审误漏 | 量级登记 | 违规准确率、漏率 | 待接入 |
| 3 | 🔄 四期-二审 | 二审周推质检分歧单 | 云雀四期结算 | 违规准确率、漏率（2个子表） | 待接入 |
| 4 | 🚨 四期-举报 | 举报周推质检分歧单 | 云雀四期结算 | 申诉前后对比(6个指标) | 待接入 |
| 5 | 🚫 拉黑误漏 | 供应商-拉黑误漏case | 量级登记 | 违规准确率、漏率 | 待接入 |
| 6 | 📝 上墙文本 | 上墙文本申诉-云雀 | 准确率（新） | 审核准确率 + 7项明细 | ✅ 已有数据 |

---

*最后更新：2026-04-08*
