#!/usr/bin/env python3
"""
auto_download.py — 方案2: 企微表格自动下载 + 解析入库 + 更新看板

完整链路:
  1. Playwright 打开企微表格页面（持久化登录态，首次需扫码）
  2. 点击「下载/导出」按钮下载 xlsx 文件
  3. 将 xlsx 移动到 data/uploads/
  4. 调用 collector.py import 解析入库
  5. 调用 collector.py export-html 生成带数据的看板

用法:
  # 首次运行: 会打开浏览器让你扫码登录企微
  python3 src/auto_download.py --login

  # 日常运行 (登录态已保存)
  python3 src/auto_download.py
  
  # 只下载指定队列
  python3 src/auto_download.py --queue q6_shangqiang
  
  # 下载后不自动解析入库 (只下载)
  python3 src/auto_download.py --download-only

依赖:
  pip install playwright && playwright install chromium
"""

import asyncio
import os
import sys
import json
import shutil
import time
from pathlib import Path
from datetime import datetime

# ── 项目路径 ──
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
DOWNLOADS_DIR = DATA_DIR / "downloads"
BROWSER_DATA = DATA_DIR / "browser_profile"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"

# ── 日志 ──
def log(msg, level="INFO"):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"  [{now}] [{level}] {msg}")


# ============================================================
# 配置加载
# ============================================================
def load_config():
    import yaml
    if not CONFIG_FILE.exists():
        log(f"配置文件不存在: {CONFIG_FILE}", "ERROR")
        sys.exit(1)
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_queue_config(config, queue_id=None):
    """获取队列配置列表"""
    queues = config.get('queues', [])
    if queue_id:
        queues = [q for q in queues if q.get('id') == queue_id]
        if not queues:
            log(f"找不到队列: {queue_id}", "ERROR")
            sys.exit(1)
    return queues


# ============================================================
# 核心逻辑: Playwright 自动下载
# ============================================================
async def download_queues(queue_configs, headless=False, force_login=False):
    """
    用 Playwright 打开企微表格并逐个下载 Excel。
    
    返回: 成功下载的文件路径列表
    """
    from playwright.async_api import async_playwright
    
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    
    log(f"启动浏览器 (数据目录: {BROWSER_DATA})")
    
    downloaded_files = []
    
    async with async_playwright() as p:
        # 使用持久化上下文保存登录态
        context = await p.chromium.launch_persistent_context(
            str(BROWSER_DATA),
            headless=headless,
            channel="chrome",
            accept_downloads=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-size=1400,900",
            ],
            viewport={"width": 1400, "height": 900},
            # 设置默认下载目录
            downloads_path=str(DOWNLOADS_DIR),
        )

        page = None
        if context.pages:
            page = context.pages[0]
        else:
            page = await context.new_page()

        # 检查是否需要登录 & 立即导航到目标页面
        if not queue_configs:
            log("没有队列配置", "ERROR")
            await context.close()
            return []

        # 取第一个队列的URL作为起始页面（触发登录）
        first_url = queue_configs[0].get('doc_url', '')
        first_qname = queue_configs[0].get('name', '第一个队列')
        
        log(f"正在打开: [{first_qname}]")
        log(f"  URL: {first_url[:80]}...")
        
        try:
            # 直接导航到目标URL — 如果没登录，企微会自动跳到扫码页
            await page.goto(first_url, wait_until="domcontentloaded", timeout=30_000)
        except Exception as e:
            log(f"初始导航: {e} (继续等待...)", "WARN")

        # 检测是否需要扫码登录
        current_url = page.url
        await asyncio.sleep(2)
        
        # 判断条件：不在目标文档域名 → 需要登录
        needs_auth = not ('doc.weixin.qq.com' in current_url or 'docs.qq.com' in current_url)
        
        if needs_auth or force_login:
            log("\n" + "─" * 50)
            log("  🔐 请在浏览器窗口中操作:")
            log("     1. 如果出现企微登录页 → 扫码登录")
            log("     2. 登录成功后会自动跳转到表格页面")
            log("     3. 无需任何其他操作，脚本会自动继续")
            log("─" * 50)

            # 等待登录完成：检测URL进入文档域
            try:
                await page.wait_for_function(
                    """() => {
                        const url = window.location.href;
                        return url.includes('doc.weixin.qq.com') || url.includes('docs.qq.com');
                    }""",
                    timeout=180_000  # 给3分钟扫码
                )
                log("✅ 检测到登录成功！页面已跳转到文档", "SUCCESS")
                await asyncio.sleep(3)  # 等表格渲染
            except Exception:
                log("⏰ 等待登录超时(3分钟)，尝试继续...", "WARN")

        # 逐个队列下载（去重：相同 doc_url 只下载一次）
        visited_urls = {}  # url_key → 下载文件路径
        for qcfg in queue_configs:
            qid = qcfg['id']
            qname = qcfg.get('name', '')
            doc_url = qcfg.get('doc_url', '')
            
            if not doc_url:
                log(f"[{qname}] 缺少 doc_url，跳过", "WARN")
                continue
            
            # 去重：同一个表格只下载一次（如 q3 和 q3b 共享同一 doc_url）
            url_key = doc_url.split('?')[0]  # 去掉查询参数作为唯一标识
            if url_key in visited_urls:
                log(f"[{qname}] 同一文档已下载，复用文件", "INFO")
                prev_file = visited_urls[url_key]
                if prev_file and os.path.exists(prev_file):
                    downloaded_files.append(prev_file)
                continue
            
            log(f"\n{'='*50}")
            log(f"开始下载: [{qname}] ({qid})")
            log(f"URL: {doc_url[:80]}...")
            
            # 导航到目标页面
            try:
                await page.goto(doc_url, wait_until="networkidle", timeout=60_000)
                await asyncio.sleep(3)  # 等待表格渲染完成 (Canvas虚拟渲染需要时间)
            except Exception as e:
                log(f"页面加载失败: {e}", "ERROR")
                continue

            # 截图记录当前状态 (调试用)
            debug_img = DATA_DIR / f"_debug_{qid}.png"
            await page.screenshot(path=str(debug_img), full_page=False)
            log(f"页面截图已保存: {debug_img}")

            # 尝试点击下载/导出按钮
            dl_file = await click_and_download(page, qid, qname)
            
            if dl_file:
                # 移动到 uploads 目录
                dest_name = f"{qname}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                dest_path = UPLOAD_DIR / dest_name
                shutil.move(str(dl_file), str(dest_path))
                log(f"✅ 已保存: {dest_path}", "SUCCESS")
                downloaded_files.append(str(dest_path))
                # 记录 URL → 文件映射，供去重复用
                visited_urls[url_key] = str(dest_path)
            else:
                log(f"⚠️ [{qname}] 下载失败", "WARN")

        # 不关闭浏览器，保留登录态
        log("\n浏览器保持打开 (登录态已保存)")
        
    return downloaded_files


async def click_and_download(page, queue_id, queue_name):
    """
    在企微表格页面上找到并点击「下载/导出」按钮。
    
    基于截图验证的实际 UI 结构 (2026-04-08):
      右上角有一个下拉菜单 (文档标题旁的 ▼ 按钮)
      菜单内容包含: 创建者、创建时间、添加到...、版本历史记录、
                    浏览记录、同步的内容、保存为模板、生成智能表格、
                    生成副本、导出 →、打印、更多...、退出文档
      其中「导出」有子菜单箭头 → 点击后出现子选项
    
    策略优先级:
      1. 直接用 JS 文本匹配点击「导出」
      2. 尝试各种可能打开菜单的按钮
      3. 键盘快捷键兜底
    
    返回: 下载文件的路径，或 None
    """
    from playwright.async_api import TimeoutError as PWTimeout

    # ── 先截图记录初始状态 ──
    debug_img = DATA_DIR / f"_debug_{queue_id}_before.png"
    await page.screenshot(path=str(debug_img), full_page=False)

    # ── Step 1: 先尝试直接找页面上已有的「导出」文字（菜单可能已经开着）──
    log("  [Step 1] 检查页面上是否有可见的「导出」选项...")
    
    export_clicked = await try_click_export_menu_item(page, queue_name)
    if export_clicked:
        return export_clicked

    # ── Step 2: 打开右上角菜单（文档操作菜单）──
    log("  [Step 2] 尝试打开文档操作菜单...")
    
    # 从截图分析，菜单是通过以下按钮之一触发的:
    menu_button_selectors = [
        # 文档标题旁边的下拉按钮 (最可能)
        '.mini-list-dropdown-btn',
        '[aria-label="按钮:文档列表下拉"]',
        # 右上角工具栏区域的文件/菜单按钮
        '[aria-label="按钮:文件操作"]',
        'button[aria-label*="文件"]',
        'button[aria-label*="菜单"]',
        'button[aria-label*="操作"]',
        # 三点/更多按钮
        '[aria-label="更多"]',
        'button[class*="more"]',
        'button[class*="menu"]',
        # 标题栏右侧的下拉箭头
        '.sheet-header [class*="dropdown"]',
        '.header-bar [class*="dropdown"]',
        '[class*="doc-title"] + button',
        '[class*="title"] [class*="arrow"]',
        # 通用: 任何包含 ▼ 或 "更多" 的可见按钮
    ]
    
    for sel in menu_button_selectors:
        try:
            el = page.locator(sel).first
            if await el.is_visible(timeout=800):
                log(f"    找到按钮: {sel}", "DEBUG")
                try:
                    await el.click(force=True, timeout=3000)
                except Exception as click_err:
                    # force 也失败就用 JS 点
                    log(f"    force click 失败 ({click_err}), 尝试 JS click", "DEBUG")
                    await page.evaluate("""(sel) => {
                        const el = document.querySelector(sel);
                        if (el) { el.click(); return true; }
                        return false;
                    }""", sel)
                
                await asyncio.sleep(1.5)  # 等菜单动画完成
                
                # 截图看菜单是否弹出
                menu_debug = DATA_DIR / f"_debug_{queue_id}_menu.png"
                await page.screenshot(path=str(menu_debug), full_page=False)
                log(f"    菜单截图: {menu_debug}")
                
                # 尝试点「导出」
                export_clicked = await try_click_export_menu_item(page, queue_name)
                if export_clicked:
                    return export_clicked
                
                # 没找到导出，按 Esc 关闭菜单尝试下一个按钮
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.5)
                
        except Exception as e:
            continue

    # ── Step 3: 用 JS 全局搜索所有包含「导出」文字的可点击元素 ──
    log("  [Step 3] JS 全局搜索「导出」元素...")
    
    export_clicked = await js_find_and_click_export(page, queue_name)
    if export_clicked:
        return export_clicked

    # ── Step 4: 键盘快捷键兜底 ──
    log("  [Step 4] 键盘快捷键兜底...")
    
    await page.click('body', force=True)
    await asyncio.sleep(0.3)
    
    before_files = set(f.name for f in DOWNLOADS_DIR.glob("*")) if DOWNLOADS_DIR.exists() else set()
    default_dl_dir = Path.home() / "Downloads"
    before_default = set(f.name for f in default_dl_dir.glob("*.xlsx")) if default_dl_dir.exists() else set()
    
    for shortcut_desc, key_combo in [
        ("Ctrl+S", "Control+s"),
        ("Cmd+S", "Meta+s"),
        ("Cmd+Shift+S", "Meta+Shift+s"),
    ]:
        try:
            await page.keyboard.press(key_combo)
            dl_file = await poll_for_download(before_files, before_default, queue_name, timeout=30)
            if dl_file:
                return dl_file
        except Exception:
            await asyncio.sleep(0.3)
            continue

    # ── 全部失败 ──
    log("  ❌ 所有策略均未触发下载", "ERROR")
    await dump_page_structure(page, queue_id)
    return None


async def try_click_export_menu_item(page, queue_name):
    """
    在当前页面状态中寻找并点击「导出」菜单项。
    如果「导出」有子菜单，则展开子菜单后点击 Excel/xlsx 下载选项。
    
    企微大表格导出流程：点击 → "正在导出，请稍候" toast → 服务端生成(可能2-5分钟) → 浏览器触发下载
    
    策略：点击后轮询 downloads 目录等待文件出现，不依赖 expect_download 事件。
    
    返回: 下载文件路径 或 None
    """
    from playwright.async_api import TimeoutError as PWTimeout
    
    # 方法1: 用 get_by_text 精确匹配「导出」
    for text_match in ['导出', '导出 as', '.xlsx', 'Excel', '下载']:
        try:
            export_item = page.get_by_text(text_match, exact=(text_match == '导出')).first
            if await export_item.is_visible(timeout=1000):
                log(f"    找到菜单项: [{text_match}]", "SUCCESS")
                
                # 「导出」可能有子菜单（截图显示「导出 →」）
                # 先点它展开子菜单
                await export_item.click(force=True)
                await asyncio.sleep(1)
                
                # 截图看子菜单
                sub_debug = DATA_DIR / "_debug_export_submenu.png"
                await page.screenshot(path=str(sub_debug), full_page=False)
                
                # 子菜单中查找 Excel / xlsx / 下载 选项
                sub_keywords = ['Excel', 'xlsx', '.xlsx', '下载', 'Microsoft Excel']
                for sub_kw in sub_keywords:
                    try:
                        sub_item = page.get_by_text(sub_kw, exact=False).first
                        if await sub_item.is_visible(timeout=800):
                            log(f"    找到子菜单项: [{sub_kw}], 点击下载...", "SUCCESS")
                            
                            # 记录点击前的 downloads 目录文件列表
                            before_files = set(f.name for f in DOWNLOADS_DIR.glob("*")) if DOWNLOADS_DIR.exists() else set()
                            # 也检查浏览器默认下载目录
                            default_dl_dir = Path.home() / "Downloads"
                            before_default = set(f.name for f in default_dl_dir.glob("*.xlsx")) if default_dl_dir.exists() else set()
                            
                            # 点击导出按钮
                            await sub_item.click(force=True)
                            log(f"    已点击导出，等待文件生成...")
                            
                            # 轮询等待文件出现（最多 5 分钟）
                            dl_file = await poll_for_download(before_files, before_default, queue_name, timeout=300)
                            if dl_file:
                                return dl_file
                            else:
                                log(f"    轮询超时，文件未出现", "WARN")
                    except:
                        continue
                
                # 如果点了「导出」后直接触发下载（没有子菜单或自动下载）
                before_files = set(f.name for f in DOWNLOADS_DIR.glob("*")) if DOWNLOADS_DIR.exists() else set()
                before_default = set((Path.home() / "Downloads").glob("*.xlsx")) if (Path.home() / "Downloads").exists() else set()
                before_default_names = set(f.name for f in before_default)
                
                dl_file = await poll_for_download(before_files, before_default_names, queue_name, timeout=60)
                if dl_file:
                    return dl_file
                
                # 没触发下载，返回 None 让上层继续尝试其他方式
                return None
                
        except Exception:
            continue
    
    return None


async def poll_for_download(before_files, before_default_names, queue_name, timeout=300):
    """
    轮询等待下载文件出现在 downloads 目录或浏览器默认下载目录。
    
    企微表格导出的实际流程:
      1. 点击「导出 → Excel」
      2. 页面弹出 toast "正在导出表格，请稍候"
      3. 服务端生成文件 (大表格可能需要 2-5 分钟)
      4. 生成完成后浏览器触发下载
    
    Args:
        before_files: 点击前 downloads 目录的文件名集合
        before_default_names: 点击前浏览器默认下载目录的 xlsx 文件名集合
        queue_name: 队列名称（用于日志）
        timeout: 最大等待秒数
    
    返回: 下载文件路径 或 None
    """
    import time
    start_time = time.time()
    default_dl_dir = Path.home() / "Downloads"
    
    # 状态追踪
    export_toast_seen = False
    last_log_time = 0
    
    while time.time() - start_time < timeout:
        elapsed = int(time.time() - start_time)
        
        # 每 15 秒输出一次状态日志
        if elapsed - last_log_time >= 15:
            last_log_time = elapsed
            log(f"    等待导出完成... ({elapsed}s/{timeout}s)", "DEBUG")
        
        # 检查企微的"正在导出" toast
        try:
            has_exporting = await asyncio.get_event_loop().run_in_executor(
                None, lambda: False  # 非阻塞，改用 JS 检测
            )
            has_exporting = await page.evaluate("""() => {
                const bodyText = document.body.innerText || '';
                return bodyText.includes('正在导出') || bodyText.includes('请稍候');
            }""")
            if has_exporting:
                export_toast_seen = True
        except:
            pass
        
        # 检查 downloads 目录是否有新文件
        if DOWNLOADS_DIR.exists():
            current_files = set(f.name for f in DOWNLOADS_DIR.glob("*"))
            new_files = current_files - before_files
            # 企微导出的文件可能是 .xlsx、.xls、.crdownload，也可能是 UUID 无扩展名
            xlsx_new = [f for f in new_files if f.endswith(('.xlsx', '.xls', '.crdownload'))]
            uuid_new = [f for f in new_files if not f.startswith('.') and not f.endswith(('.xlsx', '.xls', '.crdownload', '.png', '.html', '.log', '.json'))]
            
            all_candidates = xlsx_new + uuid_new
            if all_candidates:
                # 等待 .crdownload 变成完整文件，或 UUID 文件写完
                await asyncio.sleep(3)
                for fname in all_candidates:
                    fpath = DOWNLOADS_DIR / fname
                    if fname.endswith('.crdownload'):
                        # 还在下载中，继续等
                        continue
                    if fpath.exists() and fpath.stat().st_size > 1000:  # 至少 1KB
                        size_mb = fpath.stat().st_size / (1024 * 1024)
                        log(f"    ✅ 检测到新文件: {fname} ({size_mb:.1f} MB)", "SUCCESS")
                        # 如果是 UUID 无扩展名文件，重命名为 .xlsx
                        if not fname.endswith(('.xlsx', '.xls')):
                            xlsx_path = fpath.with_suffix('.xlsx')
                            fpath.rename(xlsx_path)
                            log(f"    📝 重命名为: {xlsx_path.name}", "INFO")
                            return str(xlsx_path)
                        return str(fpath)
        
        # 也检查浏览器默认下载目录
        if default_dl_dir.exists():
            current_default = set(f.name for f in default_dl_dir.glob("*.xlsx"))
            new_default = current_default - before_default_names
            for fname in new_default:
                fpath = default_dl_dir / fname
                if fpath.exists() and fpath.stat().st_size > 0:
                    size_mb = fpath.stat().st_size / (1024 * 1024)
                    log(f"    ✅ 检测到默认下载目录新文件: {fname} ({size_mb:.1f} MB)", "SUCCESS")
                    # 移动到项目 downloads 目录
                    dest = DOWNLOADS_DIR / fname
                    shutil.move(str(fpath), str(dest))
                    return str(dest)
        
        await asyncio.sleep(5)
    
    log(f"    ⏰ 轮询超时 ({timeout}s)，未检测到下载文件", "WARN")
    return None


async def js_find_and_click_export(page, queue_name):
    """
    用 JS 在整个 DOM 中递归搜索包含「导出」/「下载」文字的元素，
    找到后模拟点击并轮询等待下载文件。
    
    返回: 下载文件路径 或 None
    """
    js_code = """() => {
        const keywords = ['\\u5bfc\\u51fa', '\\u4e0b\\u8f7d', 'Excel', 'xlsx'];
        const candidates = [];
        
        const walker = document.createTreeWalker(
            document.body,
            NodeFilter.SHOW_ELEMENT,
            { acceptNode: (n) => {
                const style = window.getComputedStyle(n);
                if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                    return NodeFilter.FILTER_REJECT;
                }
                return NodeFilter.FILTER_ACCEPT;
            }}
        );
        
        let node;
        while ((node = walker.nextNode())) {
            const text = node.textContent?.trim() || '';
            const innerText = node.innerText?.trim() || '';
            
            for (const kw of keywords) {
                if ((text.includes(kw) || innerText.includes(kw)) && 
                    innerText.length <= 30 && innerText.length >= 1 &&
                    innerText.indexOf(kw) < 10) {
                    candidates.push({
                        tag: node.tagName,
                        text: innerText.slice(0, 50),
                        class: (node.className || '').toString().slice(0, 80),
                        role: node.getAttribute('role') || '',
                        ariaLabel: node.getAttribute('aria-label') || '',
                        id: node.id || '',
                    });
                    break;
                }
            }
        }
        
        return candidates.slice(0, 20);
    }"""
    
    result = await page.evaluate(js_code)
    
    if result:
        log(f"    JS 找到 {len(result)} 个候选:", "DEBUG")
        for i, r in enumerate(result):
            log(f"      [{i}] <{r['tag']}> text={r['text']} role={r['role']} aria={r['ariaLabel']}", "DEBUG")
        
        # 逐个尝试点击
        for candidate in result:
            text = candidate['text']
            if any(kw in text for kw in ['导出', '下载', 'Excel']):
                try:
                    # 记录点击前的文件列表
                    before_files = set(f.name for f in DOWNLOADS_DIR.glob("*")) if DOWNLOADS_DIR.exists() else set()
                    default_dl_dir = Path.home() / "Downloads"
                    before_default = set(f.name for f in default_dl_dir.glob("*.xlsx")) if default_dl_dir.exists() else set()
                    
                    await page.evaluate("""(opts) => {
                        const els = document.querySelectorAll(opts.tag.toLowerCase());
                        for (const el of els) {
                            if ((el.textContent?.trim() || '').includes(opts.text.slice(0, 10))) {
                                el.click();
                                return true;
                            }
                        }
                        return false;
                    }""", candidate)
                    
                    # 轮询等待下载
                    dl_file = await poll_for_download(before_files, before_default, queue_name, timeout=180)
                    if dl_file:
                        return dl_file
                except Exception:
                    await asyncio.sleep(0.3)
                    continue
    
    return None


async def save_download(download_obj, queue_name):
    """保存下载文件到 downloads 目录"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{queue_name}_{timestamp}.xlsx"
        dl_path = str(DOWNLOADS_DIR / filename)
        
        await download_obj.save_as(dl_path)
        
        # save_as() 可能返回 None (Playwright 某些版本)，验证文件是否存在
        if os.path.exists(dl_path):
            size_mb = os.path.getsize(dl_path) / (1024 * 1024)
            log(f"  ✅ 下载成功!", "SUCCESS")
            log(f"     文件: {dl_path} ({size_mb:.1f} MB)")
            return dl_path
        else:
            log(f"  ⚠️ save_as 完成但文件不存在: {dl_path}", "WARN")
            # 尝试从 downloads 目录找到最新匹配文件
            candidates = sorted(
                DOWNLOADS_DIR.glob(f"{queue_name}_*.xlsx"),
                key=lambda p: p.stat().st_mtime,
                reverse=True
            )
            if candidates:
                latest = str(candidates[0])
                log(f"     使用已存在的文件: {latest}", "INFO")
                return latest
            return None
    except Exception as e:
        log(f"  保存失败: {e}", "ERROR")
        return None


async def needs_login(page):
    """判断当前页面是否需要登录"""
    url = page.url
    # 如果在企微文档页面，说明已登录
    if 'doc.weixin.qq.com' in url or 'docs.qq.com' in url:
        return False
    # 如果在空白页或登录页，需要登录
    if url in ('about:blank', '') or 'login' in url.lower():
        return True
    # 默认认为不需要 (让用户自己处理)
    return False


async def dump_page_structure(page, queue_id):
    """输出页面结构用于调试"""
    debug_html = DATA_DIR / f"_debug_{queue_id}_dom.html"
    try:
        html = await page.content()
        with open(debug_html, 'w', encoding='utf-8') as f:
            f.write(html)
        log(f"  DOM 已保存: {debug_html}")
    except:
        pass
    
    # 尝试获取所有可见文本按钮
    try:
        buttons = await page.evaluate("""() => {
            const btns = Array.from(document.querySelectorAll('button, [role="button"], [class*="btn"], span[class*="icon"]'));
            return btns.slice(0, 20).map(el => ({
                tag: el.tagName,
                text: el.textContent?.trim()?.slice(0, 50),
                title: el.title || el.getAttribute('aria-label') || '',
                class: el.className?.slice?.(0, 80) || '',
                visible: el.offsetParent !== null
            })).filter(b => b.visible);
        }""")
        log(f"  可见按钮: {json.dumps(buttons, ensure_ascii=False)[:500]}", "DEBUG")
    except Exception as e:
        log(f"  DOM 分析失败: {e}", "DEBUG")


# ============================================================
# 入口: 下载 → 解析入库 → 更新看板
# ============================================================
async def run_full_pipeline(queue_id=None, download_only=False, force_login=False, headless=True):
    """执行完整流水线: 下载 → 解析 → 更新"""
    
    config = load_config()
    queue_configs = get_queue_config(config, queue_id)

    log("=" * 56)
    log(" 方案2 自动化流水线")
    log(f" 队列数: {len(queue_configs)}")
    log(f" 时间:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 56)

    # Step 1: 下载
    log("\n【Step 1/3】自动下载企微表格")
    downloaded = await download_queues(queue_configs, headless=headless, force_login=force_login)
    
    if not downloaded:
        log("没有成功下载任何文件", "WARN")
        if not download_only:
            log("尝试用已有的 uploads 文件继续...", "INFO")
        else:
            return []

    if download_only:
        log("\n仅下载模式，跳过解析和看板更新", "INFO")
        return downloaded

    # Step 2: 解析入库
    log("\n【Step 2/3】解析数据入库")
    result = subprocess_run([
        sys.executable, str(PROJECT_ROOT / "src" / "collector.py"), "import"
    ] + ([f"--queue", queue_id] if queue_id else []))
    log(f"  collector.py import 结果: {'成功' if result == 0 else '失败({result})'}")

    # Step 3: 更新看板
    log("\n【Step 3/3】更新 HTML 看板")
    result = subprocess_run([
        sys.executable, str(PROJECT_ROOT / "src" / "collector.py"), "export-html"
    ])
    log(f"  collector.py export-html 结果: {'成功' if result == 0 else '失败({result})'}")

    log("\n" + "=" * 56)
    log(" 🎉 全部完成!")
    log(f" 看板地址: {PROJECT_ROOT / 'dashboard' / 'index.html'}")
    log("=" * 56)

    # 清理临时下载文件（导入完成后不再保留）
    cleanup_temp_files()

    return downloaded


def cleanup_temp_files():
    """清理 downloads 和 uploads 目录下的临时 xlsx 文件"""
    # 清理 downloads/
    if DOWNLOADS_DIR.exists():
        for f in DOWNLOADS_DIR.glob("*.xlsx"):
            try:
                size_mb = f.stat().st_size / (1024 * 1024)
                f.unlink()
                log(f"🗑️ 已删除 downloads/{f.name} ({size_mb:.1f} MB)")
            except OSError as e:
                log(f"⚠️ 删除失败 downloads/{f.name}: {e}")

    # 清理 uploads/ 根目录（未处理的）
    if UPLOAD_DIR.exists():
        for f in UPLOAD_DIR.glob("*.xlsx"):
            try:
                size_mb = f.stat().st_size / (1024 * 1024)
                f.unlink()
                log(f"🗑️ 已删除 uploads/{f.name} ({size_mb:.1f} MB)")
            except OSError as e:
                log(f"⚠️ 删除失败 uploads/{f.name}: {e}")

    # 清理 uploads/processed/（已处理过的历史文件）
    processed_dir = UPLOAD_DIR / "processed"
    if processed_dir.exists():
        for f in processed_dir.glob("*.xlsx"):
            try:
                size_mb = f.stat().st_size / (1024 * 1024)
                f.unlink()
                log(f"🗑️ 已删除 uploads/processed/{f.name} ({size_mb:.1f} MB)")
            except OSError as e:
                log(f"⚠️ 删除失败 uploads/processed/{f.name}: {e}")


def subprocess_run(cmd):
    """运行子进程命令"""
    import subprocess
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                          cwd=str(PROJECT_ROOT))
        if r.stdout:
            print(r.stdout[-1000:] if len(r.stdout) > 1000 else r.stdout)
        if r.stderr:
            print(r.stderr[-500:] if len(r.stderr) > 500 else r.stderr)
        return r.returncode
    except subprocess.TimeoutExpired:
        log("命令超时(300s)", "ERROR")
        return 1
    except FileNotFoundError:
        log(f"找不到命令: {cmd[0]}", "ERROR")
        return 1


# ============================================================
# CLI
# ============================================================
def main():
    import argparse
    parser = argparse.ArgumentParser(description='方案2: 企微表格自动下载+解析+更新')
    parser.add_argument('--queue', '-q', help='只处理指定队列 (如 q6_shangqiang)')
    parser.add_argument('--login', action='store_true', help='强制重新登录')
    parser.add_argument('--download-only', action='store_true', help='只下载，不解析')
    parser.add_argument('--headless', action='store_true', help='无头模式 (不显示浏览器)')
    parser.add_argument('--debug', action='store_true', help='调试模式 (显示浏览器)')
    args = parser.parse_args()

    # 检查依赖
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log("缺少 playwright 库。请运行:", "ERROR")
        log("  pip install playwright && playwright install chromium")
        sys.exit(1)

    try:
        import yaml
    except ImportError:
        log("缺少 pyyaml 库。请运行:", "ERROR")
        log("  pip install pyyaml")
        sys.exit(1)

    # headless 的反义: debug 模式下显示浏览器
    headless = not args.debug
    
    asyncio.run(run_full_pipeline(
        queue_id=args.queue,
        download_only=args.download_only,
        force_login=args.login,
        headless=headless
    ))


if __name__ == '__main__':
    main()
