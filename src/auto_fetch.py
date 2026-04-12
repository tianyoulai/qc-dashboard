#!/usr/bin/env python3
"""
QC Dashboard — 自动下载脚本 v2（B方案：Playwright 持久化浏览器）
==============================================================
不依赖 CDP/已有 Chrome，用 Playwright 自带浏览器 + 持久化登录态。

首次运行需要扫码登录企微，之后自动复用 cookie。
流程：
  1. 启动 Playwright 持久化浏览器（保留 cookie/localStorage）
  2. 如果未登录，等待用户扫码
  3. 逐个打开智能表格 → 导出 Excel → 保存到 data/downloads/
  4. 触发 collector.py import 入库

用法:
  python3 src/auto_fetch.py              # 下载全部队列
  python3 src/auto_fetch.py --queue q1   # 只下载指定队列
  python3 src/auto_fetch.py --no-import   # 仅下载，不导入
  python3 src/auto_fetch.py --headless    # 无头模式（需要已登录）
  python3 src/auto_fetch.py --login       # 仅做登录（保存登录态）
"""

import os
import sys
import json
import time
import shutil
import logging
import asyncio
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── 项目路径 ──
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"
DOWNLOAD_DIR = PROJECT_ROOT / "data" / "downloads"
USER_DATA_DIR = PROJECT_ROOT / "data" / "browser_profile"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-fetch")


# ============================================================
# 配置加载
# ============================================================
def load_config() -> dict:
    """加载 config.yaml"""
    import yaml
    if not CONFIG_FILE.exists():
        log.error(f"配置文件不存在: {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ============================================================
# 浏览器管理
# ============================================================
async def launch_browser(headless: bool = False):
    """
    启动持久化上下文浏览器（cookie/session 会保留到 disk）。

    首次使用需要登录，之后复用已保存的登录态。
    """
    from playwright.async_api import async_playwright

    pw = await async_playwright().start()

    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 使用持久化上下文 —— cookie、localStorage 都会保存到 disk
    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(USER_DATA_DIR),
        headless=headless,
        viewport={"width": 1440, "height": 900},
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
        # 下载行为配置
        accept_downloads=True,
        # 跳过不必要的资源加速加载
        args=[
            "--disable-blink-features=AutomationControlled",  # 反检测
            "--disable-extensions",
            "--no-sandbox",
        ],
    )

    page = context.pages[0] if context.pages else await context.new_page()

    # 注入反检测脚本
    await add_stealth(page)

    return pw, context, page


async def add_stealth(page):
    """注入简单的反自动化检测"""
    await page.add_init_script("""
        // 隐藏 webdriver 标识
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        
        // 修改 plugins
        Object.defineProperty(navigator, 'plugins', { 
            get: () => [1, 2, 3, 4, 5] 
        });
        
        // 修改 languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['zh-CN', 'zh', 'en']
        });
        
        // Chrome runtime
        window.chrome = { runtime: {} };
    """)


async def check_login_status(page) -> bool:
    """检查是否已登录企微（通过访问文档页面判断）"""
    try:
        resp = await page.goto(
            "https://doc.weixin.qq.com/",
            wait_until="domcontentloaded",
            timeout=15000,
        )
        url = page.url
        current_url = page.url
        
        # 如果跳转到登录页，说明未登录
        login_indicators = ["login", "passport", "auth", "sso", "qy.weixin.qq.com/cgi-bin/loginweb"]
        for indicator in login_indicators:
            if indicator in current_url.lower():
                log.info("   🔑 检测到未登录状态，需要登录")
                return False
        
        # 如果能正常打开 doc 页面说明已登录
        return True
    except Exception as e:
        log.warning(f"   登录检查异常: {e}")
        return False


async def do_login(page):
    """
    引导用户完成企微登录。
    
    显示二维码或引导用户在浏览器中操作，
    登录完成后自动保存 cookie 到磁盘。
    """
    log.info("")
    log.info("=" * 60)
    log.info("  🔑 需要登录企业微信")
    log.info("=" * 60)
    log.info("")
    log.info("  浏览器窗口即将弹出，请在其中完成登录操作：")
    log.info("  1. 扫码登录（推荐）")
    log.info("  2. 或使用账号密码登录")
    log.info("")
    log.info("  ⏳ 等待登录完成...")

    # 打开企微登录页
    await page.goto(
        "https://doc.weixin.qq.com/",
        wait_until="domcontentloaded",
        timeout=30000,
    )

    # 轮询检测登录状态（最多等 120 秒）
    max_wait = 120
    start = time.time()
    logged_in = False

    while time.time() - start < max_wait:
        current_url = page.url
        login_indicators = ["login", "passport", "auth", "sso", "cgi-bin/loginweb"]

        is_login_page = any(ind in current_url.lower() for ind in login_indicators)

        if not is_login_page:
            # 不再在登录页面了，认为登录成功
            logged_in = True
            break

        await asyncio.sleep(2)

    if logged_in:
        log.info("✅ 登录成功！登录态已保存到本地")
        log.info("   后续运行将自动使用此登录态")
        await asyncio.sleep(2)  # 等 cookie 完全写入
        return True
    else:
        log.error("❌ 登录超时（120秒）。请重新运行 --login 尝试")
        return False


# ============================================================
# 单个文档下载
# ============================================================
async def download_one_sheet(
    context,
    queue_cfg: dict,
    download_dir: Path,
    timeout: int = 90,
):
    """
    下载单个智能表格为 Excel 文件。

    操作步骤：
      1. 新开 tab 或复用页面 → 导航到文档 URL
      2. 等待表格加载完成
      3. 点击右上角 ⋮ 菜单
      4. 点击「本地Excel表格(.xlsx)」
      5. 监听下载事件，等待文件落地

    返回: (success: bool, file_path: Path or None)
    """
    from playwright.async_api import TimeoutError as PWTimeout

    qid = queue_cfg.get("id", "unknown")
    qname = queue_cfg.get("name", "")
    doc_url = queue_cfg.get("doc_url", "")

    if not doc_url:
        log.warning(f"  ⏭️ [{qname}] 无 doc_url，跳过")
        return False, None

    log.info(f"🌐 打开 [{qname}] ...")

    # 每个文档用新 tab，避免互相干扰
    page = await context.new_page()

    try:
        download_dir.mkdir(parents=True, exist_ok=True)

        # 监听下载事件
        async with handle_download(page, download_dir) as dl_info:
            # Step 1: 导航到文档
            log.info(f"   URL: {doc_url[:80]}...")
            await page.goto(
                doc_url,
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Step 2: 等待表格渲染
            log.info("   ⏳ 等待表格加载...")
            await wait_for_sheet_loaded(page, timeout=30)

            # 截图记录当前状态（调试用）
            screenshot_path = PROJECT_ROOT / "data" / f"_debug_{qid}.png"
            await page.screenshot(path=str(screenshot_path), full_page=False)
            log.debug(f"   截图已保存: {screenshot_path}")

            # Step 3: 点击导出菜单按钮
            log.info("   🔍 寻找导出菜单...")
            export_btn = await find_export_menu_button(page)
            if not export_btn:
                # 点击前截图
                before_ss = PROJECT_ROOT / "data" / "_before_click.png"
                await page.screenshot(path=str(before_ss), full_page=False)
                log.error(f"  ❌ [{qname}] 未找到导出菜单按钮")
                err_ss = PROJECT_ROOT / "data" / f"_error_{qid}.png"
                await page.screenshot(path=str(err_ss), full_page=True)
                await page.close()
                return False, None

            # 点击前截图
            before_click = PROJECT_ROOT / "data" / f"_before_click_{qid}.png"
            await page.screenshot(path=str(before_click), full_page=False)

            log.info("   👆 点击导出菜单按钮...")
            await export_btn.click()

            # 等菜单弹出（加长等待）
            await asyncio.sleep(2.0)

            # 点击后截图 — 看菜单是否弹出
            after_click = PROJECT_ROOT / "data" / f"_after_click_{qid}.png"
            await page.screenshot(path=str(after_click), full_page=False)
            log.debug(f"   截图对比: {before_click} → {after_click}")

            # Step 4: 点击「本地Excel表格(.xlsx)」
            log.info("   📥 点击导出 Excel...")
            clicked = await click_export_excel_option(page)
            if not clicked:
                log.error(f"  ❌ [{qname}] 未找到「本地Excel表格」选项")
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.3)
                await page.close()
                return False, None

        # 下载完成
        dl_path = dl_info.get("path")
        if dl_path and dl_path.exists():
            safe_name = re.sub(r'[\\/:*?"<>|]', '_', qname)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{ts}_{safe_name}_{qid}.xlsx"
            dest_path = download_dir / new_name

            try:
                shutil.move(str(dl_path), str(dest_path))
            except OSError:
                shutil.copy2(str(dl_path), str(dest_path))
                try:
                    dl_path.unlink()
                except OSError:
                    pass

            log.info(f"  ✅ [{qname}] 下载完成 → {dest_path.name}")
            await page.close()
            return True, dest_path
        else:
            log.error(f"  ❌ [{qname}] 下载未产生文件")
            await page.close()
            return False, None

    except PWTimeout as e:
        log.error(f"  ❌ [{qname}] 超时: {e}")
        await page.close()
        return False, None
    except Exception as e:
        log.error(f"  ❌ [{qname}] 异常: {e}")
        try:
            await page.close()
        except Exception:
            pass
        return False, None


class handle_download:
    """
    上下文管理器：监听 page 的 download 事件。
    
    注意：Playwright 中 expect_download 是 page 级别的方法，不是 context 级别的。
    """
    def __init__(self, page, download_dir: Path):
        self.page = page
        self.download_dir = download_dir
        self.result = {}
        self._download_info = None

    async def __aenter__(self):
        return self.result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        在代码块执行完毕后等待下载事件。
        
        因为下载是在点击导出按钮后才触发的，所以需要在退出时才去等。
        """
        if not self.page or self.page.is_closed():
            self.result["path"] = None
            return False

        try:
            log.debug("   ⏳ 等待文件下载...")
            async with self.page.expect_download(timeout=60000) as dl_info:
                download = await dl_info.value
                path = await download.path()
                self.result["path"] = Path(path)
                self.result["filename"] = download.suggested_filename
        except Exception as e:
            log.debug(f"   下载监听异常: {e}")
            self.result["path"] = None
            self.result["filename"] = None

        return False


# ============================================================
# 页面交互辅助
# ============================================================
async def wait_for_sheet_loaded(page, timeout: int = 30):
    """等待智能表格内容加载完成"""
    selectors = [
        ".sheet-container",
        ".smartsheet-grid",
        "[class*='sheet'] [class*='cell']",
        ".canvas-wrapper",
        "[role='grid']",
        "[role='table']",
        "#app-content",
        ".main-content",
        # 企微智能表格特有
        "[class*='SmartSheet']",
        "[class*='smartsheet']",
        ".grid-canvas",
    ]

    start = time.time()
    loaded = False
    while time.time() - start < timeout:
        for sel in selectors:
            try:
                elem = await page.query_selector(sel)
                if elem and await elem.is_visible():
                    log.debug(f"   表格加载完成 (selector: {sel})")
                    await asyncio.sleep(1.5)  # 数据渲染缓冲
                    loaded = True
                    break
            except Exception:
                continue
        if loaded:
            break
        await asyncio.sleep(0.5)

    if not loaded:
        log.warning("   ⚠️ 表格加载超时，继续尝试操作...")


async def find_export_menu_button(page):
    """
    找到智能表格右上角的 ⋮ 更多操作按钮。
    多层选择器策略 + JS 回退。
    返回一个可点击的对象。
    """
    # === 第一层：精确选择器 ===
    exact_selectors = [
        "[title='更多']",
        "[aria-label='更多']",
        "[aria-label='更多操作']",
        "button.more-btn",
        "[role='button'][title*='更多']",
        "[role='button'][title*='导出']",
        "[role='button'][title*='export']",
    ]

    for sel in exact_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                # 获取元素详情
                bbox = await btn.bounding_box()
                tag = await btn.evaluate("el => el.tagName")
                title = await btn.get_attribute("title") or ""
                aria = await btn.get_attribute("aria-label") or ""
                log.info(f"   🎯 精确匹配: {sel}")
                log.debug(f"      tag={tag}, title='{title}', aria='{aria}'")
                log.debug(f"      bbox={bbox}")
                return _ClickableElement(page, sel, method="selector")
        except Exception:
            continue

    # === 第二层：模糊选择器 ===
    fuzzy_selectors = [
        "[class*='more'][class*='btn']",
        "[class*='toolbar'] [class*='menu']",
        "[class*='toolbar'] button:last-child",
        ".header-actions button:last-child",
        "[class*='toolbar'] [class*='action']:last-child",
        "[class*='header'] [class*='more']",
        "[class*='top-bar'] [class*='more']",
        "[class*='nav-bar'] [class*='btn']:last-child",
    ]
    for sel in fuzzy_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                log.debug(f"   ✅ 模糊匹配: {sel}")
                return _ClickableElement(page, sel, method="selector")
        except Exception:
            continue

    # === 第三层：文本匹配 ===
    text_selectors = ["text=⋯", "text=⋮", "text=…", "text=更多"]
    for sel in text_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                log.debug(f"   ✅ 文本匹配: {sel}")
                return _ClickableElement(page, sel, method="selector")
        except Exception:
            continue

    # === 第四层：JS 搜索 + 坐标点击 ===
    log.debug("   选择器全部未命中，启动 JS 搜索...")
    result = await page.evaluate("""() => {
        const candidates = Array.from(document.querySelectorAll(
            'button, [role="button"], div[tabindex], span[tabindex]'
        )).filter(el => {
            const rect = el.getBoundingClientRect();
            // 右上角区域：x > 60% width, y < 150
            if (rect.x < window.innerWidth * 0.55 || rect.y > 160 || rect.y < 30) return false;
            if (rect.width === 0 || rect.height === 0) return false;
            if (rect.width > 200 || rect.height > 80) return false;

            const text = (el.textContent || '').trim();
            const html = el.innerHTML.toLowerCase();

            return (
                text.includes('…') || text.includes('⋮') || text.includes('⋯') ||
                text.includes('更多') ||
                html.includes('more') || html.includes('menu') ||
                html.includes('ellipsis')
            );
        });

        candidates.sort((a, b) => b.getBoundingClientRect().x - a.getBoundingClientRect().x);

        return candidates.length > 0 ? {
            found: true,
            x: Math.round(candidates[0].getBoundingClientRect().x),
            y: Math.round(candidates[0].getBoundingClientRect().y),
            tag: candidates[0].tagName,
            text: candidates[0].textContent.trim(),
            cls: String(candidates[0].className || '').substring(0, 60),
        } : { found: false };
    }""")

    if result.get("found"):
        x, y = result["x"], result["y"]
        log.debug(f"   🎯 JS 命中: {result['tag']}({result['cls']}) '{result['text']}' @({x},{y})")
        return _ClickableElement(page, None, method="coordinate", x=x, y=y)

    log.debug(f"   ❌ 所有方式均未找到导出按钮")
    log.debug(f"   标题: {await page.title()}, URL: {page.url}")
    return None


class _ClickableElement:
    """
    可点击元素的包装类。
    优先用 Playwright 原生 click(force=True)，确保触发 React 合成事件。
    """
    def __init__(self, page, selector=None, method="selector", x=0, y=0):
        self.page = page
        self.selector = selector
        self.method = method
        self.x = x
        self.y = y

    async def click(self):
        """执行点击 — 用 force=True 确保即使元素在视窗外也能点击"""
        try:
            if self.method == "coordinate":
                log.debug(f"   🖱️ 坐标点击 ({self.x}, {self.y})")
                await self.page.mouse.click(self.x, self.y)
            else:
                # 先尝试原生 force click（能触发 React/Vue 合成事件）
                log.debug(f"   🔘 Force click: {self.selector}")
                elem = await self.page.query_selector(self.selector)
                if elem:
                    # 先滚动到可见区域
                    await elem.scroll_into_view_if_needed()
                    await asyncio.sleep(0.3)
                    await elem.click(force=True)
                else:
                    # fallback: JS click
                    await self.page.eval_on_selector(self.selector, "el => el.click()")
        except Exception as e:
            log.debug(f"   ⚠️ 点击异常 ({self.method}): {e}")
            try:
                if self.method == "coordinate":
                    await self.page.mouse.click(self.x, self.y)
                elif self.selector:
                    elem = await self.page.query_selector(self.selector)
                    if elem:
                        await elem.evaluate("el => el.click()")
            except Exception as e2:
                log.warning(f"   ❌ fallback 也失败: {e2}")


async def click_export_excel_option(page):
    """
    在弹出的菜单中点击「本地Excel表格(.xlsx)」选项。
    
    企微智能表格的菜单可能使用自定义组件（非标准 role=menu），
    所以采用多重策略匹配。
    """
    # 先截图记录菜单状态
    menu_ss = PROJECT_ROOT / "data" / "_menu_state.png"
    await page.screenshot(path=str(menu_ss), full_page=False)
    log.debug(f"   菜单状态截图: {menu_ss}")

    # === 策略1: Playwright 文本选择器 ===
    text_options = [
        "text=本地Excel表格", "text=本地 Excel", "text=.xlsx",
        "text=导出Excel", "text=导出 Excel", "text=下载Excel",
        "text=Excel", "text=Export as Excel", "text=Download as XLSX",
        "text=Local Excel", "text=本地Excel",
    ]
    for sel in text_options:
        try:
            item = await page.query_selector(sel)
            if item and await item.is_visible():
                await item.evaluate("el => el.click()")
                log.debug(f"   ✅ 策略1命中: {sel}")
                return True
        except Exception:
            continue

    # === 策略2: JS 全文搜索（不限元素类型）===
    log.debug("   策略2: JS 全文搜索...")
    clicked2 = await page.evaluate("""() => {
        const allElements = document.querySelectorAll('div, span, li, a, button, p');
        let best = null;

        for (const el of allElements) {
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) continue;

            const text = (el.textContent || '').trim();
            if (!text || text.length > 60) continue;

            const low = text.toLowerCase();
            if (low.includes('excel') || low.includes('.xls') || low.includes('本地')) {
                if (!best) {
                    best = {
                        tag: el.tagName,
                        text: text.substring(0, 40),
                        x: Math.round(rect.x + rect.width / 2),
                        y: Math.round(rect.y + rect.height / 2),
                    };
                }
            }
        }

        if (best) {
            try {
                const targetEl = document.elementFromPoint(best.x, best.y);
                if (targetEl) targetEl.click();
            } catch(e) { /* ignore */ }
            return best;
        }
        return null;
    }""")

    if clicked2:
        log.debug(f"   ✅ 策略2b命中: [{clicked2.get('tag')}] '{clicked2.get('text')}' @({clicked2.get('x')},{clicked2.get('y')})")
        return True

    # === 策略3: 收集所有弹出层/浮层文本用于调试 ===
    log.debug("   策略3: 收集DOM快照...")
    dom_info = await page.evaluate("""() => {
        const result = [];
        const popups = document.querySelectorAll(
            '[class*="pop"], [class*="drop"], [class*="menu"], ' +
            '[class*="overlay"], [class*="modal"], [class*="dialog"], ' +
            '[class*="panel"], [class*="sheet"], [class*="tooltip"], ' +
            '[class*="layer"], [class*="popup"], [class*="drawer"], ' +
            '[class*="action-sheet"]'
        );
        popups.forEach(p => {
            const r = p.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) {
                result.push({
                    type: 'POPUP', tag: p.tagName,
                    cn: String(p.className).substring(0, 80),
                    pos: '(' + Math.round(r.x) + ',' + Math.round(r.y) + ') ' +
                          Math.round(r.width) + 'x' + Math.round(r.height),
                    text: (p.innerText || '').replace(/\s+/g, ' ').substring(0, 200),
                });
            }
        });

        // 也收集最近 body 子元素
        Array.from(document.body.children).forEach(k => {
            const r = k.getBoundingClientRect();
            if (r.width > 100 && r.height > 30) {
                const txt = k.innerText.trim();
                if (txt.length > 10 && txt.length < 300) {
                    result.push({
                        type: 'CHILD', tag: k.tagName, id: k.id || '',
                        cn: String(k.className).substring(0, 60),
                        text: txt.substring(0, 150),
                    });
                }
            }
        });
        return result;
    }""")

    if dom_info:
        for item in dom_info:
            t = item.get("type", "?")
            tag = item.get("tag", "")
            if t == "POPUP":
                log.debug(f"     {t} [{tag}] {item.get('pos','')} {item.get('cn','')}")
                log.debug(f"       内容: {item.get('text','')}")
            else:
                log.debug(f"     {t} [{tag}#{item.get('id','')}] {item.get('cn','')}")
                log.debug(f"       文本: {item.get('text','')}")

    return False


# ============================================================
# 主流程
# ============================================================
async def run_auto_fetch(
    config: dict,
    queue_filter: str = None,
    do_import: bool = True,
    headless: bool = False,
    login_only: bool = False,
    dry_run: bool = False,
):
    """主入口"""
    queues = config.get("queues", [])
    if queue_filter:
        queues = [q for q in queues if q.get("id") == queue_filter]
        if not queues:
            log.error(f"找不到队列: {queue_filter}")
            return

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    results = {
        "total": len(queues),
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "files": [],
    }

    if dry_run:
        log.info("🏃 ** DRY RUN 模式 **")
        for q in queues:
            log.info(f"  📋 [{q['name']}] ({q['id']}) → {q.get('doc_url', '')[:60]}...")
        return results

    # ── 启动浏览器 ──
    log.info("=" * 60)
    log.info("  🌐 启动浏览器...")
    if headless:
        log.info("  🤖 无头模式（需要已登录）")
    log.info("=" * 60)

    pw, context, page = await launch_browser(headless=headless)

    try:
        # 检查登录状态
        if not await check_login_status(page):
            if login_only or not headless:
                success = await do_login(page)
                if not success:
                    log.error("登录失败，终止执行")
                    return results
            elif headless:
                log.error("无头模式下未登录！请先运行: python3 src/auto_fetch.py --login")
                return results

        # 仅登录模式
        if login_only:
            log.info("✅ 登录完成！下次可使用 --headless 运行")
            results["_login_ok"] = True
            return results

        # ── 逐个下载 ──
        for idx, qcfg in enumerate(queues, 1):
            qid = qcfg.get("id", "")
            qname = qcfg.get("name", "")
            total = len(queues)

            log.info("-" * 50)
            log.info(f"[{idx}/{total}] 📊 {qname} ({qid})")
            log.info("-" * 50)

            success, fpath = await download_one_sheet(context, qcfg, DOWNLOAD_DIR)

            if success and fpath:
                results["success"] += 1
                results["files"].append(str(fpath))
            elif success:
                results["skipped"] += 1
            else:
                results["failed"] += 1

            # 队列间间隔
            if idx < total:
                await asyncio.sleep(2)

    finally:
        await context.close()
        await pw.stop()

    # ── 输出汇总 ──
    log.info("\n" + "=" * 60)
    log.info(f"  结果: ✅{results['success']} | ❌{results['failed']} | ⏭️{results['skipped']}")
    log.info("=" * 60)

    # ── 触发导入 ──
    if do_import and results["files"]:
        log.info("\n📦 触发 Excel 导入...")
        run_import()

    return results


def run_import():
    """调用 collector.py import"""
    import subprocess

    script = PROJECT_ROOT / "src" / "collector.py"
    result = subprocess.run(
        [sys.executable, str(script), "import"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=180,
    )

    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            log.info(f"  {line}")
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            if line.strip():
                log.warning(f"  {line}")

    if result.returncode == 0:
        log.info("✅ 导入完成！python3 src/collector.py status 查看状态")
    else:
        log.warning("⚠️ 导入可能有问题")


def main():
    args = sys.argv[1:]

    if "-h" in args or "--help" in args:
        print(__doc__)
        print("""
参数:
  --queue ID       只处理指定队列（如 q1_toufang）
  --no-import      仅下载，不触发导入
  --headless       无头模式（需先 --login）
  --login          仅执行登录（保存登录态）
  --dry-run        模拟运行，不实际操作浏览器
""")
        return

    queue_filter = None
    do_import = True
    headless = False
    login_only = False
    dry_run = False

    i = 0
    while i < len(args):
        if args[i] == "--queue" and i + 1 < len(args):
            queue_filter = args[i + 1]
            i += 2
        elif args[i] == "--no-import":
            do_import = False
            i += 1
        elif args[i] == "--headless":
            headless = True
            i += 1
        elif args[i] == "--login":
            login_only = True
            i += 1
        elif args[i] == "--dry-run":
            dry_run = True
            i += 1
        else:
            i += 1

    config = load_config()
    asyncio.run(run_auto_fetch(config, queue_filter, do_import, headless, login_only, dry_run))


if __name__ == "__main__":
    main()
