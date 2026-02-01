# -*- coding: utf-8 -*-
"""
===================================
ChromeFetcher - Chrome 远程调试数据源
===================================

职责：
1. 通过 Chrome 远程调试接口连接本地浏览器
2. 获取东方财富行业板块行情数据
3. 作为 Akshare 的补充数据源

连接方式：
- 连接到本地已启动的 Chrome 远程调试端口
- 调试地址：http://127.0.0.1:9222
- 无需额外启动 Chrome

特点：
- 使用真实浏览器，规避反爬机制
- 可执行 JavaScript，提取动态数据
- 支持截图和调试
"""

import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ChromeFetcher:
    """
    Chrome Fetcher - 通过 Chrome 远程调试获取网页数据
    
    使用 Playwright 连接到本地已启动的 Chrome 浏览器
    """
    
    def __init__(self, remote_debug_url: str = "http://127.0.0.1:9222"):
        """
        初始化 Chrome Fetcher
        
        Args:
            remote_debug_url: Chrome 远程调试地址，默认 http://127.0.0.1:9222
        """
        self.remote_debug_url = remote_debug_url
        self.browser = None
        self.context = None
        self.page = None
        self._playwright_imported = False
        self._cache_file = os.path.join("data", "industry_sectors_cache.json")
    
    def _ensure_playwright(self) -> bool:
        """确保 Playwright 已导入"""
        if self._playwright_imported:
            return True
        
        try:
            from playwright.sync_api import sync_playwright
            self._playwright = sync_playwright
            self._playwright_imported = True
            logger.debug("Playwright 导入成功")
            return True
        except ImportError:
            logger.error("Playwright 未安装，请运行: pip install playwright")
            return False
    
    def connect(self) -> bool:
        """
        连接到 Chrome 远程调试端口
        
        Returns:
            True 表示连接成功，False 表示连接失败
        """
        if not self._ensure_playwright():
            return False
        
        try:
            # 连接到已启动的 Chrome
            logger.info(f"尝试连接到 Chrome 远程调试端口: {self.remote_debug_url}")
            
            playwright = self._playwright().start()
            self.browser = playwright.chromium.connect_over_cdp(self.remote_debug_url)
            
            # 创建新上下文
            self.context = self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            
            # 创建新页面
            self.page = self.context.new_page()
            
            logger.info("成功连接到 Chrome 远程调试端口")
            return True
            
        except Exception as e:
            logger.error(f"连接 Chrome 失败: {e}")
            logger.error("请确保：")
            logger.error("  1. Chrome 已启动")
            logger.error("  2. 使用 --remote-debugging-port=9222 参数启动")
            logger.error("  3. 调试地址为: " + self.remote_debug_url)
            return False
    
    def is_available(self) -> bool:
        """
        检查 Chrome Fetcher 是否可用
        
        Returns:
            True 表示可用，False 表示不可用
        """
        if not self._ensure_playwright():
            return False
        
        try:
            # 尝试连接，测试是否可用
            playwright = self._playwright().start()
            browser = playwright.chromium.connect_over_cdp(self.remote_debug_url)
            browser.close()
            playwright.stop()
            return True
        except Exception as e:
            logger.debug(f"Chrome 不可用: {e}")
            return False
    
    def close_all_pages(self) -> bool:
        """
        关闭 Chrome 中的所有页面
        
        Returns:
            True 表示关闭成功，False 表示关闭失败
        """
        try:
            if self.browser and self.context:
                # 获取所有页面
                pages = self.context.pages
                logger.info(f"发现 {len(pages)} 个 Chrome 页面，准备关闭...")
                
                # 关闭所有页面
                for page in pages:
                    try:
                        page.close()
                        logger.debug(f"已关闭页面: {page.url}")
                    except:
                        pass
                
                logger.info("所有 Chrome 页面已关闭")
                return True
            else:
                # 如果未连接，尝试连接后关闭
                return self._close_all_pages_via_reconnect()
                
        except Exception as e:
            logger.error(f"关闭 Chrome 页面失败: {e}")
            return False
    
    def _close_all_pages_via_reconnect(self) -> bool:
        """通过重新连接的方式关闭所有页面"""
        if not self._ensure_playwright():
            return False
        
        try:
            # 连接到 Chrome
            playwright = self._playwright().start()
            browser = playwright.chromium.connect_over_cdp(self.remote_debug_url)
            
            # 获取所有页面
            pages = browser.contexts[0].pages
            logger.info(f"发现 {len(pages)} 个 Chrome 页面，准备关闭...")
            
            # 关闭所有页面
            for page in pages:
                try:
                    page.close()
                    logger.debug(f"已关闭页面: {page.url}")
                except:
                    pass
            
            browser.close()
            playwright.stop()
            logger.info("所有 Chrome 页面已关闭")
            return True
            
        except Exception as e:
            logger.error(f"关闭 Chrome 页面失败: {e}")
            return False
    
    def _get_cached_data(self) -> Optional[List[Dict[str, Any]]]:
        """获取缓存的数据（仅当天的数据）"""
        if not os.path.exists(self._cache_file):
            return None
        
        try:
            with open(self._cache_file, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            
            # 检查缓存日期是否为今天
            cache_date = cache.get('date', '')
            today = datetime.now().strftime('%Y-%m-%d')
            
            if cache_date == today:
                logger.info(f"[Chrome] 使用缓存数据（{cache_date}）")
                return cache.get('data', [])
            
            logger.debug(f"[Chrome] 缓存数据过期（{cache_date} != {today}）")
            return None
            
        except Exception as e:
            logger.warning(f"[Chrome] 读取缓存失败: {e}")
            return None
    
    def _save_cached_data(self, data: List[Dict[str, Any]]) -> None:
        """保存数据到缓存"""
        try:
            # 确保 data 目录存在
            os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
            
            cache = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            with open(self._cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            
            logger.info(f"[Chrome] 缓存已保存（{len(data)} 个板块）")
            
        except Exception as e:
            logger.warning(f"[Chrome] 保存缓存失败: {e}")
    
    def get_industry_sectors(self) -> List[Dict[str, Any]]:
        """
        获取行业板块行情（分页获取全部数据，带缓存）
        
        Returns:
            行业板块数据列表
        """
        # 检查缓存
        cached_data = self._get_cached_data()
        if cached_data:
            return cached_data
        
        if not self.connect():
            return []
        
        all_sectors = []
        import time
        page_num = 1
        
        try:
            logger.info("[Chrome] 开始获取行业板块行情...")
            self.page.goto("https://quote.eastmoney.com/center/gridlist.html#industry_board", 
                          wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)
            
            # 提取数据的 JavaScript 代码
            extract_js = """() => {
                const tables = Array.from(document.querySelectorAll('table'));
                const table = tables.find(t => t.querySelectorAll('tbody tr').length > 1);
                if (!table) return [];
                
                return Array.from(table.querySelectorAll('tbody tr')).map(row => {
                    const c = row.querySelectorAll('td');
                    if (c.length < 12) return null;
                    
                    let turnover = 0;
                    const t = c[6].innerText.trim();
                    if (t.includes('万亿')) turnover = parseFloat(t.replace(/[^0-9.]/g, '')) * 1e12;
                    else if (t.includes('亿')) turnover = parseFloat(t.replace(/[^0-9.]/g, '')) * 1e8;
                    else turnover = parseFloat(t.replace(/,/g, '')) || 0;
                    
                    return {
                        '板块名称': c[1].innerText.trim(),
                        '板块代码': '',
                        '最新价': parseFloat(c[3].innerText.replace(/,/g, '')) || 0,
                        '涨跌幅': parseFloat(c[5].innerText.replace('%', '')) || 0,
                        '涨跌额': parseFloat(c[4].innerText.replace(/,/g, '')) || 0,
                        '成交量': 0,
                        '成交额': turnover,
                        '涨跌家数': `${c[8].innerText.trim()}/${c[9].innerText.trim()}`,
                        '领涨股': c[10].innerText.trim(),
                        '领涨股涨跌': parseFloat(c[11].innerText.replace('%', '')) || 0
                    };
                }).filter(x => x && x['板块名称']);
            }"""
            
            # 循环获取每一页，直到找不到下一页按钮
            while True:
                logger.info(f"[Chrome] 获取第 {page_num} 页...")
                
                # 提取当前页数据
                data = self.page.evaluate(extract_js)
                
                if data:
                    existing = {s['板块名称'] for s in all_sectors}
                    new_data = [s for s in data if s['板块名称'] not in existing]
                    if new_data:
                        all_sectors.extend(new_data)
                        logger.info(f"[Chrome] 新增 {len(new_data)} 个板块，总共 {len(all_sectors)} 个")
                    else:
                        logger.info("[Chrome] 无新数据，翻页完成")
                        break
                else:
                    logger.warning("[Chrome] 当前页无数据，停止")
                    break
                
                # 检查是否有下一页按钮
                has_next = self.page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('.qtpager a'))
                        .some(el => el.getAttribute('title') === '下一页');
                }""")
                
                if not has_next:
                    logger.info("[Chrome] 无下一页按钮，翻页完成")
                    break
                
                # 点击下一页
                self.page.evaluate("""() => {
                    const nextBtn = Array.from(document.querySelectorAll('.qtpager a'))
                        .find(el => el.getAttribute('title') === '下一页');
                    if (nextBtn) nextBtn.click();
                }""")
                
                page_num += 1
                time.sleep(3)
            
            logger.info(f"[Chrome] 总共获取 {len(all_sectors)} 个行业板块")
            
            # 保存到缓存
            if all_sectors:
                self._save_cached_data(all_sectors)
            
        except Exception as e:
            logger.error(f"[Chrome] 获取失败: {e}")
        finally:
            self.close()
        
        return all_sectors
    
    def close(self):
        """关闭连接"""
        try:
            if self.page:
                self.page.close()
                logger.debug("Chrome 页面已关闭")
            
            if self.context:
                self.context.close()
                logger.debug("Chrome 上下文已关闭")
            
            if self.browser:
                self.browser.close()
                logger.info("Chrome 连接已关闭")
                
        except Exception as e:
            logger.error(f"关闭 Chrome 连接时出错: {e}")


# 测试入口
if __name__ == "__main__":
    import sys
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s'
    )
    
    fetcher = ChromeFetcher()
    
    # 测试连接
    print("=" * 60)
    print("测试 1: Chrome 连接测试")
    print("=" * 60)
    
    if fetcher.is_available():
        print("✅ Chrome 可用")
        
        # 测试获取行业板块
        print("\n" + "=" * 60)
        print("测试 2: 获取行业板块行情")
        print("=" * 60)
        
        sectors = fetcher.get_industry_sectors()
        
        if sectors:
            print(f"✅ 成功获取 {len(sectors)} 个行业板块")
            print("\n前 5 个板块:")
            for i, sector in enumerate(sectors[:5], 1):
                print(f"  {i}. {sector['板块名称']}: "
                      f"涨跌幅={sector['涨跌幅']:+.2f}%")
        else:
            print("❌ 未获取到行业板块数据")
    else:
        print("❌ Chrome 不可用")
        print("\n请确保：")
        print("  1. Chrome 已启动")
        print("  2. 使用 --remote-debugging-port=9222 参数启动")
        print("  3. 调试地址为: http://127.0.0.1:9222")
        print("\n启动命令示例:")
        print('  chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\\chrome-debug"')
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
