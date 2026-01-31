# -*- coding: utf-8 -*-
"""
===================================
LongbridgeFetcher - 备用数据源 (Priority 1)
===================================

数据来源：Longbridge OpenAPI（长桥证券）
特点：需要配置 AppKey/AppSecret/AccessToken、支持港股/美股/A股
优点：数据质量高、接口稳定、支持多市场

流控策略：
1. 实现"每30秒60次"的速率限制
2. 使用滑动窗口算法精确控制
3. 失败后指数退避重试

配置说明：
需要在 .env 文件中配置：
- LONGPORT_APP_KEY: 应用 Key
- LONGPORT_APP_SECRET: 应用 Secret
- LONGPORT_ACCESS_TOKEN: 访问 Token
"""

import logging
import time
from collections import deque
from datetime import datetime, date
from typing import Optional

import longport.openapi
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS
from config import get_config

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    速率限制器（滑动窗口算法）
    
    用于实现每 30 秒最多 60 次请求的限制
    """
    
    def __init__(self, max_requests: int = 60, time_window: int = 30):
        """
        初始化速率限制器
        
        Args:
            max_requests: 时间窗口内的最大请求数
            time_window: 时间窗口（秒）
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()  # 存储请求时间戳
    
    def acquire(self) -> None:
        """
        获取请求许可
        
        如果超过速率限制，会阻塞直到可以发起新请求
        """
        current_time = time.time()
        
        # 移除时间窗口外的请求记录
        while self.requests and current_time - self.requests[0] > self.time_window:
            self.requests.popleft()
        
        # 检查是否超过限制
        if len(self.requests) >= self.max_requests:
            # 计算需要等待的时间
            oldest_request = self.requests[0]
            sleep_time = self.time_window - (current_time - oldest_request) + 0.1  # +0.1秒缓冲
            
            logger.warning(
                f"Longbridge 达到速率限制 ({len(self.requests)}/{self.max_requests} 次/{self.time_window}秒)，"
                f"等待 {sleep_time:.1f} 秒..."
            )
            
            time.sleep(sleep_time)
            
            # 重新清理过期请求
            current_time = time.time()
            while self.requests and current_time - self.requests[0] > self.time_window:
                self.requests.popleft()
        
        # 记录当前请求
        self.requests.append(current_time)
        logger.debug(f"Longbridge 当前时间窗口请求数: {len(self.requests)}/{self.max_requests}")


class LongbridgeFetcher(BaseFetcher):
    """
    Longbridge OpenAPI 数据源实现
    
    优先级：1
    数据来源：Longbridge OpenAPI
    
    关键策略：
    - 滑动窗口算法实现速率限制（30秒60次）
    - 支持港股、美股、A股等多市场
    - 使用历史 K 线接口获取较长时间段数据
    - 失败后指数退避重试
    
    配置要求：
    - LONGPORT_APP_KEY: 应用 Key
    - LONGPORT_APP_SECRET: 应用 Secret
    - LONGPORT_ACCESS_TOKEN: 访问 Token
    
    权限说明：
    - 需要在 Longbridge 开通行情权限
    - 每月可查询的标的数量有上限（根据账户等级）
    """
    
    name = "LongbridgeFetcher"
    priority = 2  # 默认优先级为2，动态更新优先级
    
    def __init__(self, rate_limit_per_30s: int = 60):
        """
        初始化 LongbridgeFetcher
        
        Args:
            rate_limit_per_30s: 每 30 秒最大请求数（默认 60）
        """
        self.rate_limit_per_30s = rate_limit_per_30s
        self._rate_limiter = RateLimiter(max_requests=rate_limit_per_30s, time_window=30)
        self._api: Optional[object|longport.openapi.QuoteContext] = None  # QuoteContext 实例
        
        # 尝试初始化 API
        self._init_api()

        # 根据 API 初始化结果动态调整优先级
        self.priority = self._determine_priority()
    
    def _init_api(self) -> None:
        """
        初始化 Longbridge API
        
        如果配置未设置，此数据源将不可用
        """
        config = get_config()
        
        # 检查配置
        app_key = config.longport_app_key or self._get_env_var('LONGPORT_APP_KEY')
        app_secret = config.longport_app_secret or self._get_env_var('LONGPORT_APP_SECRET')
        access_token = config.longport_access_token or self._get_env_var('LONGPORT_ACCESS_TOKEN')
        
        if not app_key or not app_secret or not access_token:
            logger.warning("Longbridge 配置未完整（需要 LONGPORT_APP_KEY, LONGPORT_APP_SECRET, LONGPORT_ACCESS_TOKEN），此数据源不可用")
            return
        
        try:
            from longport.openapi import QuoteContext, Config
            
            # 创建配置
            longport_config = Config(
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
            )
            
            # 创建 QuoteContext
            self._api = QuoteContext(longport_config)
            
            logger.info("Longbridge API 初始化成功")
            
        except ImportError:
            logger.error("Longbridge SDK 未安装，请运行: pip install longport")
            self._api = None
        except Exception as e:
            logger.error(f"Longbridge API 初始化失败: {e}")
            self._api = None

    def _determine_priority(self) -> int:
        """
        根据 Token 配置和 API 初始化状态确定优先级

        策略：
        - Token 配置且 API 初始化成功：优先级 0（最高）
        - 其他情况：优先级 2（默认）

        Returns:
            优先级数字（0=最高，数字越大优先级越低）
        """

        if self._api is not None:
            # Token 配置且 API 初始化成功，提升为最高优先级
            logger.info("✅ 检测到 API 初始化成功，LongBridge 数据源优先级提升为最高 (Priority 0)")
            return 0

        # API 初始化失败，保持默认优先级
        return 2

    def _get_env_var(self, var_name: str) -> Optional[str]:
        """获取环境变量"""
        import os
        return os.getenv(var_name)
    
    def is_available(self) -> bool:
        """
        检查数据源是否可用
        
        Returns:
            True 表示可用，False 表示不可用
        """
        return self._api is not None
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """
        转换股票代码为 Longbridge 格式
        
        Longbridge 要求的格式（ticker.region）：
        - 沪市 A 股：600519.SH
        - 深市 A 股：000001.SZ
        - 港股：700.HK
        - 美股：AAPL.US
        
        Args:
            stock_code: 原始代码，如 '600519', '000001', '700', 'AAPL'
            
        Returns:
            Longbridge 格式代码，如 '600519.SH', '000001.SZ', '700.HK', 'AAPL.US'
        """
        code = stock_code.strip().upper()
        
        # 已经包含后缀的情况
        if '.' in code:
            return code
        
        # 根据代码前缀判断市场
        # 沪市：600xxx, 601xxx, 603xxx, 688xxx (科创板)
        # 深市：000xxx, 002xxx, 300xxx (创业板)
        # 港股：纯数字，通常 4-5 位
        # 美股：字母代码
        
        if code.isdigit():
            # 数字代码，可能是 A 股或港股
            if code.startswith(('600', '601', '603', '688')):
                return f"{code}.SH"
            elif code.startswith(('000', '002', '300')):
                return f"{code}.SZ"
            else:
                # 默认认为是港股（如 700, 9988 等）
                return f"{code}.HK"
        else:
            # 字母代码，认为是美股
            return f"{code}.US"
    
    def _parse_date(self, date_str: str) -> date:
        """
        解析日期字符串
        
        支持格式：
        - YYYY-MM-DD
        - YYYYMMDD
        
        Args:
            date_str: 日期字符串
            
        Returns:
            date 对象
        """
        date_str = date_str.strip()
        
        # 尝试 YYYY-MM-DD 格式
        if '-' in date_str:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        
        # 尝试 YYYYMMDD 格式
        if len(date_str) == 8 and date_str.isdigit():
            return datetime.strptime(date_str, '%Y%m%d').date()
        
        raise ValueError(f"不支持的日期格式: {date_str}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Longbridge 获取原始数据
        
        使用 history_candlesticks_by_date 接口获取历史 K 线数据
        
        流程：
        1. 检查 API 是否可用
        2. 执行速率限制检查
        3. 转换股票代码格式
        4. 调用 API 获取数据
        """
        if self._api is None:
            raise DataFetchError("Longbridge API 未初始化，请检查配置")
        
        # 速率限制检查
        self._rate_limiter.acquire()
        
        # 优先使用 stock_full_code_list 中的完整代码
        appropriate_code = self.get_appropriate_stock_code(stock_code)
        # 转换代码格式（如果已经是完整代码，_convert_stock_code 会直接返回）
        symbol = self._convert_stock_code(appropriate_code)
        
        # 解析日期
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date)
        
        logger.debug(f"调用 Longbridge history_candlesticks_by_date({symbol}, {start_date}, {end_date})")
        
        try:
            from longport.openapi import Period, AdjustType
            
            # 调用 history_candlesticks_by_date 接口获取历史 K 线数据
            resp = self._api.history_candlesticks_by_date(
                symbol=symbol,
                period=Period.Day,
                adjust_type=AdjustType.NoAdjust,
                start=start_dt,
                end=end_dt,
            )
            
            # 转换为 DataFrame
            data = []
            for candle in resp:
                data.append({
                    'date': candle.timestamp.date(),
                    'open': float(candle.open),
                    'high': float(candle.high),
                    'low': float(candle.low),
                    'close': float(candle.close),
                    'volume': int(candle.volume),
                    'amount': float(candle.turnover),
                })
            
            df = pd.DataFrame(data)
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测限流
            if any(keyword in error_msg for keyword in ['限流', 'limit', 'quota', '配额']):
                logger.warning(f"Longbridge 速率限制: {e}")
                raise RateLimitError(f"Longbridge 速率限制: {e}") from e
            
            # 检测权限问题
            if any(keyword in error_msg for keyword in ['权限', 'permission', '无权限']):
                logger.warning(f"Longbridge 权限不足: {e}")
                raise DataFetchError(f"Longbridge 权限不足，请开通行情权限: {e}") from e
            
            raise DataFetchError(f"Longbridge 获取数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Longbridge 数据
        
        Longbridge 返回的列名：
        date, open, high, low, close, volume, amount
        
        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        
        注意：Longbridge 不直接提供涨跌幅，需要计算
        """
        df = df.copy()
        
        # 计算涨跌幅（相对于前一日的收盘价）
        df = df.sort_values('date', ascending=True).reset_index(drop=True)
        df['prev_close'] = df['close'].shift(1)
        df['pct_chg'] = ((df['close'] - df['prev_close']) / df['prev_close'] * 100).round(2)
        
        # 删除临时列
        df = df.drop(columns=['prev_close'], errors='ignore')
        
        # 添加股票代码列
        df['code'] = stock_code
        
        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df

    def get_watchlist_from_group(self, group_name: str = None, group_id: int = None) -> list[str]:
        """
        从长桥自选股分组中获取股票代码列表
        
        支持通过分组名称或分组ID来获取股票列表
        
        Args:
            group_name: 分组名称（优先使用）
            group_id: 分组ID（当group_name为None时使用）
            
        Returns:
            股票代码列表，格式如 ['600519', '000001', '700.HK']
            
        Raises:
            DataFetchError: 获取失败时抛出
        """
        if self._api is None:
            raise DataFetchError("Longbridge API 未初始化，请检查配置")
        
        # 速率限制检查
        self._rate_limiter.acquire()
        
        try:
            from longport.openapi import WatchlistGroup
            
            # 调用 watchlist() 方法获取所有分组
            logger.debug(f"调用 Longbridge watchlist() 获取自选股分组")
            resp: list[WatchlistGroup] = self._api.watchlist()
            
            if not resp or len(resp) == 0:
                logger.warning("长桥自选股分组列表为空")
                return []
            
            # 查找指定的分组
            target_group = None
            
            if group_name:
                # 优先通过分组名称查找
                for group in resp:
                    if group.name == group_name:
                        target_group = group
                        logger.info(f"找到长桥自选股分组: {group_name} (ID: {group.id})")
                        break
                if not target_group:
                    logger.warning(f"未找到长桥自选股分组: {group_name}")
                    # 打印所有可用的分组名称
                    available_groups = [g.name for g in resp]
                    logger.info(f"可用的分组名称: {', '.join(available_groups)}")
            elif group_id is not None:
                # 通过分组ID查找
                for group in resp:
                    if group.id == group_id:
                        target_group = group
                        logger.info(f"找到长桥自选股分组: ID={group_id} (名称: {group.name})")
                        break
                if not target_group:
                    logger.warning(f"未找到长桥自选股分组 ID: {group_id}")
            else:
                # 未指定分组，返回第一个分组（通常是"all"）
                target_group = resp.groups[0]
                logger.info(f"未指定分组，使用默认分组: {target_group.name} (ID: {target_group.id})")
            
            # 提取股票代码
            if target_group and target_group.securities:
                stock_codes = []
                for security in target_group.securities:
                    # 将 symbol 转换为标准格式（如 700.HK -> 700.HK, 600519.SH -> 600519）
                    symbol = security.symbol
                    stock_codes.append(symbol)
                
                logger.info(f"从长桥自选股分组获取到 {len(stock_codes)} 只股票: {', '.join(stock_codes)}")
                return stock_codes
            else:
                logger.warning(f"长桥自选股分组 '{target_group.name if target_group else 'unknown'}' 中没有股票")
                return []
                
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测限流
            if any(keyword in error_msg for keyword in ['限流', 'limit', 'quota', '配额']):
                logger.warning(f"Longbridge 速率限制: {e}")
                raise RateLimitError(f"Longbridge 速率限制: {e}") from e
            
            # 检测权限问题
            if any(keyword in error_msg for keyword in ['权限', 'permission', '无权限']):
                logger.warning(f"Longbridge 权限不足: {e}")
                raise DataFetchError(f"Longbridge 权限不足，请开通行情权限: {e}") from e
            
            raise DataFetchError(f"获取长桥自选股分组失败: {e}") from e
    
    def get_realtime_quote_batch(self, symbols: list[str]) -> Optional[dict[str, dict[str, Optional]]]:
        """
        批量获取实时行情（替代 ak.stock_zh_a_spot_em）
        
        限制：
        - 每次请求最多 500 个标的
        - 需要配置 LONGPORT_APP_KEY, LONGPORT_APP_SECRET, LONGPORT_ACCESS_TOKEN
        - 只返回 A 股数据（.SH, .SZ 结尾）
        
        Args:
            symbols: 股票代码列表，如 ['600519.SH', '000001.SZ', '700.HK']
                   如果传入纯数字代码，会自动转换为 Longbridge 格式
            
        Returns:
            Dict[str, Dict[str, Any]]: 股票代码到行情数据的映射
            格式: {
                '600519.SH': {
                    '代码': '600519.SH',
                    '名称': '贵州茅台',
                    '最新价': 1850.0,
                    '昨收价': 1845.0,
                    '今开': 1852.0,
                    '最高': 1860.0,
                    '最低': 1848.0,
                    '成交量': 1234567,
                    '成交额': 2288888888.0,
                    '涨跌幅': 0.27,
                    '涨跌额': 5.0,
                    '振幅': 0.65,
                    ...
                }
            }
        """
        if not self._api:
            logger.warning("Longbridge API 未初始化，无法获取实时行情")
            return None
        
        # 过滤并转换股票代码为 Longbridge 格式
        longbridge_symbols = []
        for symbol in symbols:
            # 转换为 Longbridge 格式
            lb_symbol = self._convert_stock_code(symbol)
            # 只保留 A 股（.SH, .SZ 结尾）
            if lb_symbol.endswith('.SH') or lb_symbol.endswith('.SZ'):
                longbridge_symbols.append(lb_symbol)
        
        if not longbridge_symbols:
            logger.warning(f"没有 A 股代码需要获取实时行情")
            return None
        
        logger.info(f"[Longbridge] 准备获取 {len(longbridge_symbols)} 只 A 股的实时行情")
        
        try:
            # 速率限制检查
            self._rate_limiter.acquire()
            
            # 分批查询（每批最多 500 个）
            batch_size = 500
            all_quotes = {}
            
            for i in range(0, len(longbridge_symbols), batch_size):
                batch = longbridge_symbols[i:i + batch_size]
                
                logger.debug(f"[Longbridge] 调用 quote() 接口，批次 {i//batch_size + 1}，{len(batch)} 只股票")
                
                # 调用 Longbridge API
                resp = self._api.quote(batch)
                
                # 解析响应
                for quote in resp:
                    symbol = str(quote.symbol)
                    
                    # 计算涨跌幅
                    last_done = float(quote.last_done) if quote.last_done else 0.0
                    prev_close = float(quote.prev_close) if quote.prev_close else 0.0
                    change_pct = 0.0
                    if prev_close > 0:
                        change_pct = (last_done - prev_close) / prev_close * 100
                    
                    change_amount = last_done - prev_close
                    
                    # 计算振幅
                    high = float(quote.high) if quote.high else 0.0
                    low = float(quote.low) if quote.low else 0.0
                    amplitude = 0.0
                    if prev_close > 0:
                        amplitude = (high - low) / prev_close * 100
                    
                    all_quotes[symbol] = {
                        '代码': symbol,
                        '名称': '',  # 需要从 STOCK_NAME_MAP 或其他接口获取
                        '最新价': last_done,
                        '昨收价': prev_close,
                        '今开': float(quote.open) if quote.open else 0.0,
                        '最高': high,
                        '最低': low,
                        '成交量': int(quote.volume) if quote.volume else 0,
                        '成交额': float(quote.turnover) if quote.turnover else 0.0,
                        '涨跌幅': round(change_pct, 2),
                        '涨跌额': round(change_amount, 2),
                        '振幅': round(amplitude, 2),
                        '量比': 0.0,  # 需要额外计算
                        '换手率': 0.0,  # 需要额外计算
                        '市盈率-动态': 0.0,  # 需要额外获取
                        '市净率': 0.0,  # 需要额外获取
                        '总市值': 0.0,  # 需要额外获取
                        '流通市值': 0.0,  # 需要额外获取
                        '涨速': 0.0,
                        '5分钟涨跌': 0.0,
                        '60日涨跌幅': 0.0,
                        '年初至今涨跌幅': 0.0,
                    }
                
                logger.info(f"[Longbridge] 批次 {i//batch_size + 1} 完成，获取到 {len(resp)} 只股票的实时行情")
            
            logger.info(f"[Longbridge] 实时行情获取完成，共 {len(all_quotes)} 只股票")
            return all_quotes
            
        except Exception as e:
            logger.error(f"[Longbridge] 获取实时行情失败: {e}")
            return None
    
    def get_realtime_quote_with_indexes(self, symbols: list[str]) -> Optional[dict[str, dict[str, Optional]]]:
        """
        使用 calc_indexes 接口获取实时行情和计算指标（推荐使用）
        
        该接口返回的计算指标包括：
        - 最新价、涨跌额、涨跌幅
        - 成交量、成交额
        - 换手率、量比、振幅
        - 总市值、流入资金
        - 市盈率(TTM)、市净率
        - 股息率(TTM)
        - 五日/十日/半年涨幅
        - 五分钟涨幅
        
        Args:
            symbols: 股票代码列表，如 ['600519.SH', '000001.SZ']
                   如果传入纯数字代码，会自动转换为 Longbridge 格式
                   
        Returns:
            Dict[str, Dict[str, Any]]: 股票代码到行情数据的映射
            格式: {
                '600519.SH': {
                    '代码': '600519.SH',
                    '最新价': 1850.0,
                    '涨跌额': 5.0,
                    '涨跌幅': 0.27,
                    '成交量': 1234567,
                    '成交额': 2288888888.0,
                    '换手率': 0.15,
                    '量比': 1.2,
                    '振幅': 0.65,
                    '总市值': 2345678900000.0,
                    '流入资金': 123456789.0,
                    '市盈率': 21.26,
                    '市净率': 31.71,
                    '股息率': 0.64,
                    '五日涨幅': -9.76,
                    '十日涨幅': -11.87,
                    '半年涨幅': -7.01,
                    '五分钟涨幅': 0.0,
                    ...
                }
            }
        """
        if not self._api:
            logger.warning("Longbridge API 未初始化，无法获取实时行情")
            return None
        
        # 转换股票代码为 Longbridge 格式
        longbridge_symbols = []
        for symbol in symbols:
            lb_symbol = self._convert_stock_code(symbol)
            longbridge_symbols.append(lb_symbol)
        
        logger.info(f"[Longbridge] 准备获取 {len(longbridge_symbols)} 只股票的实时行情和计算指标")
        
        try:
            # 速率限制检查
            self._rate_limiter.acquire()
            
            # 导入 CalcIndex 枚举
            from longport.openapi import CalcIndex
            
            # 指定需要获取的计算指标
            # 主要指标：最新价、涨跌、成交量、成交额、换手率、量比、振幅、市值、PE、PB
            calc_indexes = [
                CalcIndex.LastDone,           # 最新价
                CalcIndex.ChangeValue,          # 涨跌额
                CalcIndex.ChangeRate,         # 涨跌幅
                CalcIndex.Volume,             # 成交量
                CalcIndex.Turnover,           # 成交额
                CalcIndex.TurnoverRate,       # 换手率
                CalcIndex.VolumeRatio,        # 量比
                CalcIndex.Amplitude,          # 振幅
                CalcIndex.TotalMarketValue,   # 总市值
                CalcIndex.CapitalFlow,        # 资金流入
                CalcIndex.PeTtmRatio,         # 市盈率(TTM)
                CalcIndex.PbRatio,            # 市净率
                CalcIndex.FiveDayChangeRate,  # 五日涨幅
                CalcIndex.TenDayChangeRate,   # 十日涨幅
                CalcIndex.HalfYearChangeRate, # 半年涨幅
                CalcIndex.FiveMinutesChangeRate,  # 五分钟涨幅
            ]
            
            logger.debug(f"[Longbridge] 调用 calc_indexes() 接口，{len(longbridge_symbols)} 只股票，{len(calc_indexes)} 个指标")
            
            # 调用 Longbridge API
            resp = self._api.calc_indexes(longbridge_symbols, calc_indexes)
            
            # 解析响应
            all_quotes = {}
            for quote in resp:
                symbol = str(quote.symbol)
                
                # 映射字段名到中文（与 Akshare 保持一致）
                all_quotes[symbol] = {
                    '代码': symbol,
                    '最新价': float(quote.last_done) if quote.last_done else 0.0,
                    '涨跌额': float(quote.change_value) if quote.change_value else 0.0,
                    '涨跌幅': float(quote.change_rate) if quote.change_rate else 0.0,
                    '成交量': int(quote.volume) if quote.volume else 0,
                    '成交额': float(quote.turnover) if quote.turnover else 0.0,
                    '换手率': float(quote.turnover_rate) if quote.turnover_rate else 0.0,
                    '量比': float(quote.volume_ratio) if quote.volume_ratio else 0.0,
                    '振幅': float(quote.amplitude) if quote.amplitude else 0.0,
                    '总市值': float(quote.total_market_value) if quote.total_market_value else 0.0,
                    '流入资金': float(quote.capital_flow) if quote.capital_flow else 0.0,
                    '市盈率': float(quote.pe_ttm_ratio) if quote.pe_ttm_ratio else 0.0,
                    '市净率': float(quote.pb_ratio) if quote.pb_ratio else 0.0,
                    '股息率': float(quote.dividend_ratio_ttm) if quote.dividend_ratio_ttm else 0.0,
                    '五日涨幅': float(quote.five_day_change_rate) if quote.five_day_change_rate else 0.0,
                    '十日涨幅': float(quote.ten_day_change_rate) if quote.ten_day_change_rate else 0.0,
                    '半年涨幅': float(quote.half_year_change_rate) if quote.half_year_change_rate else 0.0,
                    '五分钟涨幅': float(quote.five_minutes_change_rate) if quote.five_minutes_change_rate else 0.0,
                }
            
            logger.info(f"[Longbridge] calc_indexes 获取成功，共 {len(all_quotes)} 只股票")
            return all_quotes
            
        except Exception as e:
            logger.error(f"[Longbridge] calc_indexes 获取实时行情失败: {e}")
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = LongbridgeFetcher()
    
    if not fetcher.is_available():
        print("LongbridgeFetcher 不可用，请检查配置")
    else:
        try:
            # 测试获取000547数据
            df = fetcher.get_daily_data('000547', start_date='2025-12-01', end_date='2025-12-31')
            print(f"获取成功，共 {len(df)} 条数据")
            print(df.columns)
            print("----------------------")
            print(df.tail())
        except Exception as e:
            print(f"获取失败: {e}")