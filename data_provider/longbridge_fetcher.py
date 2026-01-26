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
        
        # 转换代码格式
        symbol = self._convert_stock_code(stock_code)
        
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