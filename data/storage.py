"""
数据存储模块
使用 SQLite 数据库存储股票日线数据
"""
import os
from datetime import datetime
from typing import List, Optional
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from .models import DailyData


class StockStorage:
    """股票数据存储类"""
    
    def __init__(self, db_path: str = "stocks.db"):
        """
        初始化存储实例
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.engine: Engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.create_tables()
    
    @contextmanager
    def session_scope(self):
        """提供事务性会话"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def create_tables(self):
        """创建数据库表"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_data (
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    turnover_rate REAL,
                    PRIMARY KEY (symbol, date)
                )
            """))
            
            # 创建索引以提高查询性能
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_symbol_date 
                ON daily_data(symbol, date)
            """))
            conn.commit()
    
    def save_daily_data(self, symbol: str, data: pd.DataFrame):
        """
        保存股票日线数据到数据库
        
        Args:
            symbol: 股票代码
            data: 包含日线数据的 DataFrame
        """
        if data.empty:
            return
        
        # 确保数据按日期排序
        data = data.sort_values('date')
        
        # 使用 upsert 逻辑：存在则更新，不存在则插入
        with self.session_scope() as session:
            for _, row in data.iterrows():
                session.execute(text("""
                    INSERT OR REPLACE INTO daily_data 
                    (symbol, date, open, high, low, close, volume, amount, turnover_rate)
                    VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :amount, :turnover_rate)
                """), {
                    'symbol': symbol,
                    'date': str(row['date']),
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume'],
                    'amount': row['amount'],
                    'turnover_rate': row.get('turnover_rate', None)
                })
    
    def get_latest_date(self, symbol: str) -> Optional[str]:
        """
        获取指定股票的最新交易日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            最新日期字符串 (YYYY-MM-DD) 或 None
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(date) FROM daily_data WHERE symbol = :symbol
            """), {'symbol': symbol}).fetchone()
            return result[0] if result else None
    
    def get_oldest_date(self, symbol: str) -> Optional[str]:
        """
        获取指定股票的最早交易日期
        
        Args:
            symbol: 股票代码
            
        Returns:
            最早日期字符串 (YYYY-MM-DD) 或 None
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MIN(date) FROM daily_data WHERE symbol = :symbol
            """), {'symbol': symbol}).fetchone()
            return result[0] if result else None
    
    def get_all_symbols(self) -> List[str]:
        """
        获取所有已存储的股票代码
        
        Returns:
            股票代码列表
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT symbol FROM daily_data ORDER BY symbol
            """)).fetchall()
            return [row[0] for row in result]
    
    def get_data_range(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定日期范围内的数据
        
        Args:
            symbol: 股票代码
            start_date: 起始日期
            end_date: 结束日期
            
        Returns:
            DataFrame
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT * FROM daily_data 
                WHERE symbol = :symbol AND date >= :start_date AND date <= :end_date
                ORDER BY date
            """), {
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date
            }).fetchall()
            
            if not result:
                return pd.DataFrame()
            
            columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'turnover_rate']
            return pd.DataFrame(result, columns=columns)
    
    def symbol_exists(self, symbol: str) -> bool:
        """
        检查股票代码是否存在
        
        Args:
            symbol: 股票代码
            
        Returns:
            是否存在
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 1 FROM daily_data WHERE symbol = :symbol LIMIT 1
            """), {'symbol': symbol}).fetchone()
            return result is not None
    
    def delete_symbol_data(self, symbol: str):
        """
        删除指定股票的所有数据
        
        Args:
            symbol: 股票代码
        """
        with self.engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM daily_data WHERE symbol = :symbol
            """), {'symbol': symbol})
            conn.commit()
    
    def get_data_count(self, symbol: str) -> int:
        """
        获取指定股票的数据条数
        
        Args:
            symbol: 股票代码
            
        Returns:
            数据条数
        """
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM daily_data WHERE symbol = :symbol
            """), {'symbol': symbol}).fetchone()
            return result[0] if result else 0
