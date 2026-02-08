"""
测试用例
"""
import pytest
import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

import pandas as pd

from data.storage import StockStorage
from data.collector import StockCollector
from data.models import DailyData


class TestStockStorage:
    """测试 StockStorage 类"""
    
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        # 清理
        if os.path.exists(path):
            os.remove(path)
    
    @pytest.fixture
    def storage(self, temp_db):
        """创建存储实例"""
        return StockStorage(temp_db)
    
    def test_init_creates_tables(self, storage):
        """测试初始化时创建表"""
        # 如果表不存在会抛出异常
        result = storage.get_all_symbols()
        assert isinstance(result, list)
    
    def test_save_and_get_daily_data(self, storage):
        """测试保存和获取日线数据"""
        symbol = "600519"
        
        # 创建测试数据
        test_data = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03', '2024-01-04'],
            'open': [100.0, 101.0, 102.0],
            'high': [105.0, 106.0, 107.0],
            'low': [99.0, 100.0, 101.0],
            'close': [103.0, 104.0, 105.0],
            'volume': [1000000, 1100000, 1200000],
            'amount': [103000000, 114400000, 126000000],
            'turnover_rate': [0.5, 0.6, 0.7]
        })
        
        # 保存数据
        storage.save_daily_data(symbol, test_data)
        
        # 获取最新日期
        latest = storage.get_latest_date(symbol)
        assert latest == '2024-01-04'
        
        # 获取最早日期
        oldest = storage.get_oldest_date(symbol)
        assert oldest == '2024-01-02'
        
        # 获取数据条数
        count = storage.get_data_count(symbol)
        assert count == 3
    
    def test_incremental_update(self, storage):
        """测试增量更新"""
        symbol = "000001"
        
        # 第一次保存
        data1 = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03'],
            'open': [10.0, 11.0],
            'high': [12.0, 13.0],
            'low': [9.0, 10.0],
            'close': [11.0, 12.0],
            'volume': [1000, 1100],
            'amount': [11000, 13200]
        })
        storage.save_daily_data(symbol, data1)
        
        # 第二次保存（新数据）
        data2 = pd.DataFrame({
            'date': ['2024-01-04', '2024-01-05'],
            'open': [12.0, 13.0],
            'high': [14.0, 15.0],
            'low': [11.0, 12.0],
            'close': [13.0, 14.0],
            'volume': [1200, 1300],
            'amount': [15600, 18200]
        })
        storage.save_daily_data(symbol, data2)
        
        # 验证数据条数
        count = storage.get_data_count(symbol)
        assert count == 4
        
        # 验证最新日期
        latest = storage.get_latest_date(symbol)
        assert latest == '2024-01-05'
    
    def test_get_data_range(self, storage):
        """测试获取指定范围的数据"""
        symbol = "600036"
        
        data = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
            'open': [10.0, 11.0, 12.0, 13.0],
            'high': [12.0, 13.0, 14.0, 15.0],
            'low': [9.0, 10.0, 11.0, 12.0],
            'close': [11.0, 12.0, 13.0, 14.0],
            'volume': [1000, 1100, 1200, 1300],
            'amount': [11000, 13200, 15600, 18200]
        })
        storage.save_daily_data(symbol, data)
        
        # 获取范围数据
        result = storage.get_data_range(symbol, '2024-01-03', '2024-01-04')
        
        assert len(result) == 2
        assert result.iloc[0]['date'] == '2024-01-03'
        assert result.iloc[1]['date'] == '2024-01-04'
    
    def test_get_all_symbols(self, storage):
        """测试获取所有股票代码"""
        # 保存多只股票数据
        for symbol in ['600519', '000001', '600036']:
            data = pd.DataFrame({
                'date': ['2024-01-02'],
                'open': [100.0],
                'high': [105.0],
                'low': [99.0],
                'close': [103.0],
                'volume': [1000000],
                'amount': [103000000]
            })
            storage.save_daily_data(symbol, data)
        
        symbols = storage.get_all_symbols()
        
        assert len(symbols) == 3
        assert '600519' in symbols
        assert '000001' in symbols
        assert '600036' in symbols
    
    def test_symbol_exists(self, storage):
        """测试检查股票是否存在"""
        symbol = "601857"
        
        assert storage.symbol_exists(symbol) is False
        
        data = pd.DataFrame({
            'date': ['2024-01-02'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.0],
            'close': [10.5],
            'volume': [1000],
            'amount': [10500]
        })
        storage.save_daily_data(symbol, data)
        
        assert storage.symbol_exists(symbol) is True
    
    def test_delete_symbol_data(self, storage):
        """测试删除股票数据"""
        symbol = "601988"
        
        data = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03'],
            'open': [10.0, 11.0],
            'high': [12.0, 13.0],
            'low': [9.0, 10.0],
            'close': [11.0, 12.0],
            'volume': [1000, 1100],
            'amount': [11000, 13200]
        })
        storage.save_daily_data(symbol, data)
        
        assert storage.symbol_exists(symbol) is True
        assert storage.get_data_count(symbol) == 2
        
        storage.delete_symbol_data(symbol)
        
        assert storage.symbol_exists(symbol) is False
        assert storage.get_data_count(symbol) == 0
    
    def test_empty_data_handling(self, storage):
        """测试空数据处理"""
        symbol = "000000"
        
        # 空 DataFrame
        empty_data = pd.DataFrame()
        storage.save_daily_data(symbol, empty_data)
        
        # 应该不抛出异常
        assert storage.get_data_count(symbol) == 0
        assert storage.get_latest_date(symbol) is None
    
    def test_replace_existing_data(self, storage):
        """测试替换已有数据"""
        symbol = "600000"
        
        # 第一次保存
        data1 = pd.DataFrame({
            'date': ['2024-01-02'],
            'open': [10.0],
            'high': [12.0],
            'low': [9.0],
            'close': [11.0],
            'volume': [1000],
            'amount': [11000]
        })
        storage.save_daily_data(symbol, data1)
        
        # 更新同一天的数据
        data2 = pd.DataFrame({
            'date': ['2024-01-02'],
            'open': [10.5],  # 不同的值
            'high': [12.5],
            'low': [9.5],
            'close': [11.5],
            'volume': [1200],
            'amount': [13800]
        })
        storage.save_daily_data(symbol, data2)
        
        # 数据条数应该不变
        assert storage.get_data_count(symbol) == 1
        
        # 获取最新数据验证
        result = storage.get_data_range(symbol, '2024-01-02', '2024-01-02')
        assert result.iloc[0]['close'] == 11.5


class TestStockCollector:
    """测试 StockCollector 类"""
    
    @pytest.fixture
    def temp_db(self):
        """创建临时数据库"""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.remove(path)
    
    @pytest.fixture
    def storage(self, temp_db):
        """创建存储实例"""
        return StockStorage(temp_db)
    
    @pytest.fixture
    def collector(self, storage):
        """创建采集器实例"""
        return StockCollector(
            storage=storage,
            start_date="2024-01-01",
            end_date="2024-01-31"
        )
    
    def test_collector_init(self, collector):
        """测试采集器初始化"""
        assert collector.storage is not None
        assert collector.start_date == "2024-01-01"
        assert collector.delay == 0.5
    
    def test_format_date(self, collector):
        """测试日期格式化"""
        assert collector._format_date("2024-01-15") == "2024-01-15"
        assert collector._format_date("2024/01/15") == "2024-01-15"
        assert collector._format_date("20240115") == "2024-01-15"
        assert collector._format_date("") is None
        assert collector._format_date(None) is None
    
    def test_process_data(self, collector):
        """测试数据处理"""
        raw_data = pd.DataFrame({
            '日期': ['2024-01-02', '2024-01-03'],
            '开盘': [100.0, 101.0],
            '最高': [105.0, 106.0],
            '最低': [99.0, 100.0],
            '收盘': [103.0, 104.0],
            '成交量': [1000000, 1100000],
            '成交额': [103000000, 114400000],
            '换手率': [0.5, 0.6]
        })
        
        processed = collector._process_data(raw_data, "600519")
        
        assert 'date' in processed.columns
        assert 'open' in processed.columns
        assert len(processed) == 2
        assert processed.iloc[0]['date'] == "2024-01-02"
    
    def test_process_empty_data(self, collector):
        """测试处理空数据"""
        empty_data = pd.DataFrame()
        result = collector._process_data(empty_data, "600519")
        assert result.empty
    
    def test_process_data_missing_columns(self, collector):
        """测试处理缺少列的数据"""
        incomplete_data = pd.DataFrame({
            '日期': ['2024-01-02'],
            '开盘': [100.0]
        })
        
        result = collector._process_data(incomplete_data, "600519")
        assert result.empty
    
    def test_collect_stock_with_mock(self, storage):
        """使用 Mock 测试采集股票"""
        collector = StockCollector(storage)
        
        # Mock akshare 返回数据
        mock_data = pd.DataFrame({
            '日期': ['2024-01-02', '2024-01-03'],
            '开盘': [100.0, 101.0],
            '最高': [105.0, 106.0],
            '最低': [99.0, 100.0],
            '收盘': [103.0, 104.0],
            '成交量': [1000000, 1100000],
            '成交额': [103000000, 114400000],
            '换手率': [0.5, 0.6]
        })
        
        with patch('akshare.stock_zh_a_hist', return_value=mock_data):
            count = collector.collect_stock("600519")
            
            assert count == 2
            assert storage.symbol_exists("600519")
            assert storage.get_data_count("600519") == 2
    
    def test_collect_stock_no_new_data(self, storage):
        """测试采集无新数据的股票"""
        collector = StockCollector(storage)
        
        # 先保存一些数据
        existing_data = pd.DataFrame({
            'date': ['2024-01-02'],
            'open': [100.0],
            'high': [105.0],
            'low': [99.0],
            'close': [103.0],
            'volume': [1000000],
            'amount': [103000000]
        })
        storage.save_daily_data("000001", existing_data)
        
        # Mock 返回空数据
        with patch('akshare.stock_zh_a_hist', return_value=pd.DataFrame()):
            count = collector.collect_stock("000001")
            
            assert count == 0
    
    def test_collect_incremental_data(self, storage):
        """测试增量采集"""
        collector = StockCollector(storage, start_date="2024-01-01", end_date="2024-01-31")
        
        # 先保存一些数据
        existing_data = pd.DataFrame({
            'date': ['2024-01-02', '2024-01-03'],
            'open': [100.0, 101.0],
            'high': [105.0, 106.0],
            'low': [99.0, 100.0],
            'close': [103.0, 104.0],
            'volume': [1000000, 1100000],
            'amount': [103000000, 114400000]
        })
        storage.save_daily_data("600036", existing_data)
        
        # Mock 返回新数据
        mock_data = pd.DataFrame({
            '日期': ['2024-01-04', '2024-01-05'],
            '开盘': [104.0, 105.0],
            '最高': [107.0, 108.0],
            '最低': [103.0, 104.0],
            '收盘': [106.0, 107.0],
            '成交量': [1300000, 1400000],
            '成交额': [138000000, 154000000],
            '换手率': [0.8, 0.9]
        })
        
        with patch('akshare.stock_zh_a_hist', return_value=mock_data):
            count = collector.collect_stock("600036")
            
            assert count == 2
            # 验证总数
            assert storage.get_data_count("600036") == 4


class TestDailyDataModel:
    """测试 DailyData 模型"""
    
    def test_daily_data_creation(self):
        """测试创建 DailyData 对象"""
        data = DailyData(
            symbol="600519",
            date="2024-01-02",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            amount=103000000,
            turnover_rate=0.5
        )
        
        assert data.symbol == "600519"
        assert data.date == "2024-01-02"
        assert data.open == 100.0
        assert data.close == 103.0
    
    def test_daily_data_repr(self):
        """测试 DailyData 字符串表示"""
        data = DailyData(
            symbol="600519",
            date="2024-01-02",
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            amount=103000000
        )
        
        assert repr(data) == "<DailyData 600519 2024-01-02>"
    
    def test_daily_data_optional_fields(self):
        """测试可选字段"""
        data = DailyData(
            symbol="000001",
            date="2024-01-03",
            open=10.0,
            high=11.0,
            low=9.0,
            close=10.5,
            volume=100000,
            amount=1050000
            # turnover_rate 可选，不传入
        )
        
        assert data.turnover_rate is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
