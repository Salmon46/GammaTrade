import unittest
from unittest.mock import MagicMock
import os
import sys

# Add relevant paths
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from main import StrategyEngine
from common.python import metrics

class TestStrategyMetrics(unittest.TestCase):
    def setUp(self):
        self.redis_mock = MagicMock()
        self.engine = StrategyEngine(self.redis_mock)
        self.engine.add_to_watchlist(['BTC/USD'])

    def test_ticks_received_metric(self):
        # We check if the metric can be called without error
        # In a real test we'd check the value if possible
        data = {
            'symbol': 'BTC/USD',
            'price': 50000.0,
            'source': 'test_source',
            'timestamp': 1000
        }
        self.engine._handle_market_data(data)
        # No crash means success for this simple check

if __name__ == '__main__':
    unittest.main()
