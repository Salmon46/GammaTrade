
import unittest
import json
import time
from unittest.mock import MagicMock
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from processing.strategy_engine.main import StrategyEngine

class TestSideTracking(unittest.TestCase):
    def setUp(self):
        self.mock_redis = MagicMock()
        self.engine = StrategyEngine(self.mock_redis, initial_cash=100000.0)
        
        # Mock Protobuf unavailability to test JSON fallback (easier here, or mock proto)
        # The engine logic handles both. Let's test the logic flow which is shared.
        # We'll use the _handle_execution_report method directly.
        
    def test_buy_execution(self):
        """Test that a BUY execution increases position and updates avg price."""
        symbol = "BTC/USD"
        
        # 1. Initial State
        self.assertEqual(self.engine.positions.get(symbol, 0), 0)
        
        # 2. Receive BUY Execution (quantity 1.0 @ 50000)
        exec_report = {
            'symbol': symbol,
            'status': 'FILLED',
            'filled_quantity': 1.0,
            'average_fill_price': 50000.0,
            'side': 'BUY'
        }
        
        # We need to bypass the protobuf/json deserialization and call handle directly
        # But _handle_execution_report expects a dict if deserialized, or it deserializes bytes?
        # The method _handle_execution_report takes a DICT. Wait, let's check source.
        # It takes 'data: Dict[str, Any]'. Yes.
        
        self.engine._handle_execution_report(exec_report)
        
        # 3. Verify State
        self.assertEqual(self.engine.positions[symbol], 1.0)
        self.assertEqual(self.engine.avg_entry_prices[symbol], 50000.0)
        
        # 4. Receive Second BUY (quantity 1.0 @ 60000)
        exec_report_2 = {
            'symbol': symbol,
            'status': 'FILLED',
            'filled_quantity': 1.0,
            'average_fill_price': 60000.0,
            'side': 'BUY'
        }
        self.engine._handle_execution_report(exec_report_2)
        
        # 5. Verify Average Price ((50000*1 + 60000*1) / 2 = 55000)
        self.assertEqual(self.engine.positions[symbol], 2.0)
        self.assertEqual(self.engine.avg_entry_prices[symbol], 55000.0)

    def test_sell_execution(self):
        """Test that a SELL execution decreases position and calculates PnL."""
        symbol = "ETH/USD"
        
        # 1. Setup Initial Long Position
        self.engine.positions[symbol] = 2.0
        self.engine.avg_entry_prices[symbol] = 3000.0
        
        # 2. Receive SELL Execution (quantity 1.0 @ 4000.0)
        exec_report = {
            'symbol': symbol,
            'status': 'FILLED',
            'filled_quantity': 1.0,
            'average_fill_price': 4000.0,
            'side': 'SELL'
        }
        
        self.engine._handle_execution_report(exec_report)
        
        # 3. Verify State
        # Remaining qty: 1.0
        self.assertEqual(self.engine.positions[symbol], 1.0)
        # Avg entry price should NOT change on reduce-only
        self.assertEqual(self.engine.avg_entry_prices[symbol], 3000.0)
        # Realized PnL: (4000 - 3000) * 1 = 1000
        self.assertEqual(self.engine.realized_pnl[symbol], 1000.0)
        
    def test_unknown_side_warning(self):
        """Test that missing side logs a warning and doesn't update positions."""
        symbol = "LTC/USD"
        
        exec_report = {
            'symbol': symbol,
            'status': 'FILLED',
            'filled_quantity': 1.0,
            'average_fill_price': 100.0,
            # No side
        }
        
        with self.assertLogs('strategy_engine', level='WARNING') as cm:
            self.engine._handle_execution_report(exec_report)
            
        self.assertTrue(any("missing side metadata" in o for o in cm.output))
        self.assertEqual(self.engine.positions.get(symbol, 0), 0)

if __name__ == '__main__':
    unittest.main()
