import unittest
from common.python.metrics import TICKS_RECEIVED, SERVICE_STATUS, ORDER_LATENCY

class TestMetrics(unittest.TestCase):
    def test_counter_increment(self):
        # We can't easily reset prometheus metrics between tests in a simple way
        # but we can check if they accept values
        TICKS_RECEIVED.labels(symbol='BTC-USD', source='coinbase', type='trade').inc()
        # No exception means it works
    
    def test_gauge_set(self):
        SERVICE_STATUS.labels(service_name='test_service').set(1.0)
        # No exception means it works

    def test_histogram_observe(self):
        ORDER_LATENCY.labels(adapter='test_adapter').observe(0.05)
        # No exception means it works

if __name__ == '__main__':
    unittest.main()
