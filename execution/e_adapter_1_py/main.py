"""
E-Adapter 1: Alpaca Execution Adapter

This adapter receives trade signals from the Execution Router,
executes orders via the Alpaca API, and publishes execution reports.
"""

import os
import sys
import asyncio
import logging
import time
from typing import Optional, Dict, Any
import uuid

import redis
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType

# Add common directory to path for Protobuf imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'common', 'python'))

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('e_adapter_1_alpaca')


class AlpacaExecutionAdapter:
    """Alpaca order execution adapter."""
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        redis_client: redis.Redis,
        exec_channel: str = 'exec_queue_alpaca',
        report_channel: str = 'execution_report',
        paper: bool = True
    ):
        self.redis_client = redis_client
        self.exec_channel = exec_channel
        self.report_channel = report_channel
        self.paper = paper
        
        # Initialize Alpaca client
        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=api_secret,
            paper=paper
        )
        
        self._running = False
        self._pubsub = None
        
        # Import Protobuf classes
        try:
            from common.python.trade_signal_pb2 import TradeSignal, TradeAction, OrderType as PbOrderType
            from common.python.execution_report_pb2 import ExecutionReport, ExecutionStatus
            self.TradeSignal = TradeSignal
            self.TradeAction = TradeAction
            self.PbOrderType = PbOrderType
            self.ExecutionReport = ExecutionReport
            self.ExecutionStatus = ExecutionStatus
            self._protobuf_available = True
        except ImportError:
            logger.warning("Protobuf bindings not found.")
            self._protobuf_available = False
    
    def _deserialize_signal(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize trade signal from Protobuf."""
        if self._protobuf_available:
            try:
                msg = self.TradeSignal()
                msg.ParseFromString(data)
                return {
                    'signal_id': msg.signal_id,
                    'symbol': msg.symbol,
                    'action': 'BUY' if msg.action == self.TradeAction.BUY else 'SELL',
                    'quantity': msg.quantity,
                    'order_type': 'MARKET' if msg.order_type == self.PbOrderType.MARKET else 'LIMIT',
                    'limit_price': msg.limit_price,
                    'stop_price': msg.stop_price,
                    'strategy_id': msg.strategy_id,
                    'confidence': msg.confidence,
                    'reason': msg.reason,
                    'time_in_force': msg.time_in_force
                }
            except Exception as e:
                logger.error(f"Failed to parse signal: {e}")
                return None
        
        # Fallback to JSON
        try:
            import json
            return json.loads(data.decode('utf-8'))
        except:
            return None
    
    def _serialize_report(self, report_data: Dict[str, Any]) -> bytes:
        """Serialize execution report to Protobuf."""
        if self._protobuf_available:
            msg = self.ExecutionReport()
            msg.signal_id = report_data.get('signal_id', '')
            msg.order_id = report_data.get('order_id', '')
            msg.symbol = report_data.get('symbol', '')
            
            status_map = {
                'PENDING': self.ExecutionStatus.PENDING,
                'SUBMITTED': self.ExecutionStatus.SUBMITTED,
                'PARTIAL_FILL': self.ExecutionStatus.PARTIAL_FILL,
                'FILLED': self.ExecutionStatus.FILLED,
                'CANCELLED': self.ExecutionStatus.CANCELLED,
                'REJECTED': self.ExecutionStatus.REJECTED,
                'FAILED': self.ExecutionStatus.FAILED,
            }
            msg.status = status_map.get(report_data.get('status', 'PENDING'), 
                                        self.ExecutionStatus.PENDING)
            
            msg.requested_quantity = report_data.get('requested_quantity', 0.0)
            msg.filled_quantity = report_data.get('filled_quantity', 0.0)
            msg.average_fill_price = report_data.get('average_fill_price', 0.0)
            msg.remaining_quantity = report_data.get('remaining_quantity', 0.0)
            msg.commission = report_data.get('commission', 0.0)
            msg.adapter = 'alpaca'
            msg.timestamp = report_data.get('timestamp', int(time.time() * 1000))
            msg.error_message = report_data.get('error_message', '')
            
            return msg.SerializeToString()
        else:
            import json
            return json.dumps(report_data).encode('utf-8')
    
    async def execute_order(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an order via Alpaca API."""
        symbol = signal['symbol']
        quantity = signal['quantity']
        side = OrderSide.BUY if signal['action'] == 'BUY' else OrderSide.SELL
        
        logger.info(f"Executing order: {signal['action']} {quantity} {symbol}")
        
        try:
            # Extract TimeInForce from signal, default to Day
            tif_val = signal.get('time_in_force', 0) 
            # Proto enum: DAY=0, GTC=1, IOC=2, FOK=3
            tif_map = {
                0: TimeInForce.DAY,
                1: TimeInForce.GTC,
                2: TimeInForce.IOC,
                3: TimeInForce.FOK
            }
            tif = tif_map.get(tif_val, TimeInForce.DAY)

            if signal.get('order_type', 'MARKET') == 'MARKET':
                order_request = MarketOrderRequest(
                    symbol=symbol,
                    qty=quantity,
                    side=side,
                    time_in_force=tif
                )
            else:
                order_request = LimitOrderRequest(
                    symbol=symbol,
                    qty=quantity,
                    side=side,
                    time_in_force=tif,
                    limit_price=signal.get('limit_price', 0.0)
                )
            
            # Submit order
            order = self.trading_client.submit_order(order_request)
            
            logger.info(f"Order submitted: {order.id} - Status: {order.status}")
            
            return {
                'signal_id': signal['signal_id'],
                'order_id': str(order.id),
                'symbol': symbol,
                'status': 'SUBMITTED' if order.status.value == 'new' else order.status.value.upper(),
                'requested_quantity': quantity,
                'filled_quantity': float(order.filled_qty or 0),
                'average_fill_price': float(order.filled_avg_price or 0),
                'remaining_quantity': quantity - float(order.filled_qty or 0),
                'commission': 0.0,
                'timestamp': int(time.time() * 1000)
            }
            
        except Exception as e:
            logger.error(f"Order execution failed: {e}")
            return {
                'signal_id': signal['signal_id'],
                'order_id': '',
                'symbol': symbol,
                'status': 'FAILED',
                'requested_quantity': quantity,
                'filled_quantity': 0,
                'average_fill_price': 0,
                'remaining_quantity': quantity,
                'commission': 0,
                'timestamp': int(time.time() * 1000),
                'error_message': str(e)
            }
    
    async def _process_signal(self, data: bytes):
        """Process an incoming trade signal."""
        signal = self._deserialize_signal(data)
        if not signal:
            return
        
        logger.info(f"Received signal: {signal['signal_id']} - "
                   f"{signal['action']} {signal['quantity']} {signal['symbol']}")
        
        # Execute the order
        report = await self.execute_order(signal)
        
        # Publish execution report
        serialized = self._serialize_report(report)
        self.redis_client.publish(self.report_channel, serialized)
        
        logger.info(f"Published report: {report['status']} for {signal['signal_id']}")
    
    async def run(self):
        """Run the execution adapter."""
        self._running = True
        
        # Subscribe to execution queue
        self._pubsub = self.redis_client.pubsub()
        self._pubsub.subscribe(self.exec_channel)
        
        mode = "PAPER" if self.paper else "LIVE"
        logger.info(f"Alpaca Execution Adapter started ({mode} mode)")
        logger.info(f"Listening on: {self.exec_channel}")
        logger.info(f"Publishing to: {self.report_channel}")
        
        while self._running:
            try:
                message = self._pubsub.get_message(timeout=0.1)
                
                if message and message['type'] == 'message':
                    await self._process_signal(message['data'])
                
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)
        
        self._pubsub.close()
    
    async def stop(self):
        """Stop the adapter."""
        self._running = False


async def main():
    """Main entry point."""
    api_key = os.getenv('ALPACA_API_KEY')
    api_secret = os.getenv('ALPACA_API_SECRET')
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    paper = os.getenv('PAPER_TRADING', 'true').lower() == 'true'
    
    if not api_key or not api_secret:
        logger.error("ALPACA_API_KEY and ALPACA_API_SECRET must be set")
        sys.exit(1)
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=False
    )
    
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Create and run adapter
    adapter = AlpacaExecutionAdapter(
        api_key=api_key,
        api_secret=api_secret,
        redis_client=redis_client,
        paper=paper
    )
    
    try:
        await adapter.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await adapter.stop()
        redis_client.close()


if __name__ == '__main__':
    asyncio.run(main())
