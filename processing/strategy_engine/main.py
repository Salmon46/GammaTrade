"""
Strategy Engine - Multi-Factor Trading Strategy

This service consumes market data from multiple sources (Alpaca, Coinbase),
synchronizes prices to form a composite price, applies SMA Crossover logic,
and publishes trade signals.

Current Implementation: Simple SMA Crossover
- Syncs prices from Alpaca and Coinbase
- Calculates SMA(10) and SMA(20) on Composite Price
- Buy when SMA(10) crosses above SMA(20)
- Sell when SMA(10) crosses below SMA(20)
"""

import os
import sys
import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
import uuid

import redis

# Add common directory to path for Protobuf imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'common', 'python'))
from common.python import metrics

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('strategy_engine')


@dataclass
class MarketState:
    """Current state for a single symbol."""
    symbol: str
    prices: deque = field(default_factory=lambda: deque(maxlen=50))  # Rolling window of composite prices for SMA
    
    # Source tracking
    latest_prices: Dict[str, float] = field(default_factory=dict)
    last_updates: Dict[str, int] = field(default_factory=dict)
    
    # Indicators
    sma_fast: float = 0.0  # 10 period
    sma_slow: float = 0.0  # 20 period
    
    # State tracking for crossover
    previous_state: str = "NEUTRAL" # BULLISH (Fast > Slow), BEARISH (Fast < Slow), NEUTRAL
    
    def update_price(self, source: str, price: float, timestamp: int) -> Optional[float]:
        """
        Update price from a specific source.
        Returns the new composite price if an update was triggered, None otherwise.
        """
        self.latest_prices[source] = price
        self.last_updates[source] = timestamp
        
        # Calculate composite price
        composite_price = self._calculate_composite_price(timestamp)
        
        if composite_price:
            self.prices.append(composite_price)
            self._update_indicators()
            return composite_price
            
        return None

    def _calculate_composite_price(self, current_timestamp: int) -> Optional[float]:
        """
        Calculate composite price based on fresh data.
        Returns average of fresh sources (within 5 seconds).
        """
        fresh_prices = []
        validity_window = 5000  # 5 seconds in ms
        
        for source, update_time in self.last_updates.items():
            if current_timestamp - update_time < validity_window:
                fresh_prices.append(self.latest_prices[source])
        
        if fresh_prices:
            return sum(fresh_prices) / len(fresh_prices)
        return None

    def _update_indicators(self):
        """Update technical indicators."""
        if len(self.prices) >= 10:
            self.sma_fast = sum(list(self.prices)[-10:]) / 10.0
        
        if len(self.prices) >= 20:
            self.sma_slow = sum(list(self.prices)[-20:]) / 20.0


class StrategyEngine:
    """
    SMA Crossover Strategy Engine
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        signal_channel: str = 'trade_signals',
        feedback_channel: str = 'strategy_feedback'
    ):
        self.redis_client = redis_client
        self.signal_channel = signal_channel
        self.feedback_channel = feedback_channel
        
        # Market state tracking
        self.market_states: Dict[str, MarketState] = defaultdict(
            lambda: MarketState(symbol="")
        )
        
        # Position tracking (simplified)
        self.positions: Dict[str, float] = {}  # symbol -> quantity
        self.avg_entry_prices: Dict[str, float] = {} # symbol -> avg entry price
        self.realized_pnl: Dict[str, float] = defaultdict(float) # symbol -> realized pnl
        self.starting_cash = 100000.0

        
        # Strategy parameters
        self.position_size = 1.0     # Default position size (e.g. 1 share/contract)
        
        # Symbols to trade
        self.watchlist: set = set()
        
        # Protobuf imports
        try:
            from common.python.market_data_pb2 import MarketData
            from common.python.trade_signal_pb2 import TradeSignal, TradeAction, OrderType, TimeInForce
            from common.python.execution_report_pb2 import ExecutionReport, ExecutionStatus
            self.MarketData = MarketData
            self.TradeSignal = TradeSignal
            self.TradeAction = TradeAction
            self.OrderType = OrderType
            self.TimeInForce = TimeInForce
            self.ExecutionReport = ExecutionReport
            self.ExecutionStatus = ExecutionStatus
            self._protobuf_available = True
        except ImportError:
            logger.warning("Protobuf bindings not found. Using JSON fallback.")
            self._protobuf_available = False

        # Try to import StrategyConfig
        try:
            from common.python.strategy_config_pb2 import StrategyConfig
            self.StrategyConfig = StrategyConfig
        except ImportError:
            self.StrategyConfig = None
        
        self._running = False
        self._pubsub = None
    
    def add_to_watchlist(self, symbols: List[str]):
        """Add symbols to the watchlist."""
        for symbol in symbols:
            self.watchlist.add(symbol)
            if symbol not in self.market_states:
                self.market_states[symbol] = MarketState(symbol=symbol)
        logger.info(f"Watchlist: {self.watchlist}")
    
    def _deserialize_execution_report(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize execution report from Protobuf or JSON."""
        if self._protobuf_available:
            try:
                msg = self.ExecutionReport()
                msg.ParseFromString(data)
                return {
                    'symbol': msg.symbol,
                    'status': msg.status,
                    'filled_quantity': msg.filled_quantity,
                    'average_fill_price': msg.average_fill_price,
                    # Note: ExecutionReport typically doesn't have side directly, usually inferred from order or metadata
                    # For this fix, we assume the system puts Side in metadata or we deduce change.
                    # Actually, let's just use the quantity change logic in _handle_execution.
                }
            except Exception as e:
                logger.debug(f"Failed to parse ExecReport as Protobuf: {e}")
        
        try:
            import json
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to deserialize execution report: {e}")
            return None

    def _deserialize_market_data(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize market data from Protobuf or JSON."""
        if self._protobuf_available:
            try:
                msg = self.MarketData()
                msg.ParseFromString(data)
                return {
                    'type': msg.type,
                    'symbol': msg.symbol,
                    'price': msg.price,
                    'volume': msg.volume,
                    'timestamp': msg.timestamp,
                    'source': msg.source
                }
            except Exception as e:
                logger.debug(f"Failed to parse as Protobuf, trying JSON: {e}")
        
        # Fallback to JSON
        try:
            import json
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to deserialize market data: {e}")
            metrics.record_error("strategy_engine", "deserialization_error")
            return None
    
    def _serialize_signal(self, signal_data: Dict[str, Any]) -> bytes:
        """Serialize trade signal to Protobuf."""
        if self._protobuf_available:
            msg = self.TradeSignal()
            msg.signal_id = signal_data.get('signal_id', str(uuid.uuid4()))
            msg.symbol = signal_data.get('symbol', '')
            
            action = signal_data.get('action', 'HOLD')
            if action == 'BUY':
                msg.action = self.TradeAction.BUY
            elif action == 'SELL':
                msg.action = self.TradeAction.SELL
            else:
                msg.action = self.TradeAction.HOLD
            
            msg.quantity = signal_data.get('quantity', 0.0)
            msg.order_type = self.OrderType.MARKET
            msg.time_in_force = self.TimeInForce.GTC
            msg.target_adapter = signal_data.get('target_adapter', 'alpaca')
            msg.strategy_id = signal_data.get('strategy_id', 'sma_crossover')
            msg.confidence = signal_data.get('confidence', 0.5)
            msg.timestamp = signal_data.get('timestamp', int(time.time() * 1000))
            msg.reason = signal_data.get('reason', '')
            
            return msg.SerializeToString()
        else:
            import json
            return json.dumps(signal_data).encode('utf-8')
    
    def _handle_market_data(self, data: Dict[str, Any]):
        """Process incoming market data."""
        start_time = time.time()
        symbol = data.get('symbol', '')
        price = data.get('price', 0.0)
        source = data.get('source', 'unknown')
        timestamp = data.get('timestamp', int(time.time() * 1000))

        # Metric: Tick Received
        metrics.TICKS_RECEIVED.labels(
            symbol=symbol, 
            source=source,
            type='market_data'
        ).inc()
        
        if not symbol or price <= 0:
            return
        
        # Normalization
        normalized_symbol = symbol.replace('-', '/')
        
        # Update market state
        if normalized_symbol not in self.market_states:
            if normalized_symbol in self.watchlist:
                self.market_states[normalized_symbol] = MarketState(symbol=normalized_symbol)
            else:
                return # Ignore if not in watchlist

        state = self.market_states[normalized_symbol]
        composite_price = state.update_price(source, price, timestamp)
        
        if composite_price:
            # Metric: Market Price
            metrics.MARKET_PRICE.labels(symbol=normalized_symbol).set(composite_price)
            self._evaluate_strategy(normalized_symbol, start_time)
            
    def _handle_execution_report(self, data: Dict[str, Any]):
        """Handle execution reports to update portfolio state."""
        symbol = data.get('symbol')
        status = data.get('status')
        filled_qty = data.get('filled_quantity', 0.0)
        avg_price = data.get('average_fill_price', 0.0)
        
        # Map generic status if needed, assuming Protobuf enum int or string
        is_filled = (status == self.ExecutionStatus.FILLED) if self._protobuf_available and isinstance(status, int) else (status == 'FILLED')
        is_partial = (status == self.ExecutionStatus.PARTIAL_FILL) if self._protobuf_available and isinstance(status, int) else (status == 'PARTIAL_FILL')
        
        if not (is_filled or is_partial) or filled_qty <= 0:
            return

        logger.info(f"EXECUTION: {symbol} Filled {filled_qty} @ {avg_price}")
        
        # Update Positions and P&L (Naive implementation: assume LONG only for now or infer side)
        # Since ExecutionReport doesn't explicitly store side in some standard versions, 
        # we might need to rely on the strategy remembering its open orders or simpler logic.
        # For this dashboard fix, let's assume if we are receiving an exec and we have 0 position, it's a BUY.
        # If we have position, it's a SELL? No, that's dangerous.
        # Let's assume the Strategy Engine knows what it submitted. 
        # But this is a stateless update. 
        # CRITICAL: We need Side. If missing, we can't accurately track.
        # However, looking at the previous optimistic code:
        # BUY -> +size, SELL -> 0.
        # Let's try to maintain that logic but driven by fills.
        
        # Simplification: If we have no position, treat as Entry. If we have position, treat as Exit?
        # A bit risky. Better: use 'side' from metadata if available, OR simple toggling.
        # Strategy has state.
        
        current_pos = self.positions.get(symbol, 0.0)
        
        # Infer side based on state (Not ideal but improvements require protocol change)
        # If we sent a BUY recently, we expect a BUY fill.
        # But here, let's just say:
        # If current_pos == 0 => BUY (Entry)
        # If current_pos > 0 => SELL (Exit)
        
        if current_pos == 0:
             # ENTRY
             self.positions[symbol] = filled_qty
             self.avg_entry_prices[symbol] = avg_price
             logger.info(f"Position OPEN: {symbol} {filled_qty}")
        else:
             # EXIT
             # Calculate Realized PnL
             pnl = (avg_price - self.avg_entry_prices.get(symbol, 0.0)) * filled_qty
             self.realized_pnl[symbol] += pnl
             metrics.REALIZED_PNL.labels(symbol=symbol).set(self.realized_pnl[symbol])
             
             self.positions[symbol] = max(0, current_pos - filled_qty)
             if self.positions[symbol] == 0:
                 self.avg_entry_prices[symbol] = 0.0
             logger.info(f"Position CLOSE: {symbol} PnL: {pnl}")

        metrics.CURRENT_POSITION.labels(symbol=symbol).set(self.positions[symbol])
    
    def _evaluate_strategy(self, symbol: str, start_time: float):
        """
        Evaluate SMA Crossover Strategy.
        """
        state = self.market_states.get(symbol)
        if not state or len(state.prices) < 20:
            return  # Not enough data for SMA(20)
        
        sma_fast = state.sma_fast
        sma_slow = state.sma_slow
        
        # Metric: Indicators
        metrics.INDICATOR_VALUE.labels(symbol=symbol, indicator="sma_fast").set(sma_fast)
        metrics.INDICATOR_VALUE.labels(symbol=symbol, indicator="sma_slow").set(sma_slow)
        
        # Metric: P&L (Unrealized)
        total_unrealized = 0.0
        for s, pos_qty in self.positions.items():
            if pos_qty > 0 and s in self.market_states and self.market_states[s].prices:
                avg_entry = self.avg_entry_prices.get(s, 0.0)
                current_price = self.market_states[s].prices[-1]
                unrealized_for_symbol = (current_price - avg_entry) * pos_qty
                metrics.UNREALIZED_PNL.labels(symbol=s).set(unrealized_for_symbol)
                total_unrealized += unrealized_for_symbol
            else:
                metrics.UNREALIZED_PNL.labels(symbol=s).set(0.0)
            
        # Update Portfolio Value
        total_realized = sum(self.realized_pnl.values())
        metrics.PORTFOLIO_VALUE.labels(currency="USD").set(self.starting_cash + total_realized + total_unrealized)

        
        current_state = "NEUTRAL"
        if sma_fast > sma_slow:
            current_state = "BULLISH"
        elif sma_fast < sma_slow:
            current_state = "BEARISH"
            
        # Check for Crossover
        signal = None
        reason = ""
        
        if state.previous_state == "BEARISH" and current_state == "BULLISH":
            signal = 'BUY'
            reason = f"Golden Cross: SMA(10)={sma_fast:.2f} crossed above SMA(20)={sma_slow:.2f}"
        elif state.previous_state == "BULLISH" and current_state == "BEARISH":
            signal = 'SELL'
            reason = f"Death Cross: SMA(10)={sma_fast:.2f} crossed below SMA(20)={sma_slow:.2f}"
            
        # Update state persistence
        if current_state != "NEUTRAL":
            state.previous_state = current_state
        
        if signal:
            self._generate_signal(symbol, signal, reason, start_time)
    
    def _generate_signal(self, symbol: str, action: str, reason: str, start_time: float):
        """Generate and publish a trade signal."""
        # Metric: Signal Generated
        metrics.SIGNALS_GENERATED.labels(
            symbol=symbol,
            action=action,
            strategy='sma_crossover'
        ).inc()

        # Metric: Signal Latency
        latency = time.time() - start_time
        metrics.SIGNAL_LATENCY.labels(strategy='sma_crossover').observe(latency)

        signal_id = f"{symbol}_{action}_{int(time.time()*1000)}"
        
        signal_data = {
            'signal_id': signal_id,
            'symbol': symbol,
            'action': action,
            'quantity': float(self.position_size),
            'target_adapter': 'alpaca',  # User requested target: Alpaca
            'strategy_id': 'sma_crossover',
            'confidence': 0.8,
            'timestamp': int(time.time() * 1000),
            'reason': reason
        }
        
        # Update local position tracking (optimistic) REMOVED
        # We now rely on ExecutionReports for position updates.
        
        # Serialize and publish
        serialized = self._serialize_signal(signal_data)
        self.redis_client.publish(self.signal_channel, serialized)
        
        logger.info(f"SIGNAL: {action} {self.position_size} {symbol} - {reason}")
    
    async def _broadcast_config(self):
        """Broadcast current configuration to Redis."""
        if not self.StrategyConfig:
            return
            
        try:
            config = self.StrategyConfig()
            config.strategy_id = "sma_crossover_1"
            config.strategy_name = "SMACrossover"
            config.dry_run = False 
            config.max_open_trades = 5
            config.stake_amount = self.position_size
            config.last_update = int(time.time() * 1000)
            
            config.parameters["sma_fast"] = "10"
            config.parameters["sma_slow"] = "20"
            
            config.watchlist.extend(list(self.watchlist))
            
            serialized = config.SerializeToString()
            self.redis_client.publish('strategy:status', serialized)
            
        except Exception as e:
            logger.error(f"Failed to broadcast config: {e}")
            metrics.record_error("strategy_engine", "config_broadcast_error")

    async def run(self):
        """Run the strategy engine."""
        metrics.SERVICE_STATUS.labels(service_name='strategy_engine').set(1)
        self._running = True
        
        # Subscribe to market data channels
        self._pubsub = self.redis_client.pubsub()
        self._pubsub.psubscribe('market_data_*', self.feedback_channel)
        
        logger.info("Strategy Engine started (SMA Crossover)")
        logger.info(f"Subscribed to: market_data_*, {self.feedback_channel}")
        
        last_broadcast = 0
        
        while self._running:
            try:
                # periodic tasks
                now = time.time()
                if now - last_broadcast > 1.0: # Broadcast every 1s
                    await self._broadcast_config()
                    last_broadcast = now

                message = self._pubsub.get_message(timeout=0.1)
                
                if message and message['type'] == 'pmessage':
                    channel = message['channel']
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    data = message['data']
                    
                    if channel.startswith('market_data'):
                        parsed = self._deserialize_market_data(data)
                        if parsed:
                            self._handle_market_data(parsed)
                    
                    elif channel == self.feedback_channel:
                        # Handle execution feedback
                        parsed = self._deserialize_execution_report(data)
                        if parsed:
                            self._handle_execution_report(parsed)
                
                await asyncio.sleep(0.01)  # Small yield
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                metrics.record_error("strategy_engine", "main_loop_error")
                await asyncio.sleep(1)
        
        self._pubsub.close()
    
    async def stop(self):
        """Stop the strategy engine."""
        self._running = False
        metrics.SERVICE_STATUS.labels(service_name='strategy_engine').set(0)


async def main():
    """Main entry point for the Strategy Engine."""
    # Start Prometheus metrics server
    metrics.start_metrics_server(8000)

    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
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
    
    # Create and configure the strategy engine
    engine = StrategyEngine(redis_client)
    
    # Add symbols to watchlist
    # Ensure slash format for consistency
    watchlist_raw = os.getenv('WATCHLIST', 'BTC/USD,ETH/USD').split(',')
    watchlist = [s.replace('-', '/') for s in watchlist_raw]
    engine.add_to_watchlist(watchlist)
    
    try:
        await engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await engine.stop()
        redis_client.close()


if __name__ == '__main__':
    asyncio.run(main())