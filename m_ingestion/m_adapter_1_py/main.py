"""
M-Adapter 1: Alpaca Market Data Provider

This adapter connects to the Alpaca API to fetch real-time market data,
normalizes it to Protobuf format, and publishes to Redis channels.
"""

import os
import sys
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Optional, List, Callable

import redis
from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.data.enums import DataFeed

# Add common directory to path for Protobuf imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'common', 'python'))
from common.python import metrics

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('m_adapter_1_alpaca')


class MarketDataProvider(ABC):
    """Abstract base class for market data providers."""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: List[str]) -> None:
        """Subscribe to market data for the given symbols."""
        pass
    
    @abstractmethod
    def normalize(self, raw_data: dict) -> bytes:
        """Normalize raw data to Protobuf format and return serialized bytes."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the data source."""
        pass


class AlpacaProvider(MarketDataProvider):
    """Alpaca market data provider implementation."""
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        redis_client: redis.Redis,
        redis_channel: str = 'market_data_1',
        data_feed: DataFeed = DataFeed.IEX
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redis_client = redis_client
        self.redis_channel = redis_channel
        self.data_feed = data_feed
        
        self.stock_stream: Optional[StockDataStream] = None
        self.crypto_stream: Optional[CryptoDataStream] = None
        self._running = False
        
        # Import Protobuf after path is set
        try:
            from common.python.market_data_pb2 import MarketData
            self.MarketData = MarketData
        except ImportError:
            logger.warning("Protobuf bindings not found. Run compile_protos.sh first.")
            self.MarketData = None
    
    async def connect(self) -> None:
        """Establish connection to Alpaca data streams."""
        logger.info("Connecting to Alpaca data streams...")
        
        self.stock_stream = StockDataStream(
            self.api_key,
            self.api_secret,
            feed=self.data_feed
        )
        
        self.crypto_stream = CryptoDataStream(
            self.api_key,
            self.api_secret
        )
        
        self._running = True
        logger.info("Connected to Alpaca successfully.")
    
    async def subscribe(self, symbols: List[str]) -> None:
        """Subscribe to market data for the given symbols."""
        stock_symbols = [s for s in symbols if '/' not in s and '-' not in s]
        crypto_symbols = [s for s in symbols if '/' in s or '-' in s]
        
        # Subscribe to stock trades and quotes using modern alpaca-py API
        if stock_symbols and self.stock_stream:
            logger.info(f"Subscribing to stock symbols: {stock_symbols}")
            
            # Define handlers
            async def on_stock_trade(trade):
                self._handle_trade(trade, 'stock')
            
            async def on_stock_quote(quote):
                await self._handle_quote(quote, 'stock')
            
            # Subscribe using the modern API
            self.stock_stream.subscribe_trades(on_stock_trade, *stock_symbols)
            self.stock_stream.subscribe_quotes(on_stock_quote, *stock_symbols)
        
        # Subscribe to crypto trades
        if crypto_symbols and self.crypto_stream:
            logger.info(f"Subscribing to crypto symbols: {crypto_symbols}")
            
            async def on_crypto_trade(trade):
                self._handle_trade(trade, 'crypto')
            
            self.crypto_stream.subscribe_trades(on_crypto_trade, *crypto_symbols)
    
    def _handle_trade(self, trade, asset_type: str) -> None:
        """Handle incoming trade data."""
        try:
            # Metric: Tick Received
            metrics.TICKS_RECEIVED.labels(
                symbol=trade.symbol, 
                source=f'alpaca_{asset_type}',
                type='trade'
            ).inc()

            with metrics.PROCESSING_TIME.labels(source=f'alpaca_{asset_type}').time():
                data = {
                    'type': 'TRADE',
                    'symbol': trade.symbol,
                    'price': float(trade.price),
                    'volume': float(trade.size),
                    'timestamp': int(trade.timestamp.timestamp() * 1000),
                    'source': f'alpaca_{asset_type}'
                }
                
                serialized = self.normalize(data)
            
            self.redis_client.publish(self.redis_channel, serialized)
            
            # Metric: Tick Published
            metrics.TICKS_PUBLISHED.labels(
                symbol=trade.symbol, 
                source=f'alpaca_{asset_type}'
            ).inc()

            logger.debug(f"Published trade: {trade.symbol} @ {trade.price}")
            
        except Exception as e:
            metrics.ERRORS.labels(component='m_adapter_1', error_type='trade_processing').inc()
            logger.error(f"Error handling trade: {e}")
    
    async def _handle_quote(self, quote, asset_type: str) -> None:
        """Handle incoming quote data."""
        try:
            # Metric: Quote Received
            metrics.TICKS_RECEIVED.labels(
                symbol=quote.symbol, 
                source=f'alpaca_{asset_type}',
                type='quote'
            ).inc()

            with metrics.PROCESSING_TIME.labels(source=f'alpaca_{asset_type}').time():
                data = {
                    'type': 'QUOTE',
                    'symbol': quote.symbol,
                    'bid_price': float(quote.bid_price),
                    'ask_price': float(quote.ask_price),
                    'price': (float(quote.bid_price) + float(quote.ask_price)) / 2,
                    'timestamp': int(quote.timestamp.timestamp() * 1000),
                    'source': f'alpaca_{asset_type}'
                }
                
                serialized = self.normalize(data)
            
            self.redis_client.publish(self.redis_channel, serialized)

            # Metric: Quote Published
            metrics.TICKS_PUBLISHED.labels(
                symbol=quote.symbol, 
                source=f'alpaca_{asset_type}'
            ).inc()
            
            logger.debug(f"Published quote: {quote.symbol}")
            
        except Exception as e:
            metrics.ERRORS.labels(component='m_adapter_1', error_type='quote_processing').inc()
            logger.error(f"Error handling quote: {e}")
    
    def normalize(self, raw_data: dict) -> bytes:
        """Normalize raw data to Protobuf format and return serialized bytes."""
        if self.MarketData is None:
            # Fallback to JSON if Protobuf not available
            import json
            return json.dumps(raw_data).encode('utf-8')
        
        msg = self.MarketData()
        msg.type = raw_data.get('type', 'UNKNOWN')
        msg.symbol = raw_data.get('symbol', '')
        msg.price = raw_data.get('price', 0.0)
        msg.bid_price = raw_data.get('bid_price', 0.0)
        msg.ask_price = raw_data.get('ask_price', 0.0)
        msg.volume = raw_data.get('volume', 0.0)
        msg.timestamp = raw_data.get('timestamp', int(time.time() * 1000))
        msg.source = raw_data.get('source', 'alpaca')
        
        return msg.SerializeToString()
    
    async def disconnect(self) -> None:
        """Disconnect from Alpaca data streams."""
        self._running = False
        metrics.SERVICE_STATUS.labels(service_name='m_adapter_1').set(0)
        
        if self.stock_stream:
            await self.stock_stream.close()
        
        if self.crypto_stream:
            await self.crypto_stream.close()
        
        logger.info("Disconnected from Alpaca.")
    
    async def run(self) -> None:
        """Run the data streams."""
        metrics.SERVICE_STATUS.labels(service_name='m_adapter_1').set(1)
        tasks = []
        
        if self.stock_stream:
            tasks.append(asyncio.create_task(self._run_stream(self.stock_stream, 'stock')))
        
        if self.crypto_stream:
            tasks.append(asyncio.create_task(self._run_stream(self.crypto_stream, 'crypto')))
        
        if tasks:
            await asyncio.gather(*tasks)
    
    async def _run_stream(self, stream, name: str) -> None:
        """Run a single data stream with reconnection logic."""
        while self._running:
            try:
                logger.info(f"Starting {name} stream...")
                await stream._run_forever()
            except Exception as e:
                metrics.ERRORS.labels(component='m_adapter_1', error_type='stream_disconnect').inc()
                logger.error(f"{name} stream error: {e}")
                if self._running:
                    logger.info(f"Reconnecting {name} stream in 5 seconds...")
                    await asyncio.sleep(5)


async def main():
    """Main entry point for the Alpaca adapter."""
    # Start Prometheus metrics server
    metrics.start_metrics_server(8000)

    # Load configuration from environment
    api_key = os.getenv('ALPACA_API_KEY')
    api_secret = os.getenv('ALPACA_API_SECRET')
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    if not api_key or not api_secret:
        logger.error("ALPACA_API_KEY and ALPACA_API_SECRET must be set")
        sys.exit(1)
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=False  # We're sending binary Protobuf data
    )
    
    try:
        redis_client.ping()
        logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)
    
    # Create and run the provider
    provider = AlpacaProvider(
        api_key=api_key,
        api_secret=api_secret,
        redis_client=redis_client,
        redis_channel='market_data_1'
    )
    
    # Default symbols to subscribe to
    symbols = os.getenv('SYMBOLS', 'AAPL,MSFT,GOOGL,BTC/USD,ETH/USD').split(',')
    
    try:
        await provider.connect()
        await provider.subscribe(symbols)
        await provider.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await provider.disconnect()
        redis_client.close()


if __name__ == '__main__':
    asyncio.run(main())
