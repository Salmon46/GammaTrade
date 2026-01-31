"""
Data Logger - Persists completed trades to TimescaleDB

This service subscribes to the trades_completed channel and writes
trade records to the database for historical analysis.
"""

import os
import sys
import asyncio
import logging
import time
from typing import Optional, Dict, Any
from datetime import datetime

import redis
import asyncpg

# Add common directory to path for Protobuf imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'common', 'python'))

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_logger')


# SQL for creating the historical_trades table
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS historical_trades (
    id SERIAL,
    signal_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    total_value DECIMAL(18, 8) NOT NULL,
    commission DECIMAL(18, 8) DEFAULT 0,
    strategy_id VARCHAR(50),
    adapter VARCHAR(50),
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

-- Create hypertable for time-series optimization (TimescaleDB)
SELECT create_hypertable('historical_trades', 'timestamp', 
                         if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON historical_trades (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_strategy ON historical_trades (strategy_id, timestamp DESC);
"""


class DataLogger:
    """Persists trade data to TimescaleDB."""
    
    def __init__(
        self,
        redis_client: redis.Redis,
        db_pool: asyncpg.Pool,
        trades_channel: str = 'trades_completed'
    ):
        self.redis_client = redis_client
        self.db_pool = db_pool
        self.trades_channel = trades_channel
        
        self._running = False
        self._pubsub = None
        
        # Statistics
        self.trades_logged = 0
        self.errors = 0
        
        # Import Protobuf
        try:
            from common.python.execution_report_pb2 import TradeCompleted
            self.TradeCompleted = TradeCompleted
            self._protobuf_available = True
        except ImportError:
            logger.warning("Protobuf bindings not found.")
            self._protobuf_available = False
    
    async def initialize_schema(self):
        """Create database tables if they don't exist."""
        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute(CREATE_TABLE_SQL)
                logger.info("Database schema initialized")
            except Exception as e:
                # TimescaleDB might not be installed, try without hypertable
                logger.warning(f"TimescaleDB hypertable creation failed: {e}")
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_trades (
                        id SERIAL PRIMARY KEY,
                        signal_id VARCHAR(100) NOT NULL,
                        order_id VARCHAR(100) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        side VARCHAR(10) NOT NULL,
                        quantity DECIMAL(18, 8) NOT NULL,
                        price DECIMAL(18, 8) NOT NULL,
                        total_value DECIMAL(18, 8) NOT NULL,
                        commission DECIMAL(18, 8) DEFAULT 0,
                        strategy_id VARCHAR(50),
                        adapter VARCHAR(50),
                        timestamp TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                logger.info("Database schema initialized (without TimescaleDB)")
    
    def _deserialize_trade(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Deserialize trade completed message."""
        if self._protobuf_available:
            try:
                msg = self.TradeCompleted()
                msg.ParseFromString(data)
                return {
                    'signal_id': msg.signal_id,
                    'order_id': msg.order_id,
                    'symbol': msg.symbol,
                    'side': msg.side,
                    'quantity': msg.quantity,
                    'price': msg.price,
                    'total_value': msg.total_value,
                    'commission': msg.commission,
                    'strategy_id': msg.strategy_id,
                    'adapter': msg.adapter,
                    'timestamp': msg.timestamp
                }
            except Exception as e:
                logger.error(f"Failed to parse trade: {e}")
                return None
        
        # Fallback to JSON
        try:
            import json
            return json.loads(data.decode('utf-8'))
        except:
            return None
    
    async def _log_trade(self, trade: Dict[str, Any]):
        """Insert trade record into database."""
        try:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(trade['timestamp'] / 1000.0)
            
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO historical_trades 
                    (signal_id, order_id, symbol, side, quantity, price, 
                     total_value, commission, strategy_id, adapter, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    trade['signal_id'],
                    trade['order_id'],
                    trade['symbol'],
                    trade['side'],
                    trade['quantity'],
                    trade['price'],
                    trade['total_value'],
                    trade.get('commission', 0),
                    trade.get('strategy_id', ''),
                    trade.get('adapter', ''),
                    timestamp
                )
            
            self.trades_logged += 1
            logger.info(f"Logged trade: {trade['side']} {trade['quantity']} "
                       f"{trade['symbol']} @ {trade['price']}")
            
        except Exception as e:
            self.errors += 1
            logger.error(f"Failed to log trade: {e}")
    
    async def _process_message(self, data: bytes):
        """Process an incoming trade message."""
        trade = self._deserialize_trade(data)
        if trade:
            await self._log_trade(trade)
    
    async def run(self):
        """Run the data logger."""
        self._running = True
        
        # Initialize database schema
        await self.initialize_schema()
        
        # Subscribe to trades channel
        self._pubsub = self.redis_client.pubsub()
        self._pubsub.subscribe(self.trades_channel)
        
        logger.info("Data Logger started")
        logger.info(f"Listening on: {self.trades_channel}")
        
        while self._running:
            try:
                message = self._pubsub.get_message(timeout=0.1)
                
                if message and message['type'] == 'message':
                    await self._process_message(message['data'])
                
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1)
        
        self._pubsub.close()
        
        logger.info(f"Data Logger stopped. Logged: {self.trades_logged}, Errors: {self.errors}")
    
    async def stop(self):
        """Stop the data logger."""
        self._running = False


async def main():
    """Main entry point."""
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = int(os.getenv('POSTGRES_PORT', 5432))
    db_name = os.getenv('POSTGRES_DB', 'gammatrade')
    db_user = os.getenv('POSTGRES_USER', 'gammatrade')
    db_password = os.getenv('POSTGRES_PASSWORD', 'gammatrade')
    
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
    
    # Connect to PostgreSQL
    try:
        db_pool = await asyncpg.create_pool(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            min_size=2,
            max_size=10
        )
        logger.info(f"Connected to PostgreSQL at {db_host}:{db_port}/{db_name}")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        sys.exit(1)
    
    # Create and run data logger
    data_logger = DataLogger(
        redis_client=redis_client,
        db_pool=db_pool
    )
    
    try:
        await data_logger.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await data_logger.stop()
        await db_pool.close()
        redis_client.close()


if __name__ == '__main__':
    asyncio.run(main())
