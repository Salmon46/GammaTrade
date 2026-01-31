"""
O-Adapter 1: Open-Meteo Weather Data Provider

This adapter polls the Open-Meteo API for weather data,
normalizes it to Protobuf format, and publishes to Redis channels.

Weather data can be used for commodities/energy trading strategies
(e.g., heating oil demand correlates with cold weather).
"""

import os
import sys
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

import redis
import aiohttp

# Add common directory to path for Protobuf imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'common', 'python'))

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('o_adapter_1_openmeteo')


class ExternalDataProvider(ABC):
    """Abstract base class for external data providers."""
    
    @abstractmethod
    async def fetch(self) -> Dict[str, Any]:
        """Fetch data from the external source."""
        pass
    
    @abstractmethod
    def normalize(self, raw_data: Dict[str, Any]) -> bytes:
        """Normalize raw data to Protobuf format and return serialized bytes."""
        pass


class OpenMeteoProvider(ExternalDataProvider):
    """Open-Meteo weather data provider implementation."""
    
    # Weather code to condition mapping
    WEATHER_CODES = {
        0: 'CLEAR',           # Clear sky
        1: 'CLEAR',           # Mainly clear
        2: 'CLOUDY',          # Partly cloudy
        3: 'CLOUDY',          # Overcast
        45: 'FOG',            # Fog
        48: 'FOG',            # Depositing rime fog
        51: 'RAIN',           # Drizzle: Light
        53: 'RAIN',           # Drizzle: Moderate
        55: 'RAIN',           # Drizzle: Dense
        61: 'RAIN',           # Rain: Slight
        63: 'RAIN',           # Rain: Moderate
        65: 'RAIN',           # Rain: Heavy
        66: 'RAIN',           # Freezing Rain: Light
        67: 'RAIN',           # Freezing Rain: Heavy
        71: 'SNOW',           # Snow fall: Slight
        73: 'SNOW',           # Snow fall: Moderate
        75: 'SNOW',           # Snow fall: Heavy
        77: 'SNOW',           # Snow grains
        80: 'RAIN',           # Rain showers: Slight
        81: 'RAIN',           # Rain showers: Moderate
        82: 'STORM',          # Rain showers: Violent
        85: 'SNOW',           # Snow showers: Slight
        86: 'SNOW',           # Snow showers: Heavy
        95: 'STORM',          # Thunderstorm
        96: 'STORM',          # Thunderstorm with slight hail
        99: 'STORM',          # Thunderstorm with heavy hail
    }
    
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_channel: str = 'external_data_1',
        latitude: float = 40.7128,   # New York City default
        longitude: float = -74.0060,
        location_name: str = 'NYC',
        poll_interval: int = 60      # Poll every 60 seconds
    ):
        self.redis_client = redis_client
        self.redis_channel = redis_channel
        self.latitude = latitude
        self.longitude = longitude
        self.location_name = location_name
        self.poll_interval = poll_interval
        
        self.base_url = os.getenv(
            'OPEN_METEO_BASE_URL',
            'https://api.open-meteo.com/v1/forecast'
        )
        
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Import Protobuf after path is set
        try:
            from common.python.external_data_pb2 import ExternalData, ExternalDataType, WeatherCondition
            self.ExternalData = ExternalData
            self.ExternalDataType = ExternalDataType
            self.WeatherCondition = WeatherCondition
        except ImportError:
            logger.warning("Protobuf bindings not found. Run compile_protos.sh first.")
            self.ExternalData = None
            self.ExternalDataType = None
            self.WeatherCondition = None
    
    async def fetch(self) -> Dict[str, Any]:
        """Fetch current weather data from Open-Meteo API."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        
        params = {
            'latitude': self.latitude,
            'longitude': self.longitude,
            'current': 'temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m',
            'timezone': 'auto'
        }
        
        try:
            async with self._session.get(self.base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    current = data.get('current', {})
                    
                    weather_code = current.get('weather_code', 0)
                    condition = self.WEATHER_CODES.get(weather_code, 'UNKNOWN')
                    
                    return {
                        'type': 'WEATHER',
                        'source': 'open-meteo',
                        'location': self.location_name,
                        'latitude': self.latitude,
                        'longitude': self.longitude,
                        'temperature_celsius': current.get('temperature_2m', 0.0),
                        'humidity_percent': current.get('relative_humidity_2m', 0.0),
                        'wind_speed_kmh': current.get('wind_speed_10m', 0.0),
                        'condition': condition,
                        'weather_code': weather_code,
                        'timestamp': int(time.time() * 1000)
                    }
                else:
                    logger.error(f"Open-Meteo API error: {response.status}")
                    return {}
                    
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            return {}
    
    def normalize(self, raw_data: Dict[str, Any]) -> bytes:
        """Normalize raw data to Protobuf format and return serialized bytes."""
        if self.ExternalData is None:
            # Fallback to JSON if Protobuf not available
            import json
            return json.dumps(raw_data).encode('utf-8')
        
        msg = self.ExternalData()
        msg.type = self.ExternalDataType.WEATHER
        msg.source = raw_data.get('source', 'open-meteo')
        msg.location = raw_data.get('location', '')
        msg.latitude = raw_data.get('latitude', 0.0)
        msg.longitude = raw_data.get('longitude', 0.0)
        msg.timestamp = raw_data.get('timestamp', int(time.time() * 1000))
        
        # Weather-specific fields
        msg.temperature_celsius = raw_data.get('temperature_celsius', 0.0)
        msg.humidity_percent = raw_data.get('humidity_percent', 0.0)
        msg.wind_speed_kmh = raw_data.get('wind_speed_kmh', 0.0)
        
        # Map condition string to enum
        condition_map = {
            'CLEAR': self.WeatherCondition.CLEAR,
            'CLOUDY': self.WeatherCondition.CLOUDY,
            'RAIN': self.WeatherCondition.RAIN,
            'SNOW': self.WeatherCondition.SNOW,
            'STORM': self.WeatherCondition.STORM,
            'FOG': self.WeatherCondition.FOG,
        }
        msg.condition = condition_map.get(
            raw_data.get('condition', 'UNKNOWN'),
            self.WeatherCondition.UNKNOWN_WEATHER
        )
        
        return msg.SerializeToString()
    
    async def run(self) -> None:
        """Run the polling loop."""
        self._running = True
        logger.info(f"Starting weather data polling (interval: {self.poll_interval}s)")
        logger.info(f"Location: {self.location_name} ({self.latitude}, {self.longitude})")
        
        while self._running:
            try:
                # Fetch weather data
                raw_data = await self.fetch()
                
                if raw_data:
                    # Normalize and publish
                    serialized = self.normalize(raw_data)
                    self.redis_client.publish(self.redis_channel, serialized)
                    
                    logger.info(
                        f"Published weather: {self.location_name} - "
                        f"{raw_data.get('temperature_celsius', 0):.1f}°C, "
                        f"{raw_data.get('condition', 'UNKNOWN')}"
                    )
                
                # Wait for next poll
                await asyncio.sleep(self.poll_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
        
        # Cleanup
        if self._session:
            await self._session.close()
    
    async def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False


async def main():
    """Main entry point for the Open-Meteo adapter."""
    # Load configuration from environment
    redis_host = os.getenv('REDIS_HOST', 'localhost')
    redis_port = int(os.getenv('REDIS_PORT', 6379))
    latitude = float(os.getenv('OPEN_METEO_LATITUDE', 40.7128))
    longitude = float(os.getenv('OPEN_METEO_LONGITUDE', -74.0060))
    location_name = os.getenv('LOCATION_NAME', 'NYC')
    poll_interval = int(os.getenv('POLL_INTERVAL', 60))
    
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
    
    # Create and run the provider
    provider = OpenMeteoProvider(
        redis_client=redis_client,
        redis_channel='external_data_1',
        latitude=latitude,
        longitude=longitude,
        location_name=location_name,
        poll_interval=poll_interval
    )
    
    try:
        await provider.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await provider.stop()
        redis_client.close()


if __name__ == '__main__':
    asyncio.run(main())
