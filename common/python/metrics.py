import logging
from prometheus_client import start_http_server, Counter, Histogram, Gauge

logger = logging.getLogger('metrics')

# Standard Metrics
TICKS_RECEIVED = Counter(
    'gammatrade_ticks_received_total', 
    'Total number of market ticks received',
    ['symbol', 'source', 'type']
)

TICKS_PUBLISHED = Counter(
    'gammatrade_ticks_published_total',
    'Total number of normalized ticks published to Redis',
    ['symbol', 'source']
)

PROCESSING_TIME = Histogram(
    'gammatrade_tick_processing_seconds',
    'Time spent processing/normalizing a tick',
    ['source']
)

ERRORS = Counter(
    'gammatrade_errors_total',
    'Total number of errors encountered',
    ['component', 'error_type']
)

SIGNALS_GENERATED = Counter(
    'gammatrade_signals_generated_total',
    'Total number of trade signals generated',
    ['symbol', 'action', 'strategy']
)

# Latency Metrics (Sync with C++)
ORDER_LATENCY = Histogram(
    'gammatrade_order_latency_seconds',
    'Latency of order execution in seconds',
    ['adapter'],
    buckets=(.001, .005, .01, .05, .1, .5, 1.0, 5.0, float("inf"))
)

SIGNAL_LATENCY = Histogram(
    'gammatrade_signal_latency_seconds',
    'Latency of signal generation in seconds',
    ['strategy'],
    buckets=(.0001, .0005, .001, .005, .01, .05, .1, float("inf"))
)

SERVICE_STATUS = Gauge(
    'gammatrade_service_status', 
    'Service status (1=Up, 0=Down)', 
    ['service_name']
)

# Business / Strategy Metrics
PORTFOLIO_VALUE = Gauge(
    'gammatrade_portfolio_value',
    'Total portfolio value (Cash + Unrealized)',
    ['currency']
)

CURRENT_POSITION = Gauge(
    'gammatrade_position_quantity',
    'Current position size',
    ['symbol']
)

UNREALIZED_PNL = Gauge(
    'gammatrade_pnl_unrealized',
    'Unrealized Profit/Loss',
    ['symbol']
)

REALIZED_PNL = Gauge(
    'gammatrade_pnl_realized',
    'Realized Profit/Loss',
    ['symbol']
)

MARKET_PRICE = Gauge(
    'gammatrade_market_price',
    'Current composite market price',
    ['symbol']
)

INDICATOR_VALUE = Gauge(
    'gammatrade_indicator_value',
    'Technical indicator value',
    ['symbol', 'indicator']
)

def set_service_status(service_name, status):
    """Set the service status metric."""
    SERVICE_STATUS.labels(service_name=service_name).set(status)

def record_error(component, error_type):
    """Record an error metric."""
    ERRORS.labels(component=component, error_type=error_type).inc()

def start_metrics_server(port=8000):
    """Start the Prometheus metrics server."""
    try:
        start_http_server(port)
        logger.info(f"Metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")