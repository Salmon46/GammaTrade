#!/bin/bash
# Compile Protocol Buffer schemas for Python and C++

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR"
PYTHON_OUT="$SCRIPT_DIR/python"
CPP_OUT="$SCRIPT_DIR/cpp"

# Detect protoc
if command -v protoc &> /dev/null; then
    PROTOC="protoc"
else
    # Fallback for Windows host (if applicable)
    PROTOC="/c/ProgramData/chocolatey/lib/protoc/tools/bin/protoc.exe"
fi

# Create output directories
mkdir -p "$PYTHON_OUT"
mkdir -p "$CPP_OUT"

echo "Compiling Protocol Buffer schemas..."

# Compile for Python
for proto_file in "$PROTO_DIR"/*.proto; do
    if [ -f "$proto_file" ]; then
        echo "  Python: $(basename "$proto_file")"
        "$PROTOC" --proto_path="$PROTO_DIR" --python_out="$PYTHON_OUT" "$proto_file"
    fi
done

# Compile for C++
for proto_file in "$PROTO_DIR"/*.proto; do
    if [ -f "$proto_file" ]; then
        echo "  C++: $(basename "$proto_file")"
        "$PROTOC" --proto_path="$PROTO_DIR" --cpp_out="$CPP_OUT" "$proto_file"
    fi
done

# Create Python __init__.py
cat > "$PYTHON_OUT/__init__.py" << 'EOF'
# Auto-generated Protocol Buffer bindings
from .market_data_pb2 import MarketData, MarketDataBatch
from .trade_signal_pb2 import TradeSignal, TradeAction, OrderType, TimeInForce
from .external_data_pb2 import ExternalData, ExternalDataType, WeatherCondition
from .execution_report_pb2 import ExecutionReport, TradeCompleted, ExecutionStatus
from .strategy_config_pb2 import StrategyConfig

__all__ = [
    'MarketData', 'MarketDataBatch',
    'TradeSignal', 'TradeAction', 'OrderType', 'TimeInForce',
    'ExternalData', 'ExternalDataType', 'WeatherCondition',
    'ExecutionReport', 'TradeCompleted', 'ExecutionStatus',
    'StrategyConfig',
]
EOF

echo "Done! Generated files in:"
echo "  Python: $PYTHON_OUT"
echo "  C++: $CPP_OUT"
