#!/bin/bash
set -e

# Compile Protobuf schemas if common directory exists
if [ -d "/app/common" ]; then
    echo "Compiling Protobuf schemas..."
    bash /app/common/compile_protos.sh
else
    echo "Warning: /app/common directory not found, skipping proto compilation."
fi

# Execute the passed command
exec "$@"
