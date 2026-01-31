#!/bin/bash
# C++ Entrypoint Script for GammaTrade Services
# Builds the project on startup (to work with volume mounts) then runs the binary

set -e

# Build the project if CMakeLists.txt exists
if [ -f "CMakeLists.txt" ]; then
    echo "[entrypoint] Building C++ project..."
    rm -rf build
    mkdir -p build
    cd build
    cmake ..
    make -j$(nproc)
    cd ..
    echo "[entrypoint] Build complete!"
fi

# Execute the passed command
exec "$@"
