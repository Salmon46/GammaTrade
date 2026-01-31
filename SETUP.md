# Setup & Installation Guide

## Prerequisites

- **Docker** & **Docker Compose**: The primary runtime environment.
- **Python 3.9+**: For local strategy development.
- **CMake 3.10+** & **G++**: For local C++ development.
- **Redis**: Required for local testing without Docker.

## Environment Configuration

Create a `.env` file in the root directory:

```ini
# Exchange Keys
ALPACA_API_KEY=your_key
ALPACA_SECRET_KEY=your_secret
COINBASE_API_KEY=your_key

# System Config
REDIS_HOST=localhost
REDIS_PORT=6379
log_level=INFO
```

## Running the Stack

The entire system is containerized. To start:

```bash
docker-compose up --build
```

To stop:

```bash
docker-compose down
```

## Local Development

### Python Components

1. Create a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # or venv\Scripts\activate on Windows
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

   *(Note: You may need to compile the protobufs first using `common/compile_protos.sh`)*

### C++ Components

1. Navigate to the router directory:

   ```bash
   cd processing/execution_router
   ```

2. Build with CMake:

   ```bash
   mkdir build && cd build
   cmake ..
   make
   ```
