# System Architecture

GammaTrade follows a distributed microservices pattern, optimizing for ease of strategy development while maintaining high-performance execution standards.

## High-Level Data Flow

```mermaid
graph TD
    MD[Market Data Ingestion] -->|Ticks/Bars| Bus[Message Bus (Redis)]
    Bus -->|Market Data| Strat[Strategy Engine (Python)]
    Strat -->|Trade Signals| Bus
    Bus -->|Trade Signals| Router[Execution Router (C++)]
    Router -->|Validated Orders| Adapter[Execution Adapter]
    Adapter -->|API Request| Exchange[Exchange]
    Exchange -->|Fills/Updates| Adapter
    Adapter -->|Execution Reports| Bus
```

## Component Details

### 1. Ingestion Layer (`m_ingestion`, `o_ingestion`)

Responsible for normalizing external data feeds into internal Protobuf formats.

- **Market Data**: Websocket/REST connections to data providers (e.g., Alpaca, Coinbase).
- **Order Data**: Listens for fill updates and account state changes.

### 2. Processing Layer (`processing/`)

#### Strategy Engine (Python)

- Subscribes to market data.
- Runs technical analysis and quantitative models.
- Emits `TradeSignal` messages.

#### Execution Router (C++)

- **Performance Critical**: Written in C++ for minimal latency.
- **Risk Management**: Checks signal validity, position limits, and max drawdown rules before routing.
- **Routing**: Determines the appropriate execution adapter based on the asset class and venue.

### 3. Execution Layer (`execution/`)

- Stateless adapters that translate internal `Order` messages into venue-specific API calls.
- Handles rate limiting and connection maintenance.

### 4. Common Infrastructure (`common/`)

- Shared Protocol Buffer definitions (`.proto`) ensure strict typing across Python and C++ services.
- Shared utility libraries for Redis communication and logging.
