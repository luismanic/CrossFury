# TriFury - High Frequency Triangular Arbitrage Bot

TriFury is a high-performance triangular arbitrage bot written in Rust, designed to identify and evaluate profitable triangular arbitrage opportunities across cryptocurrency exchanges with ultra-low latency.

## Overview

Triangular arbitrage capitalizes on pricing discrepancies among three trading pairs on the same exchange. The trading route follows:

1. Convert Token A to Token B
2. Convert Token B to Token C
3. Convert Token C back to Token A

This bot focuses on monitoring and analyzing these opportunities in real-time, with a particular emphasis on perpetual futures markets.

## Features

- **Ultra-Fast Processing**: Runs at ~15,000+ checks per second, significantly faster than Python implementations
- **Real-time WebSocket Integration**: Maintains connections to exchange WebSockets for instant price updates
- **Synthetic Pair Support**: Creates synthetic trading pairs to expand arbitrage opportunities
- **Smart Prioritization**: Focuses computing resources on tokens showing recent price volatility
- **Configurable Focus Mode**: Can focus on a specific subset of tokens for improved performance
- **Detailed Logging**: Logs profitable opportunities with comprehensive metrics

## Requirements

- Rust 1.70.0 or higher
- Cargo package manager

## Usage

### Building from source

```bash
# Clone the repository
git clone https://github.com/yourusername/trifury.git
cd trifury

# Build in release mode for maximum performance
cargo build --release

# Run the application
cargo run --release
```

### Configuration

The main configuration options are available in `src/constants.rs`:

- `FOCUS_PERP_TOKENS`: List of tokens to focus on when `ACTIVE_FOCUS_MODE` is true
- `ACTIVE_FOCUS_MODE`: When enabled, only monitors the specified focus tokens
- `ALLOW_SYNTHETIC_PAIRS`: Enables creation of synthetic trading pairs
- `CHECK_INTERVAL`: Interval between triangle checks (configured for microsecond-level precision)
- `MAX_WS_CONNECTIONS`: Maximum number of concurrent WebSocket connections

## Performance Optimizations

TriFury is built with performance as a primary goal:

- **Lock-free data structures**: Uses `DashMap` for concurrent access to price data without locking
- **Zero-copy data handling**: Minimizes data copying and allocation during critical paths
- **Efficient memory management**: Rust's ownership model ensures optimal memory usage
- **Microsecond-precision timing**: Operates at microsecond intervals for maximum throughput
- **Parallel processing**: Distributes work across multiple threads using Tokio's async runtime

## License

MIT

## Disclaimer

This software is for educational and research purposes only. Trading cryptocurrencies involves significant risk. Always perform your own due diligence before trading.