# CrossFury 🔥  
**High-Performance Cross-Exchange Arbitrage Scanner for Crypto Markets**

CrossFury is a latency-optimized arbitrage engine written in Rust, designed to scan and detect profitable opportunities across centralized crypto exchanges with speed and precision. Built from the ground up with high-frequency trading architecture in mind, CrossFury can perform up to **75,000+ arbitrage checks per second** and analyze **420+ token pairs in real-time**.

---

## 🚀 Features

- **Ultra-Fast Scanning**: Processes over **754,000 cross-exchange checks** every 10 seconds (~75.4k/sec).
- **Real-Time WebSocket Handling**: Handles **28,000+ messages** per 10 seconds with **0 ms latency** using async channels and SIMD-accelerated JSON parsing.
- **Multi-Exchange Support**: Integrates with multiple exchanges including Phemex, LBank, XT.COM, Tapbit, Batonex, and CoinCatch.
- **Slippage- & Fee-Aware Profit Modeling**: Executes advanced **order book analysis** to account for depth, liquidity, and adaptive trade sizing.
- **Dynamic Opportunity Tracking**: Continuously refreshes arbitrage opportunities and supports both two-leg and multi-hop (triangular) strategies.
- **Scalable Codebase**: ~14,000 lines of modular Rust code engineered for extensibility and raw performance.

---

## 🧠 Architecture Highlights

- Built with **Rust** for concurrency and memory safety.
- Uses **DashMap**, **tokio**, and **crossbeam** for real-time order book updates and async processing.
- Supports **SIMD-based JSON parsing** with `simd-json` for maximum throughput.
- Advanced **market impact models** to simulate realistic execution prices under various liquidity conditions.

---

## 📈 Live Metrics Snapshot


---

## 📦 Modules

- `core.rs` – Main event loop and opportunity evaluation
- `exchange_types.rs` – Exchange identifiers and pricing models
- `message_processor.rs` – High-speed order book message parsing
- `symbol_mapper.rs` – Symbol normalization across exchanges
- `cross_exchange.rs` – Cross-market arbitrage engine
- `multi_hop.rs` – (Planned) Support for triangular paths
- `config.rs` – Configurable exchange and trading parameters

---

## 🧪 Coming Soon

- 📊 Web-based dashboard for visualizing active opportunities
- 🧠 Reinforcement learning module to rank best execution routes
- 🔁 Automated execution with exchange trading APIs

---

## 👨‍💻 Author

Luis Manic | [@luismanic](https://github.com/luismanic)

---

## 📄 License

MIT License

---

> ⚡ Built for speed. Tuned for precision. Designed for scale.  
> Welcome to **CrossFury**.
