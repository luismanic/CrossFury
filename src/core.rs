// core.rs - Updated with profitable opportunities counter

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64,Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

//
// CONSTANTS
//

// Fee constants
pub const PERP_MAKER_FEE: f64 = 0.0001; // 0.01%
pub const PERP_TAKER_FEE: f64 = 0.0006; // 0.06%

// Consolidated and expanded token list (over 100 tokens)
pub const TOP_TOKENS: [&str; 156] = [
    // Original tokens
    "BTC", "ETH", "USDT", "BNB", "XRP", "USDC", "SOL", "ADA", "DOGE", "TRX", "SEI",
    "TON", "DOT", "MATIC", "DAI", "LTC", "BCH", "SHIB", "AVAX", "LINK", "XLM",
    "UNI", "LEO", "ATOM", "OKB", "ETC", "XMR", "FIL", "HBAR", "APT", "ICP", "BONK",
    "NEAR", "VET", "ARB", "QNT", "OP", "CRO", "AAVE", "GRT", "STX", "ALGO", "JTO",
    "EGLD", "FTM", "EOS", "XTZ", "IMX", "FLOW", "THETA", "XEC", "AXS", "SAND",
    "MANA", "RUNE", "NEO", "CFX", "KAVA", "ROSE", "SUI", "ZEC", "ENJ", "BAT", "JASMY",
    "LDO", "SNX", "GALA", "CAKE", "ONE", "COMP", "DASH", "ZIL", "CHZ", "FET", "INJ",
    "DYDX", "1INCH", "PEPE", "GMX", "AR", "FARTCOIN", "FLOKI", "JUP", "ONDO", "C98",
    "ENA", "NEIRO", "BERA", "TIA", "NOT", "ACT", "AI16Z", "AIXBT", "ALCH", "BIGTIME",
    "ARKM", "UXLINK", "TRUMP", "SPX", "ORCA", "HYPE", "ZRO", "LAYER", "TUT", "CHEEMS",
    "SAFE", "PARTI", "WIF", "TAO", "WAL", "API3", "BROCCOLI", "PENDLE", "APE", "OM",
    "MUBARAK", "POPCAT", "JELLY", "DOGS", "ETHFI", "PEOPLE", "KAITO", "KSM", "ENS",
    "FUN", "STMX", "HNT", "LEND", "STPT", "CVC", "MATIC", "AUDIO", "RLC", "AUCTION",
    "MIOTA", "MKR", "KNC", "ZRX", "REN", "BAL", "LRC", "SUSHI", "YFI", "UMA", "PNUT",
    "BAND", "CELR", "OGN", "SC", "OMG", "QTUM", "IOST", "ONT", "WAVES", "CRV", "EIGEN",
];

// Configuration flags
pub const ACTIVE_FOCUS_MODE: bool = false;

// WebSocket URL for Phemex
pub const WEBSOCKET_URL: &str = "wss://ws.phemex.com";

// Connection settings
pub const MAX_WS_CONNECTIONS: usize = 25;
pub const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 50;

// Connection recovery settings
pub const RECONNECT_DELAY_BASE: f64 = 0.5; // Start with shorter delay
pub const MAX_RECONNECT_DELAY: f64 = 5.0; // Cap the delay at 5 seconds
pub const PING_INTERVAL: Duration = Duration::from_millis(500); // .5s ping interval

// Volatility tracking settings
pub const VOLATILITY_THRESHOLD: f64 = 0.05; // 0.05% price change to flag as volatile
pub const VOLATILITY_WINDOW: u64 = 60; // Track price changes over 60 seconds
pub const ACTIVE_TOKEN_TIMEOUT: u64 = 30; // How long a token remains "active" after price change

// File to cache products data
pub const PERP_PRODUCTS_CACHE_FILE: &str = "phemex_perp_products_cache.json";

// Timeouts
pub const INITIAL_TIMEOUT: Duration = Duration::from_secs(10); // Longer timeout for initial product fetch
pub const WEBSOCKET_TIMEOUT: Duration = Duration::from_secs(5); // Shorter timeout for WebSocket messages

// In core.rs, add this constant back:
pub const SYMBOL_TYPE_PERPETUAL: &str = "perpetual";

// Connection stale detection settings
pub const STALE_CONNECTION_TIMEOUT: i64 = 30000; // 30 seconds (increased from 20)
pub const FORCE_RECONNECT_TIMEOUT: i64 = 45000; // 45 seconds of complete silence

// OrderbookUpdate
#[derive(Debug, Clone)]
pub struct OrderbookUpdate {
    pub symbol: String,
    pub best_ask: f64,
    pub best_bid: f64,
    pub timestamp: i64,
    pub scale: i32,
    pub is_synthetic: bool,
    pub leg1: Option<String>,
    pub leg2: Option<String>,
    // Add these new fields:
    pub depth_asks: Option<Vec<(f64, f64)>>, // (price, quantity) pairs
    pub depth_bids: Option<Vec<(f64, f64)>>, // (price, quantity) pairs
}

/// Represents a trading product
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Product {
    pub symbol: String,
    pub base_token: String,
    pub quote_token: String,
    pub status: String,
    pub price_scale: i32,
    pub qty_scale: i32,
    pub symbol_type: String,
}

#[derive(Debug, Clone)]
pub struct PriceData {
    pub best_ask: f64,
    pub best_bid: f64,
    pub timestamp: i64,
    pub scale: i32,
    pub is_synthetic: bool,
    pub leg1: Option<String>,
    pub leg2: Option<String>,
    pub depth_asks: Option<Vec<(f64, f64)>>, // Changed to Option
    pub depth_bids: Option<Vec<(f64, f64)>>, // Changed to Option
}

/// Tracks volatility information for a token
#[derive(Debug, Clone)]
pub struct VolatilityData {
    pub pct_change: f64,
    pub timestamp: i64,
}

/// Application state that will be shared across threads
#[derive(Debug, Clone)]
pub struct AppState {
    pub price_data: Arc<DashMap<String, PriceData>>,
    pub price_scales: Arc<DashMap<String, i32>>,
    pub symbol_to_tokens: Arc<DashMap<String, (String, String)>>, // Symbol -> (base, quote)
    pub is_initializing: Arc<RwLock<bool>>,
    
    // Performance tracking counters
    pub price_updates: Arc<AtomicU64>,             // Count of price updates received
    pub websocket_messages: Arc<AtomicU64>,        // Count of WebSocket messages received
    pub cross_exchange_checks: Arc<AtomicU64>,     // Count of cross-exchange checks performed
    pub profitable_opportunities: Arc<AtomicU64>,  // Count of profitable arbitrage opportunities found
    
    // Global timestamp tracking using atomics
    pub last_check_time: Arc<AtomicU64>,          // Global timestamp of last message
    pub connection_timestamps: Arc<DashMap<String, AtomicU64>>, // Per-connection timestamps
    
    // Connection health flags
    pub connection_health: Arc<DashMap<String, bool>>, // Connection ID -> healthy flag
    
    // Message queue for orderbook updates
    pub orderbook_queue: Option<mpsc::UnboundedSender<OrderbookUpdate>>,
    
    // Reconnection signals for connections
    pub reconnect_signals: Arc<DashMap<String, bool>>, // Connection ID -> should reconnect flag
}

impl AppState {
    pub fn new() -> Self {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;
        
        Self {
            price_data: Arc::new(DashMap::new()),
            price_scales: Arc::new(DashMap::new()),
            symbol_to_tokens: Arc::new(DashMap::new()),
            is_initializing: Arc::new(RwLock::new(true)),
            
            // Initialize performance counters
            price_updates: Arc::new(AtomicU64::new(0)),
            websocket_messages: Arc::new(AtomicU64::new(0)),
            cross_exchange_checks: Arc::new(AtomicU64::new(0)),
            profitable_opportunities: Arc::new(AtomicU64::new(0)),
            
            // Initialize timestamp tracking using atomics
            last_check_time: Arc::new(AtomicU64::new(current_time)),
            connection_timestamps: Arc::new(DashMap::new()),
            
            // Initialize connection health tracking
            connection_health: Arc::new(DashMap::new()),
            
            // Initialize message queue as None (will be set in main)
            orderbook_queue: None,
            
            // Initialize reconnection signals
            reconnect_signals: Arc::new(DashMap::new()),
        }
    }
    
    /// Get the next request ID using static atomic counter
    pub async fn get_next_request_id(&self) -> u64 {
        static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
        REQUEST_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
    }
    
    /// Update the global timestamp with the current time
    #[inline(always)]
    pub fn update_global_timestamp(&self) {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;
        self.last_check_time.store(current_time, Ordering::SeqCst);
    }
    
    /// Update a specific connection's timestamp
    #[inline(always)]
    pub fn update_connection_timestamp(&self, connection_id: &str) {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;
        
        // Update the connection-specific timestamp
        match self.connection_timestamps.entry(connection_id.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                entry.get().store(current_time, Ordering::SeqCst);
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(AtomicU64::new(current_time));
            }
        }
        
        // Also update the global timestamp
        self.last_check_time.store(current_time, Ordering::SeqCst);
        
        // Mark this connection as healthy
        self.connection_health.insert(connection_id.to_string(), true);
    }
    
    /// Get how long a connection has been idle in milliseconds
    #[inline(always)]
    pub fn get_connection_idle_time(&self, connection_id: &str) -> u64 {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;
        
        if let Some(timestamp) = self.connection_timestamps.get(connection_id) {
            let last_time = timestamp.value().load(Ordering::SeqCst);
            if current_time > last_time {
                current_time - last_time
            } else {
                0 // Clock skew protection
            }
        } else {
            STALE_CONNECTION_TIMEOUT as u64 + 1 // No record means it's been idle since the beginning
        }
    }
    
    /// Get how long all connections have been idle
    #[inline(always)]
    pub fn get_global_idle_time(&self) -> u64 {
        let current_time = chrono::Utc::now().timestamp_millis() as u64;
        let last_time = self.last_check_time.load(Ordering::SeqCst);
        
        if current_time > last_time {
            current_time - last_time
        } else {
            0 // Clock skew protection
        }
    }
    
    /// Mark a connection as unhealthy
    #[inline(always)]
    pub fn mark_connection_unhealthy(&self, connection_id: &str) {
        self.connection_health.insert(connection_id.to_string(), false);
    }
    
    /// Check if a connection is healthy
    #[inline(always)]
    pub fn is_connection_healthy(&self, connection_id: &str) -> bool {
        self.connection_health.get(connection_id)
            .map(|entry| *entry.value())
            .unwrap_or(false)
    }
    
    // Signal that a connection should reconnect
    pub fn signal_reconnect(&self, connection_id: &str) {
        self.reconnect_signals.insert(connection_id.to_string(), true);
        self.mark_connection_unhealthy(connection_id);
    }
    
    // Check if a connection should reconnect
    pub fn should_reconnect(&self, connection_id: &str) -> bool {
        self.reconnect_signals.get(connection_id)
            .map(|entry| *entry.value())
            .unwrap_or(false)
    }
    
    // Clear reconnection signal
    pub fn clear_reconnect_signal(&self, connection_id: &str) {
        self.reconnect_signals.insert(connection_id.to_string(), false);
    }
    
    // Method to get price update count and reset counter
    pub fn get_and_reset_price_update_count(&self) -> u64 {
        self.price_updates.swap(0, Ordering::Relaxed)
    }
    
    // Method to get WebSocket message count and reset counter
    pub fn get_and_reset_websocket_message_count(&self) -> u64 {
        self.websocket_messages.swap(0, Ordering::Relaxed)
    }
    
    // Method to get cross-exchange check count and reset counter
    pub fn get_and_reset_cross_exchange_count(&self) -> u64 {
        self.cross_exchange_checks.swap(0, Ordering::Relaxed)
    }
    
    // Method to get and reset profitable opportunities counter
    pub fn get_and_reset_profitable_opportunities(&self) -> u64 {
        self.profitable_opportunities.swap(0, Ordering::Relaxed)
    }
    
    // Increment the price update counter
    pub fn increment_price_updates(&self, count: u64) {
        self.price_updates.fetch_add(count, Ordering::Relaxed);
    }
    
    // Increment the WebSocket messages counter
    pub fn increment_websocket_messages(&self, count: u64) {
        self.websocket_messages.fetch_add(count, Ordering::Relaxed);
    }
    
    // Increment the cross-exchange checks counter
    pub fn increment_cross_exchange_checks(&self, count: u64) {
        self.cross_exchange_checks.fetch_add(count, Ordering::Relaxed);
    }
    
    // Increment the profitable opportunities counter
    pub fn increment_profitable_opportunities(&self, count: u64) {
        self.profitable_opportunities.fetch_add(count, Ordering::Relaxed);
    }
}

/// Represents WebSocket message types we'll be working with
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum WebSocketMessage {
    OrderBookUpdate {
        book: OrderBook,
        symbol: String,
        #[serde(default)]
        depth: Option<i32>,
    },
    PerpOrderBookUpdate {
        orderbook_p: OrderBook,  // Notice: orderbook_p instead of book
        symbol: String,
        depth: i32,
        sequence: i64,
        timestamp: i64,
        #[serde(rename = "type")]
        message_type: String,
    },
    // New variant specifically for Phemex orderbook_p messages with various optional fields
    PhemexOrderbook {
        orderbook_p: OrderBook,
        symbol: String,
        depth: i32,
        #[serde(default)]
        sequence: Option<i64>,  // Made optional
        #[serde(default)]
        timestamp: Option<i64>, // Made optional
        #[serde(rename = "type", default)]
        message_type: Option<String>,
        #[serde(default)]
        dts: Option<i64>,
        #[serde(default)]
        mts: Option<i64>,
    },
    TypedMessage {
        #[serde(rename = "type")]
        message_type: String,
        symbol: String,
        #[serde(default)]
        book: Option<OrderBook>,
        #[serde(default)]
        orderbook_p: Option<OrderBook>,  // Add field for perpetual orderbook
        #[serde(default)]
        depth: Option<OrderBook>,
        #[serde(default)]
        sequence: Option<i64>,
        #[serde(default)]
        timestamp: Option<i64>,
    },
    // Updated to better handle Phemex's response format
    Response {
        id: u64,
        #[serde(default)]
        result: Option<serde_json::Value>,
        #[serde(default)]
        error: Option<serde_json::Value>,
    },
    TradeUpdate {
        trades: Vec<serde_json::Value>,
        symbol: String,
    },
    Unknown(serde_json::Value),
}

/// Order book data structure
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBook {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

/// Error types that can occur in our application
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    #[error("WebSocket error: {0}")]
    WebSocketError(String),
    
    #[error("HTTP request error: {0}")]
    RequestError(#[from] reqwest::Error),
    
    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),
    
    #[error("Missing price data for symbol: {0}")]
    MissingPriceData(String),
    
    #[error("Timeout error")]
    TimeoutError,
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Other error: {0}")]
    Other(String),
}