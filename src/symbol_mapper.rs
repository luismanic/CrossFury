// symbol_mapper.rs - Optimized version for cross-exchange mapping

use std::collections::{HashMap, HashSet};
use crate::{core::*, CrossExchangeArb, ExchangeFees};
use crate::exchange_types::Exchange;

/// A structure to manage symbol mappings across exchanges
pub struct SymbolMapper {
    // Map from canonical symbol (e.g., BTC/USDT) to exchange-specific symbols
    canonical_to_exchange: HashMap<String, HashMap<String, String>>,
    
    // Map from exchange-specific symbol to canonical
    exchange_to_canonical: HashMap<String, String>,
    
    // Pre-computed full symbol mappings to avoid string concatenation in hot paths
    full_symbol_mappings: HashMap<(String, String), String>, // (canonical, exchange) -> full_prefixed_symbol
}

impl SymbolMapper {
    /// Create a new empty symbol mapper
    pub fn new() -> Self {
        Self {
            canonical_to_exchange: HashMap::new(),
            exchange_to_canonical: HashMap::new(),
            full_symbol_mappings: HashMap::new(),
        }
    }
    
    /// Build the symbol mapper from app state price data
    pub fn build_from_price_data(app_state: &AppState) -> Self {
        let mut mapper = Self::new();
        
        // First, gather all symbols by exchange
        let mut exchange_symbols: HashMap<String, Vec<String>> = HashMap::new();
        
        for entry in app_state.price_data.iter() {
            let full_symbol = entry.key().clone();
            let parts: Vec<&str> = full_symbol.split(':').collect();
            
            if parts.len() == 2 {
                let exchange = parts[0].to_string();
                let symbol = parts[1].to_string();
                
                exchange_symbols.entry(exchange).or_insert_with(Vec::new).push(symbol);
            }
        }

        // Extract base and quote currencies from each symbol
        for (exchange, symbols) in &exchange_symbols {
            for symbol in symbols {
                // Try to extract base/quote using different formats
                let (base, quote) = if symbol.contains('_') {
                    // Format: BASE_QUOTE
                    let parts: Vec<&str> = symbol.split('_').collect();
                    if parts.len() == 2 {
                        (parts[0].to_uppercase(), parts[1].to_uppercase())
                    } else {
                        continue; // Invalid format
                    }
                } else if symbol.contains('-') {
                    // Format: BASE-QUOTE
                    let parts: Vec<&str> = symbol.split('-').collect();
                    if parts.len() == 2 {
                        (parts[0].to_uppercase(), parts[1].to_uppercase())
                    } else {
                        continue; // Invalid format
                    }
                } else {
                    // Format: BASEQUOTE - try to split based on common quote currencies
                    let common_quotes = ["USDT", "USD", "BTC", "ETH", "USDC"];
                    let mut base = symbol.clone();
                    let mut quote = String::new();
                    
                    for &q in &common_quotes {
                        if symbol.to_uppercase().ends_with(q) {
                            let split_point = symbol.len() - q.len();
                            base = symbol[..split_point].to_string();
                            quote = symbol[split_point..].to_string();
                            break;
                        }
                    }
                    
                    if quote.is_empty() {
                        continue; // Couldn't split
                    }
                    
                    (base.to_uppercase(), quote.to_uppercase())
                };
                
                // Create canonical symbol format: BASE/QUOTE
                let canonical = format!("{}/{}", base, quote);
                
                // Add mappings
                mapper.add_mapping(&canonical, exchange, symbol);
            }
        }
        
        // Pre-compute full symbol mappings for faster lookups
        for (canonical, exchange_map) in &mapper.canonical_to_exchange {
            for (exchange, symbol) in exchange_map {
                let full_symbol = format!("{}:{}", exchange, symbol);
                mapper.full_symbol_mappings.insert((canonical.clone(), exchange.clone()), full_symbol);
            }
        }
        
        mapper
    }
    
    /// Add a mapping from canonical symbol to exchange-specific symbol
    fn add_mapping(&mut self, canonical: &str, exchange: &str, exchange_symbol: &str) {
        // Store mapping from canonical to exchange
        self.canonical_to_exchange
            .entry(canonical.to_string())
            .or_insert_with(HashMap::new)
            .insert(exchange.to_string(), exchange_symbol.to_string());
        
        // Store reverse mapping
        let prefixed_symbol = format!("{}:{}", exchange, exchange_symbol);
        self.exchange_to_canonical.insert(prefixed_symbol, canonical.to_string());
    }
    
    /// Get all canonical symbols that exist on multiple exchanges
    pub fn get_multi_exchange_symbols(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        
        for (canonical, exchange_map) in &self.canonical_to_exchange {
            if exchange_map.len() >= 2 {
                result.insert(canonical.clone());
            }
        }
        
        result
    }
    
    /// Get the full prefixed exchange symbol for a canonical symbol on a specific exchange
    /// Optimized version using pre-computed mappings for faster lookups
    pub fn get_exchange_symbol(&self, canonical: &str, exchange: &str) -> Option<String> {
        // Use pre-computed mapping for fast lookup
        if let Some(full_symbol) = self.full_symbol_mappings.get(&(canonical.to_string(), exchange.to_string())) {
            return Some(full_symbol.clone());
        }
        
        // Fallback to computing the mapping if not found
        self.canonical_to_exchange
            .get(canonical)
            .and_then(|exchange_map| exchange_map.get(exchange))
            .map(|symbol| format!("{}:{}", exchange, symbol))
    }
    
    /// Convert from exchange symbol to canonical form
    pub fn to_canonical(&self, exchange_symbol: &str) -> Option<String> {
        self.exchange_to_canonical.get(exchange_symbol).cloned()
    }
    
    /// Get exchange symbols for a canonical symbol across all exchanges
    pub fn get_all_exchange_symbols(&self, canonical: &str) -> Vec<String> {
        let mut result = Vec::new();
        
        if let Some(exchange_map) = self.canonical_to_exchange.get(canonical) {
            for (exchange, symbol) in exchange_map {
                result.push(format!("{}:{}", exchange, symbol));
            }
        }
        
        result
    }
}

/// Process cross-exchange arbitrage using the symbol mapper
pub fn process_mapped_cross_exchange_arbitrage(
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Vec<CrossExchangeArb> {
    let mut all_opportunities = Vec::new();
    
    // Build the symbol mapper
    let symbol_mapper = SymbolMapper::build_from_price_data(app_state);
    
    // Get canonical symbols that exist on multiple exchanges
    let multi_exchange_symbols = symbol_mapper.get_multi_exchange_symbols();
    
    // If we have symbols on multiple exchanges, process them
    if !multi_exchange_symbols.is_empty() {
        for canonical in &multi_exchange_symbols {
            // Get exchange symbols for this canonical symbol
            let exchange_symbols = symbol_mapper.get_all_exchange_symbols(canonical);
            
            // Skip if we don't have at least two
            if exchange_symbols.len() < 2 {
                continue;
            }
            
            // Get actual price data for each exchange symbol
            let mut exchange_prices: HashMap<String, (f64, f64)> = HashMap::new(); // Exchange -> (bid, ask)
            
            for exchange_symbol in &exchange_symbols {
                if let Some(price_data) = app_state.price_data.get(exchange_symbol) {
                    let exchange = exchange_symbol.split(':').next().unwrap_or("UNKNOWN");
                    exchange_prices.insert(exchange.to_string(), (price_data.best_bid, price_data.best_ask));
                }
            }
            
            // Process all pairs of exchanges
            for (buy_exchange, (_, buy_ask)) in &exchange_prices {
                for (sell_exchange, (sell_bid, _)) in &exchange_prices {
                    // Skip same exchange
                    if buy_exchange == sell_exchange {
                        continue;
                    }
                    
                    // Skip invalid prices
                    if *buy_ask <= 0.0 || *sell_bid <= 0.0 {
                        continue;
                    }
                    
                    // Calculate profit
                    let gross_profit_pct = (*sell_bid / *buy_ask - 1.0) * 100.0;
                    
                    // Get fees
                    let buy_fee = match buy_exchange.as_str() {
                        "PHEMEX" => 0.0006, // 0.06%
                        "MEXC" => 0.0002,   // 0.02%
                        "XTCOM" => 0.0006,  // 0.06%
                        "HBIT" => 0.0006,  // 0.06%
                        "BATONEX" => 0.0006,  // 0.06%
                        "TAPBIT" => 0.0006,  // 0.06%
                        "COINCATCH" => 0.0006,  // 0.06%
                        _ => 0.001,         // Default 0.1%
                    };
                    
                    let sell_fee = match sell_exchange.as_str() {
                        "PHEMEX" => 0.0006, // 0.06%
                        "MEXC" => 0.0002,   // 0.02%
                        "XTCOM" => 0.0006,  // 0.06%
                        "HBIT" => 0.0006,  // 0.06%
                        "BATONEX" => 0.0006,  // 0.06%
                        "TAPBIT" => 0.0006,  // 0.06%
                        "COINCATCH" => 0.0006,  // 0.06%
                        _ => 0.001,         // Default 0.1%
                    };
                    
                    // Calculate total fees
                    let total_fees_pct = (buy_fee + sell_fee) * 100.0;
                    
                    // Calculate net profit
                    let net_profit_pct = gross_profit_pct - total_fees_pct;
                    
                    // If profitable, add to opportunities
                    if net_profit_pct >= 0.2 { // Min threshold
                        // Parse exchanges to enum
                        let buy_exchange_enum = match buy_exchange.as_str() {
                            "PHEMEX" => Exchange::Phemex,
                            "MEXC" => Exchange::LBank,
                            "XTCOM" => Exchange::XtCom,
                            "HBIT" => Exchange::Hbit,
                            "BATONEX" => Exchange::Batonex,
                            "TAPBIT" => Exchange::TapBit,
                            "COINCATCH" => Exchange::CoinCatch,
                            _ => continue, // Skip unknown exchange
                        };
                        
                        let sell_exchange_enum = match sell_exchange.as_str() {
                            "PHEMEX" => Exchange::Phemex,
                            "MEXC" => Exchange::LBank,
                            "XTCOM" => Exchange::XtCom,
                            "HBIT" => Exchange::Hbit,
                            "BATONEX" => Exchange::Batonex,
                            "TAPBIT" => Exchange::TapBit,
                            "COINCATCH" => Exchange::CoinCatch,
                            _ => continue, // Skip unknown exchange
                        };
                        
                        // Create opportunity
                        let opportunity = CrossExchangeArb {
                            symbol: canonical.clone(),
                            buy_exchange: buy_exchange_enum,
                            sell_exchange: sell_exchange_enum,
                            buy_price: *buy_ask,
                            sell_price: *sell_bid,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            profit_pct: gross_profit_pct,
                            net_profit_pct,
                            total_fees_pct,
                        };
                        
                        all_opportunities.push(opportunity);
                    }
                }
            }
        }
    }
    
    // Sort by profit
    all_opportunities.sort_by(|a, b| b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal));
    
    all_opportunities
}

/// Enum to represent different symbol format styles
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolFormat {
    /// BASE_QUOTE with underscore (e.g., BTC_USDT)
    Underscore,
    /// BASE-QUOTE with hyphen (e.g., BTC-USDT)
    Hyphen,
    /// BASEQUOTE with no separator (e.g., BTCUSDT)
    NoSeparator,
}

/// Get the format of a symbol
pub fn detect_symbol_format(symbol: &str) -> SymbolFormat {
    if symbol.contains('_') {
        SymbolFormat::Underscore
    } else if symbol.contains('-') {
        SymbolFormat::Hyphen
    } else {
        SymbolFormat::NoSeparator
    }
}