// cross_exchange.rs - Optimized cross-exchange arbitrage calculation logic with enhanced scaling detection
// and multi-hop arbitrage support

use crate::core::*;
use crate::exchange_types::{Exchange, ExchangeFees, CrossExchangeArb, StandardOrderBook, MultiHopArbitragePath};
use crate::token_lists::{TARGET_TOKENS, normalize_symbol, extract_exchange};
use crate::config::get_config;
use log::{info, warn, debug, error};
use std::collections::{HashMap, HashSet, VecDeque};
use lazy_static::lazy_static;
use std::sync::{Mutex, RwLock};
use csv::Writer;
use crate::utils::ensure_exchange_prefix;
use crate::error_handling::init_error_tracker;
use chrono::Utc;

// Add missing MIN_PROFIT_THRESHOLD constant
pub const MIN_PROFIT_THRESHOLD: f64 = 0.10; // 0.10% minimum profit threshold

// Global buffer for cross-exchange opportunities
lazy_static! {
    static ref CROSS_EX_BUFFER: Mutex<Vec<CrossExchangeArb>> = Mutex::new(Vec::new());
    
    // Track recently detected opportunities to avoid duplicates
    static ref RECENT_OPPORTUNITIES: Mutex<HashSet<String>> = Mutex::new(HashSet::with_capacity(10000));
    
    // Track suspicious symbols with too many opportunities
    static ref SUSPICIOUS_SYMBOLS: Mutex<HashMap<String, u32>> = Mutex::new(HashMap::new());
    
    static ref EXCHANGE_SCALING_RELATIONSHIPS: RwLock<HashMap<(String, Exchange, Exchange), ScalingRelationship>> = {
        let map = HashMap::with_capacity(1000);
        RwLock::new(map)
    };
        
    // Buffer for multi-hop arbitrage opportunities
    static ref MULTI_HOP_BUFFER: Mutex<Vec<MultiHopArbitragePath>> = Mutex::new(Vec::new());
}

// Add this structure for tracking scaling relationships
#[derive(Debug, Clone)]
struct ScalingRelationship {
    symbol: String,
    exchange1: Exchange,
    exchange2: Exchange,
    observed_ratios: Vec<f64>,
    confirmed_scale_factor: Option<f64>,
    last_update: i64, // timestamp
    sample_count: usize, // how many samples we've collected
}

impl ScalingRelationship {
    fn new(symbol: &str, ex1: Exchange, ex2: Exchange) -> Self {
        Self {
            symbol: symbol.to_string(),
            exchange1: ex1,
            exchange2: ex2,
            observed_ratios: Vec::with_capacity(50), // Store up to 50 observations
            confirmed_scale_factor: None,
            last_update: Utc::now().timestamp_millis(),
            sample_count: 0,
        }
    }
    
    // Add a new price ratio observation
    fn add_observation(&mut self, ratio: f64) {
        // Don't add obviously erroneous values
        if ratio <= 0.0 || !ratio.is_finite() {
            return;
        }
        
        self.sample_count += 1;
        self.last_update = Utc::now().timestamp_millis();
        
        // Only keep the most recent observations (up to capacity)
        if self.observed_ratios.len() >= 50 {
            self.observed_ratios.remove(0); // Remove oldest
        }
        
        self.observed_ratios.push(ratio);
        
        // Try to confirm a scaling factor if we have enough samples
        if self.sample_count >= 30 && self.observed_ratios.len() >= 15 {
            self.try_confirm_scaling_factor();
        }
    }
    
    // Attempt to determine if there's a consistent scaling factor
    fn try_confirm_scaling_factor(&mut self) {
        if self.observed_ratios.len() < 15 {
            return; // Not enough data
        }
        
        // Calculate median to avoid influence of outliers
        let mut sorted = self.observed_ratios.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];
        
        // Check if observations are consistently close to the median
        let consistent_count = sorted.iter()
            .filter(|&&r| (r / median - 1.0).abs() < 0.03) // Within 3% of median
            .count();
            
        // If at least 75% of observations are consistent, confirm the scaling factor
        if consistent_count >= sorted.len() * 3 / 4 {
            // Try to round to a clean power of 10 if possible
            let log10 = median.log10();
            let rounded_log10 = log10.round();
            
            if (log10 - rounded_log10).abs() < 0.05 {
                // It's a power of 10, use the clean rounded value
                self.confirmed_scale_factor = Some(10.0_f64.powf(rounded_log10));
                info!("Learned scaling factor for {}: {} to {} = power of 10: {:.0}", 
                      self.symbol, self.exchange1, self.exchange2, rounded_log10);
            } else {
                // Use the exact median as scaling factor
                self.confirmed_scale_factor = Some(median);
                info!("Learned precise scaling factor for {}: {} to {} = {:.6}", 
                      self.symbol, self.exchange1, self.exchange2, median);
            }
        }
    }
}

// Function to update exchange relationships with new price observations
fn update_exchange_scaling(
    symbol: &str,
    exchange1: Exchange,
    exchange2: Exchange,
    price1: f64,
    price2: f64
) {
    // Skip invalid prices
    if price1 <= 0.0 || price2 <= 0.0 || !price1.is_finite() || !price2.is_finite() {
        return;
    }
    
    // Calculate ratio between prices
    let ratio = price2 / price1;
    
    // Update the relationship in our database
    let mut relationships = EXCHANGE_SCALING_RELATIONSHIPS.write().unwrap();
    
    // Create key for both directions (we'll store the relationship once)
    let key = if exchange1 as usize <= exchange2 as usize {
        (symbol.to_string(), exchange1, exchange2)
    } else {
        (symbol.to_string(), exchange2, exchange1)
    };
    
    // Get or create the relationship
    let relationship = relationships
        .entry(key)
        .or_insert_with(|| ScalingRelationship::new(symbol, exchange1, exchange2));
    
    // Add the observation (adjust ratio direction if needed)
    if exchange1 as usize <= exchange2 as usize {
        relationship.add_observation(ratio);
    } else {
        relationship.add_observation(1.0 / ratio);
    }
}

// Function to get a learned scaling factor if available
fn get_learned_scaling_factor(symbol: &str, from_exchange: Exchange, to_exchange: Exchange) -> Option<f64> {
    let relationships = EXCHANGE_SCALING_RELATIONSHIPS.read().unwrap();
    
    // Try in the original direction
    let key1 = (symbol.to_string(), from_exchange, to_exchange);
    if let Some(relationship) = relationships.get(&key1) {
        if let Some(factor) = relationship.confirmed_scale_factor {
            return Some(factor);
        }
    }
    
    // Try in the reverse direction
    let key2 = (symbol.to_string(), to_exchange, from_exchange);
    if let Some(relationship) = relationships.get(&key2) {
        if let Some(factor) = relationship.confirmed_scale_factor {
            return Some(1.0 / factor); // Invert the factor
        }
    }
    
    None
}

/// Enhanced detection of scaling differences between exchanges
fn detect_scaling_difference(
    symbol: &str,
    buy_exchange: Exchange,
    sell_exchange: Exchange,
    buy_price: f64,
    sell_price: f64
) -> Option<f64> {
    // First check if we have a learned scaling factor
    if let Some(learned_factor) = get_learned_scaling_factor(symbol, buy_exchange, sell_exchange) {
        // Check if the current ratio is close to our learned factor
        let current_ratio = sell_price / buy_price;
        if (current_ratio / learned_factor - 1.0).abs() < 0.05 {
            // The current ratio matches our learned factor
            return Some(learned_factor);
        }
    }
    
    // If we don't have a learned factor or it doesn't match, check common patterns
    let ratio = sell_price / buy_price;
    
    // Check if it's close to a power of 10 (most common case)
    let log10_ratio = ratio.log10().abs();
    let nearest_power = log10_ratio.round();
    
    if (log10_ratio - nearest_power).abs() < 0.05 {
        // It's a power of 10 scaling factor
        return Some(10.0_f64.powf(nearest_power));
    }
    
    // Check if it's close to common scaling factors (e.g., 1000/3, 1000/4, etc.)
    let common_divisors = [2.0, 3.0, 4.0, 5.0, 8.0, 16.0, 32.0, 64.0, 100.0, 1000.0];
    
    for &power in &[1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0] {
        for &divisor in &common_divisors {
            let check_ratio = power / divisor;
            if (ratio / check_ratio - 1.0).abs() < 0.05 {
                return Some(check_ratio);
            }
        }
    }
    
    // Not a recognized scaling pattern
    None
}

/// Get token-specific validation parameters
fn get_token_validation_params(symbol: &str) -> (f64, f64) {
    // Returns (max_reasonable_profit_pct, max_price_variation_pct)
    let config = get_config();
    
    // Try to get token-specific config first
    if let Some(token_config) = config.get_token_config(symbol) {
        return (token_config.max_reasonable_profit_pct, token_config.max_price_variation_pct);
    }
    
    // Extract base token from symbol
    let base_token = extract_base_token(symbol);
    
    match base_token {
        // Major assets - tight spreads, high liquidity
        "BTC" | "ETH" => (1.5, 0.02),  // 1.5% max profit, 2% variation
        
        // Secondary assets - moderate spreads
        "BNB" | "SOL" | "XRP" | "ADA" | "DOT" | "MATIC" | "LINK" | "AVAX" => 
            (2.5, 0.03),  // 2.5% max profit, 3% variation
        
        // Meme tokens - higher volatility
        "DOGE" | "SHIB" | "PEPE" | "FLOKI" | "BONK" | "WIF" => 
            (5.0, 0.08),  // 5% max profit, 8% variation
            
        // Lower cap assets - wider spreads
        "AR" | "FIL" | "ATOM" | "NEAR" | "ZEC" | "EGLD" | "FTM" | "HBAR" | 
        "APT" | "ICP" | "ARB" | "OP" | "CRO" | "AAVE" | "GRT" | "STX" => 
            (3.0, 0.05),  // 3% max profit, 5% variation
            
        // Default for other tokens
        _ => (config.arbitrage.max_reasonable_profit_pct, 0.04),  // Default from config
    }
}

// Helper function to get base token from symbol (e.g., "BTCUSDT" -> "BTC")
fn extract_base_token(symbol: &str) -> &str {
    let quote_currencies = ["USDT", "USD", "BTC", "ETH", "USDC"];
    
    for &quote in &quote_currencies {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            return &symbol[..symbol.len() - quote.len()];
        }
    }
    
    // If we can't extract, return the whole symbol
    symbol
}

// Helper function to normalize price between exchanges
fn normalize_price(exchange: Exchange, symbol: &str, price: f64) -> f64 {
    let base_token = extract_base_token(symbol);
    
    // Get scaling factor from learned relationships
    if let Some(scaling_factor) = get_learned_scaling_factor(symbol, exchange, Exchange::Phemex) {
        return price * scaling_factor;
    }
    
    // Use configured exchange-specific scaling if available
    let config = get_config();
    if let Some(exchange_config) = config.get_exchange_config(&exchange) {
        if let Some(token_config) = config.get_token_config(base_token) {
            return price * token_config.slippage_factor;
        }
    }
    
    // Default to 1.0 (no scaling)
    price
}

/// Buffer a profitable cross-exchange opportunity.
pub fn buffer_cross_exchange_opportunity(arb: CrossExchangeArb) {
    let mut buf = CROSS_EX_BUFFER.lock().unwrap();
    buf.push(arb);
}

/// Buffer a profitable multi-hop arbitrage opportunity
pub fn buffer_multi_hop_opportunity(arb: MultiHopArbitragePath) {
    let mut buf = MULTI_HOP_BUFFER.lock().unwrap();
    buf.push(arb);
}

/// Flush the cross-exchange opportunity buffer to a CSV file.
pub async fn flush_cross_ex_buffer(filename: &str) -> Result<(), AppError> {
    let mut buf = CROSS_EX_BUFFER.lock().unwrap();
    if buf.is_empty() {
        return Ok(());
    }
    
    // Sort the opportunities by net profit percentage (descending)
    buf.sort_by(|a, b| b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal));
    
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;
    
    let mut writer = Writer::from_writer(file);
    
    // If the file is empty, write a header.
    let metadata = std::fs::metadata(filename)?;
    if metadata.len() == 0 {
        writer.write_record(&[
            "timestamp", "symbol", "buy_exchange", "sell_exchange",
            "buy_price", "sell_price", "profit_pct", "net_profit_pct"
        ])?;
    }
    
    for record in buf.iter() {
        writer.write_record(&[
            &format!("{}", record.timestamp),
            &record.symbol,
            &format!("{:?}", record.buy_exchange),
            &format!("{:?}", record.sell_exchange),
            &format!("{:.6}", record.buy_price),
            &format!("{:.6}", record.sell_price),
            &format!("{:.4}", record.profit_pct),
            &format!("{:.4}", record.net_profit_pct),
        ])?;
    }
    
    writer.flush()?;
    info!("Flushed {} cross-exchange opportunities to {}", buf.len(), filename);
    buf.clear();
    
    // Also clean out stale tracked opportunities
    let mut recent_opps = RECENT_OPPORTUNITIES.lock().unwrap();
    if recent_opps.len() > 2000 {
        info!("Clearing opportunity tracking cache of {} items", recent_opps.len());
        recent_opps.clear();
    }
    
    Ok(())
}

/// Flush multi-hop arbitrage opportunities to CSV file
pub async fn flush_multi_hop_buffer(filename: &str) -> Result<(), AppError> {
    let mut buf = MULTI_HOP_BUFFER.lock().unwrap();
    if buf.is_empty() {
        return Ok(());
    }
    
    // Sort by net profit percentage
    buf.sort_by(|a, b| b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal));
    
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;
    
    let mut writer = Writer::from_writer(file);
    
    // If the file is empty, write a header
    let metadata = std::fs::metadata(filename)?;
    if metadata.len() == 0 {
        writer.write_record(&[
            "timestamp", "path_id", "hop_count", "symbols", "exchanges", 
            "profit_pct", "net_profit_pct", "fees_pct", "slippage_pct"
        ])?;
    }
    
    for record in buf.iter() {
        writer.write_record(&[
            &format!("{}", record.timestamp),
            &record.path_id,
            &format!("{}", record.symbol_path.len()),
            &record.symbol_path.join(":"),
            &record.exchange_path.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>().join(":"),
            &format!("{:.4}", record.total_profit_pct),
            &format!("{:.4}", record.net_profit_pct),
            &format!("{:.4}", record.total_fees_pct),
            &format!("{:.4}", record.total_slippage_pct),
        ])?;
    }
    
    writer.flush()?;
    info!("Flushed {} multi-hop arbitrage opportunities to {}", buf.len(), filename);
    buf.clear();
    
    Ok(())
}

/// Create a unique key for an arbitrage opportunity
#[inline(always)]
fn get_opportunity_key(symbol: &str, buy_exchange: &Exchange, sell_exchange: &Exchange) -> String {
    format!("{}:{:?}:{:?}", symbol, buy_exchange, sell_exchange)
}

/// Check if this opportunity was recently detected (to avoid duplicates)
#[inline(always)]
fn is_new_opportunity(symbol: &str, buy_exchange: Exchange, sell_exchange: Exchange) -> bool {
    let key = get_opportunity_key(symbol, &buy_exchange, &sell_exchange);
    
    let mut recent_opps = RECENT_OPPORTUNITIES.lock().unwrap();
    if recent_opps.contains(&key) {
        // We've already seen this opportunity recently
        return false;
    }
    
    // Add to recent opportunities
    recent_opps.insert(key);    
    true
}

/// Check if prices are reasonable and not likely to be data errors
#[inline(always)]
fn is_price_reasonable(price: f64) -> bool {
    // Prices should be positive and within reasonable ranges
    price > 0.00001 && price < 1_000_000.0
}

/// Check if a symbol is producing suspiciously many opportunities 
#[inline(always)]
fn is_symbol_suspicious(symbol: &str) -> bool {
    let mut suspicious = SUSPICIOUS_SYMBOLS.lock().unwrap();
    
    // Increment count for this symbol
    let count = suspicious.entry(symbol.to_string()).or_insert(0);
    *count += 1;
    
    // If more than 20 opportunities from same symbol, mark as suspicious
    if *count > 20 {
        info!("Symbol {} marked as suspicious with {} opportunities", symbol, count);
        return true;
    }
    
    // Clear tracking if too large
    if suspicious.len() > 200 {
        *suspicious = HashMap::new();
    }
    
    false
}

/// Check if an arbitrage opportunity passes sanity checks
#[inline(always)]
fn passes_sanity_checks(
    symbol: &str,
    buy_exchange: Exchange,
    sell_exchange: Exchange,
    buy_price: f64,
    sell_price: f64,
    net_profit_pct: f64
) -> bool {
    let config = get_config();
    
    // Skip complex checks for obvious non-profitable cases
    if net_profit_pct < config.arbitrage.min_profit_threshold_pct || buy_price <= 0.0 || sell_price <= 0.0 {
        return false;
    }
    
    // Update exchange scaling knowledge (for future use)
    update_exchange_scaling(symbol, buy_exchange, sell_exchange, buy_price, sell_price);
    
    // Check for scaling differences between exchanges
    if let Some(scale_factor) = detect_scaling_difference(symbol, buy_exchange, sell_exchange, buy_price, sell_price) {
        // This is likely just a scaling difference, not a real opportunity
        info!("Ignoring exchange scaling difference for {}: ratio {:.4} (detected factor {:.4})", 
              symbol, sell_price / buy_price, scale_factor);
        return false;
    }
    
    // Get token-specific validation parameters
    let (max_reasonable_profit, max_price_variation) = get_token_validation_params(symbol);
    
    // Profit should be reasonable (not absurdly high)
    if net_profit_pct > max_reasonable_profit {
        // If we're still seeing suspiciously high profits after normalization,
        // it might be a data error
        info!("Rejecting opportunity with abnormally high profit: {} with profit {:.2}%", 
              symbol, net_profit_pct);
        return false;
    }

    // Check if this symbol is producing too many opportunities
    if is_symbol_suspicious(symbol) {
        return false;
    }
    
    true
}

/// Compute cross-exchange profit with slippage
#[inline(always)]
pub fn compute_cross_exchange_profit_with_slippage(
    symbol: &str,
    buy_exchange: Exchange,
    sell_exchange: Exchange,
    buy_price: f64,
    sell_price: f64,
    buy_orderbook: Option<&StandardOrderBook>,
    sell_orderbook: Option<&StandardOrderBook>,
    _exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Option<CrossExchangeArb> {
    let config = get_config();
    
    // Skip invalid prices
    if buy_price <= 0.0 || sell_price <= 0.0 {
        return None;
    }
    
    // First check if this is just a scaling difference
    if let Some(_) = detect_scaling_difference(symbol, buy_exchange, sell_exchange, buy_price, sell_price) {
        // It's a scaling difference, not a real opportunity
        return None;
    }
    
    // Normalize prices to account for exchange-specific scaling
    let normalized_buy_price = normalize_price(buy_exchange, symbol, buy_price);
    let normalized_sell_price = normalize_price(sell_exchange, symbol, sell_price);
    
    // Skip if no profit after normalization
    if normalized_sell_price <= normalized_buy_price {
        return None;
    }
    
    // Get fees from the configuration first
    let buy_fee = if let Some(exchange_config) = config.get_exchange_config(&buy_exchange) {
        exchange_config.taker_fee_pct / 100.0
    } else {
        // Fallback to hardcoded values
        match buy_exchange {
            Exchange::Phemex => 0.0006, // 0.06%
            Exchange::LBank => 0.0006,  // 0.06%
            Exchange::XtCom => 0.0006,  // 0.06%
            Exchange::TapBit => 0.0006, // 0.06%
            Exchange::Hbit => 0.0005,   // 0.05%
            Exchange::Batonex => 0.0007, // 0.07%
            Exchange::CoinCatch => 0.0006, // 0.06%
            _ => 0.001, // Default 0.1%
        }
    };
    
    let sell_fee = if let Some(exchange_config) = config.get_exchange_config(&sell_exchange) {
        exchange_config.taker_fee_pct / 100.0
    } else {
        // Fallback to hardcoded values
        match sell_exchange {
            Exchange::Phemex => 0.0006, // 0.06%
            Exchange::LBank => 0.0006,  // 0.06%
            Exchange::XtCom => 0.0006,  // 0.06%
            Exchange::TapBit => 0.0006, // 0.06%
            Exchange::Hbit => 0.0005,   // 0.05%
            Exchange::Batonex => 0.0007, // 0.07%
            Exchange::CoinCatch => 0.0006, // 0.06%
            _ => 0.001, // Default 0.1%
        }
    };
    
    // Apply fees on both opening and closing positions (2x per exchange)
    let total_fees_pct = (buy_fee * 2.0 + sell_fee * 2.0) * 100.0;
    
    // Define trade size - use config value
    let base_trade_size = config.arbitrage.default_trade_size_usd;
    
    // Calculate effective prices and slippage with our enhanced methods
    let (effective_buy_price, buy_slippage_pct, has_buy_liquidity) = match buy_orderbook {
        Some(orderbook) => {
            let (slippage, has_liquidity) = orderbook.calculate_enhanced_slippage(
                base_trade_size,
                true, // is_buy
                symbol
            );
            let effective_price = normalized_buy_price * (1.0 + slippage);
            (effective_price, slippage, has_liquidity)
        },
        None => {
            // Apply default slippage estimate from config
            let slippage_pct = config.arbitrage.default_slippage_pct;
            (normalized_buy_price * (1.0 + slippage_pct), slippage_pct, false)
        }
    };
    
    let (effective_sell_price, sell_slippage_pct, has_sell_liquidity) = match sell_orderbook {
        Some(orderbook) => {
            let (slippage, has_liquidity) = orderbook.calculate_enhanced_slippage(
                base_trade_size,
                false, // is_sell
                symbol
            );
            let effective_price = normalized_sell_price * (1.0 - slippage);
            (effective_price, slippage, has_liquidity)
        },
        None => {
            // Apply default slippage estimate from config
            let slippage_pct = config.arbitrage.default_slippage_pct;
            (normalized_sell_price * (1.0 - slippage_pct), slippage_pct, false)
        }
    };
    
    // Skip if no profit after normalization, slippage, and fees
    if effective_sell_price <= effective_buy_price {
        return None;
    }
    
    // Apply additional liquidity risk factor if books are thin
    let liquidity_risk_factor = if has_buy_liquidity && has_sell_liquidity {
        0.0 // No additional factor if books are healthy
    } else if !has_buy_liquidity && !has_sell_liquidity {
        0.3 // 0.3% additional risk if both books are thin
    } else {
        0.15 // 0.15% if only one book is thin
    };
    
    // Calculate total slippage percentage
    let total_slippage_pct = (buy_slippage_pct * 2.0 + sell_slippage_pct * 2.0) * 100.0 + liquidity_risk_factor;
    
    // Calculate gross profit percentage (before fees & slippage)
    let gross_pct = (effective_sell_price / effective_buy_price - 1.0) * 100.0;
    
    // Calculate net profit after fees and slippage
    let net_pct = gross_pct - total_fees_pct - total_slippage_pct;
    
    // Get token-specific validation parameters
    let (max_reasonable_profit, _) = get_token_validation_params(symbol);
    
    // Only return if profitable at least MIN_PROFIT_THRESHOLD AND not suspiciously high
    if net_pct >= config.arbitrage.min_profit_threshold_pct && net_pct <= max_reasonable_profit {
        let timestamp = chrono::Utc::now().timestamp_millis();
        
        return Some(CrossExchangeArb {
            symbol: symbol.to_string(),
            buy_exchange,
            sell_exchange,
            buy_price,  // Store original price for reference
            sell_price, // Store original price for reference
            timestamp,
            profit_pct: gross_pct,
            net_profit_pct: net_pct,
            total_fees_pct: total_fees_pct + total_slippage_pct, // Include slippage in fees
        });
    }
    
    None
}

/// Compute potential profit for a cross-exchange arbitrage opportunity (Simple version)
#[inline(always)]
pub fn compute_cross_exchange_profit(
    symbol: &str,
    buy_exchange: Exchange,
    sell_exchange: Exchange,
    buy_price: f64,
    sell_price: f64,
) -> Option<CrossExchangeArb> {
    let config = get_config();
    
    // Skip invalid prices
    if buy_price <= 0.0 || sell_price <= 0.0 {
        return None;
    }
    
    // NEW: Normalize prices to account for exchange-specific scaling
    let normalized_buy_price = normalize_price(buy_exchange, symbol, buy_price);
    let normalized_sell_price = normalize_price(sell_exchange, symbol, sell_price);
    
    // Skip if no profit after normalization
    if normalized_sell_price <= normalized_buy_price {
        return None;
    }
    
    // Get fees from configuration first
    let buy_fee = if let Some(exchange_config) = config.get_exchange_config(&buy_exchange) {
        exchange_config.taker_fee_pct / 100.0
    } else {
        // Fallback to hardcoded lookup
        match buy_exchange {
            Exchange::Phemex => 0.0006, // 0.06%
            Exchange::LBank => 0.0006,  // 0.06%
            Exchange::XtCom => 0.0006,  // 0.06%
            Exchange::TapBit => 0.0006, // 0.06%
            Exchange::Hbit => 0.0005,   // 0.05%
            Exchange::Batonex => 0.0007, // 0.07%
            Exchange::CoinCatch => 0.0006, // 0.06%
            _ => 0.001, // Default 0.1%
        }
    };
    
    let sell_fee = if let Some(exchange_config) = config.get_exchange_config(&sell_exchange) {
        exchange_config.taker_fee_pct / 100.0
    } else {
        // Fallback to hardcoded lookup
        match sell_exchange {
            Exchange::Phemex => 0.0006, // 0.06%
            Exchange::LBank => 0.0006,  // 0.06%
            Exchange::XtCom => 0.0006,  // 0.06%
            Exchange::TapBit => 0.0006, // 0.06%
            Exchange::Hbit => 0.0005,   // 0.05%
            Exchange::Batonex => 0.0007, // 0.07%
            Exchange::CoinCatch => 0.0006, // 0.06%
            _ => 0.001, // Default 0.1%
        }
    }; 
    
    // Calculate gross profit percentage (before fees)
    let gross_pct = (normalized_sell_price / normalized_buy_price - 1.0) * 100.0;
    
    // Calculate fees as percentage, accounting for both opening and closing positions
    let total_fees_pct = (buy_fee * 2.0 + sell_fee * 2.0) * 100.0;
    
    // Calculate net profit after fees
    let net_pct = gross_pct - total_fees_pct;
    
    // Only return if profitable at least minimum threshold
    if net_pct >= config.arbitrage.min_profit_threshold_pct {
        let timestamp = chrono::Utc::now().timestamp_millis();
        
        return Some(CrossExchangeArb {
            symbol: symbol.to_string(),
            buy_exchange,
            sell_exchange,
            buy_price,  // Store original price for reference
            sell_price, // Store original price for reference
            timestamp,
            profit_pct: gross_pct,
            net_profit_pct: net_pct,
            total_fees_pct,
        });
    }
    
    None
}

/// Find multi-hop arbitrage opportunities using graph-based algorithm
pub fn find_multi_hop_arbitrage_opportunities(
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Vec<MultiHopArbitragePath> {
    let config = get_config();
    let mut opportunities = Vec::new();
    
    // Skip if multi-hop arbitrage is disabled
    if !config.features.enable_multi_hop_arbitrage {
        return opportunities;
    }
    
    // Get the maximum path length from config
    let max_path_length = config.arbitrage.max_path_length;
    
    // Get default trade size
    let default_trade_size = config.arbitrage.default_trade_size_usd;
    
    // Get the minimum profit threshold
    let min_profit_threshold = config.arbitrage.min_profit_threshold_pct;
    
    // Build a graph representation for path finding
    let (graph, symbol_indices, exchange_indices) = build_arbitrage_graph(app_state);
    
    // Reverse mappings
    let reverse_symbol_indices: HashMap<usize, String> = symbol_indices.iter()
        .map(|(k, v)| (*v, k.clone()))
        .collect();
        
    let reverse_exchange_indices: HashMap<usize, Exchange> = exchange_indices.iter()
        .map(|(k, v)| (*v, *k))
        .collect();
    
    // For each starting currency, find profitable paths
    for (start_symbol, start_idx) in &symbol_indices {
        // Skip synthetic currencies or ones without price data
        if start_symbol.contains(':') || start_symbol == "USDT" {
            continue; // Skip prefixed symbols and USDT (base currency)
        }
        
        // Use modified Bellman-Ford or BFS for each source
        let profitable_paths = find_profitable_paths(
            &graph,
            *start_idx,
            max_path_length,
            &reverse_symbol_indices,
            &reverse_exchange_indices,
            app_state,
            exchange_fees,
            default_trade_size,
            min_profit_threshold,
        );
        
        // Add to opportunities
        opportunities.extend(profitable_paths);
    }
    
    // Sort by net profit (descending)
    opportunities.sort_by(|a, b| {
        b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    // Only keep top 100 opportunities to avoid memory issues
    if opportunities.len() > 100 {
        opportunities.truncate(100);
    }
    
    opportunities
}

/// Build a weighted directed graph for arbitrage path finding
fn build_arbitrage_graph(
    app_state: &AppState,
) -> (
    Vec<Vec<(usize, usize, f64)>>, // graph[from_node] = [(to_node, exchange_idx, rate)]
    HashMap<String, usize>,        // symbol_indices
    HashMap<Exchange, usize>,      // exchange_indices
) {
    let mut symbol_indices = HashMap::new();
    let mut exchange_indices = HashMap::new();
    
    // First pass: collect all unique symbols and exchanges
    for entry in app_state.price_data.iter() {
        let full_symbol = entry.key();
        
        // Extract exchange and symbol
        if let Some((exchange_str, symbol)) = full_symbol.split_once(':') {
            // Parse exchange
            if let Ok(exchange) = exchange_str.parse::<Exchange>() {
                let next_idx = exchange_indices.len();
                let exchange_idx = *exchange_indices
                    .entry(exchange)
                    .or_insert(next_idx);
                
                // Extract base and quote tokens
                if let Some((base, quote)) = extract_trading_pair(symbol) {
                    // Add tokens to symbol indices
                    let next_idx = symbol_indices.len();
                    let base_idx = *symbol_indices
                        .entry(base.to_string())
                        .or_insert(next_idx);
                        
                    let next_idx = symbol_indices.len();
                    let quote_idx = *symbol_indices
                        .entry(quote.to_string())
                        .or_insert(next_idx);
                }
            }
        }
    }
    
    // Initialize graph with empty adjacency lists
    let mut graph = vec![Vec::new(); symbol_indices.len()];
    
    // Second pass: build graph edges
    for entry in app_state.price_data.iter() {
        let full_symbol = entry.key();
        
        // Extract exchange and symbol
        if let Some((exchange_str, symbol)) = full_symbol.split_once(':') {
            if let Ok(exchange) = exchange_str.parse::<Exchange>() {
                if let Some(&exchange_idx) = exchange_indices.get(&exchange) {
                    let price_data = entry.value();
                    
                    // Extract base and quote tokens
                    if let Some((base, quote)) = extract_trading_pair(symbol) {
                        if let (Some(&base_idx), Some(&quote_idx)) = (
                            symbol_indices.get(&base),
                            symbol_indices.get(&quote)
                        ) {
                            // Add edges for the trading pair
                            // Buy: QUOTE -> BASE at ASK price
                            if price_data.best_ask > 0.0 {
                                let buy_rate = 1.0 / price_data.best_ask; // How much base you get per quote
                                graph[quote_idx].push((base_idx, exchange_idx, buy_rate));
                            }
                            
                            // Sell: BASE -> QUOTE at BID price
                            if price_data.best_bid > 0.0 {
                                let sell_rate = price_data.best_bid; // How much quote you get per base
                                graph[base_idx].push((quote_idx, exchange_idx, sell_rate));
                            }
                        }
                    }
                }
            }
        }
    }
    
    (graph, symbol_indices, exchange_indices)
}

/// Extract base and quote tokens from a trading pair symbol
fn extract_trading_pair(symbol: &str) -> Option<(String, String)> {
    // Common quote currencies
    let quote_currencies = ["USDT", "USD", "BTC", "ETH", "USDC", "BNB"];
    
    for &quote in &quote_currencies {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            let base = &symbol[..symbol.len() - quote.len()];
            return Some((base.to_string(), quote.to_string()));
        }
    }
    
    None
}

/// Find profitable paths from a starting node using BFS with cycle detection
fn find_profitable_paths(
    graph: &[Vec<(usize, usize, f64)>],
    start_idx: usize,
    max_path_length: usize,
    symbol_map: &HashMap<usize, String>,
    exchange_map: &HashMap<usize, Exchange>,
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
    trade_size: f64,
    min_profit_threshold: f64,
) -> Vec<MultiHopArbitragePath> {
    let mut profitable_paths = Vec::new();
    
    // Use BFS to find potential arbitrage cycles
    struct PathState {
        node: usize,
        path: Vec<usize>,
        exchanges: Vec<usize>,
        rates: Vec<f64>,
        cumulative_rate: f64,
    }
    
    let mut queue = VecDeque::new();
    
    // Add starting node
    queue.push_back(PathState {
        node: start_idx,
        path: vec![start_idx],
        exchanges: Vec::new(),
        rates: Vec::new(),
        cumulative_rate: 1.0,
    });
    
    // Track visited nodes to avoid simple loops
    let mut visited = HashSet::new();
    visited.insert(start_idx);
    
    while let Some(state) = queue.pop_front() {
        let current_node = state.node;
        
        // For paths longer than 2, check if we've found a cycle back to start
        if state.path.len() > 2 && current_node == start_idx {
            // We've found a potential arbitrage cycle
            let profit_pct = (state.cumulative_rate - 1.0) * 100.0;
            
            // Only consider profitable paths above threshold
            if profit_pct >= min_profit_threshold {
                // Create a multi-hop path
                let mut path = MultiHopArbitragePath::new(state.path.len());
                
                // Populate path data
                for i in 0..state.path.len() {
                    let symbol = symbol_map.get(&state.path[i])
                        .cloned()
                        .unwrap_or_default();
                        
                    let exchange = if i > 0 {
                        *exchange_map.get(&state.exchanges[i-1]).unwrap_or(&Exchange::Phemex)
                    } else {
                        // For the first hop, use the last exchange (completing the cycle)
                        if !state.exchanges.is_empty() {
                            *exchange_map.get(&state.exchanges.last().unwrap()).unwrap_or(&Exchange::Phemex)
                        } else {
                            Exchange::Phemex // Default
                        }
                    };
                    
                    let price = if i > 0 { state.rates[i-1] } else { 1.0 };
                    
                    path.add_hop(symbol, exchange, price);
                }
                
                // Generate unique path ID
                path.generate_path_id();
                
                // Get orderbooks for slippage calculation
                let orderbooks: Vec<Option<&StandardOrderBook>> = (0..path.exchange_path.len())
                    .map(|i| {
                        if i >= path.symbol_path.len() || i >= path.exchange_path.len() {
                            return None;
                        }
                        
                        // Form trading pair from tokens
                        let symbol_str = &path.symbol_path[i];
                        let exchange = path.exchange_path[i];
                        
                        let next_symbol = if i + 1 < path.symbol_path.len() {
                            &path.symbol_path[i+1]
                        } else {
                            &path.symbol_path[0] // Wrap around for last hop
                        };
                        
                        // Create trading pair symbol
                        let trading_pair = format!("{}{}", symbol_str, next_symbol);
                        
                        // Get orderbook
                        let full_symbol = format!("{}:{}", exchange, trading_pair);
                        get_orderbook_for_exchange(app_state, &full_symbol)
                    })
                    .collect();
                
                // Calculate fees and slippage
                path.calculate_fees_and_slippage(exchange_fees, &orderbooks, trade_size);
                
                // Only add if still profitable after fees and slippage
                if path.net_profit_pct >= min_profit_threshold {
                    profitable_paths.push(path);
                }
            }
            
            // Don't explore further from this cycle
            continue;
        }
        
        // Don't exceed max path length
        if state.path.len() >= max_path_length {
            continue;
        }
        
        // Explore neighbors
        for &(next_node, exchange_idx, rate) in &graph[current_node] {
            // Skip if this would create a simple loop (not back to start)
            if next_node != start_idx && state.path.contains(&next_node) {
                continue;
            }
            
            // Create new path state
            let mut new_path = state.path.clone();
            new_path.push(next_node);
            
            let mut new_exchanges = state.exchanges.clone();
            new_exchanges.push(exchange_idx);
            
            let mut new_rates = state.rates.clone();
            new_rates.push(rate);
            
            let new_cumulative_rate = state.cumulative_rate * rate;
            
            // Only continue if path has potential to be profitable
            // (early pruning based on cumulative rate)
            if state.path.len() >= 2 && new_cumulative_rate < 1.0 {
                continue;
            }
            
            // Add to queue
            queue.push_back(PathState {
                node: next_node,
                path: new_path,
                exchanges: new_exchanges,
                rates: new_rates,
                cumulative_rate: new_cumulative_rate,
            });
        }
    }
    
    profitable_paths
}

/// Helper function to get orderbook for a symbol on a specific exchange
#[inline]
fn get_orderbook_for_exchange<'a>(app_state: &'a AppState, symbol: &str) -> Option<&'a StandardOrderBook> {
    if let Some(price_data) = app_state.price_data.get(symbol) {
        let data = price_data.value();
        
        // Create a minimal orderbook with just best bid and ask
        let exchange_str = if symbol.contains(':') {
            symbol.split(':').next().unwrap_or("UNKNOWN")
        } else {
            "UNKNOWN"
        };
        
        let exchange = match exchange_str {
            "PHEMEX" => Exchange::Phemex,
            "LBANK" => Exchange::LBank, 
            "XTCOM" => Exchange::XtCom,
            "TAPBIT" => Exchange::TapBit,
            "HBIT" => Exchange::Hbit,
            "BATONEX" => Exchange::Batonex,
            "COINCATCH" => Exchange::CoinCatch,
            _ => return None
        };
        
        let timestamp = data.timestamp;
        
        // Create orderbook with depth if available
        // Return None since we can't create a reference to a local orderbook
        // This function needs to be redesigned to not return references
        return None;
    }
    
    None
}

/// Get all valid symbols that exist on multiple exchanges
pub fn get_cross_exchange_symbols(app_state: &AppState) -> HashSet<String> {
    let mut symbols = HashSet::new();
    let mut exchange_symbols: HashMap<Exchange, HashSet<String>> = HashMap::new();
    
    // Collect symbols by exchange
    for price_entry in app_state.price_data.iter() {
        let full_symbol = price_entry.key();

        // Make sure we have a properly prefixed symbol
        let full_symbol = if !full_symbol.contains(':') {
            warn!("Found unprefixed symbol in price_data: {}", full_symbol);
            ensure_exchange_prefix(full_symbol, "UNKNOWN")
        } else {
            full_symbol.clone()
        };
        
        // Extract exchange and symbol
        if full_symbol.contains(':') {
            let parts: Vec<&str> = full_symbol.split(':').collect();
            if parts.len() == 2 {
                let exchange_str = parts[0];
                let symbol = parts[1].to_string();
                
                // Parse exchange
                let exchange = match exchange_str {
                    "PHEMEX" => Some(Exchange::Phemex),
                    "LBANK" => Some(Exchange::LBank),
                    "XTCOM" => Some(Exchange::XtCom),
                    "TAPBIT" => Some(Exchange::TapBit),
                    "HBIT" => Some(Exchange::Hbit),
                    "BATONEX" => Some(Exchange::Batonex),
                    "COINCATCH" => Some(Exchange::CoinCatch),
                    _ => None
                };
                
                // Add to exchange-specific set
                if let Some(exch) = exchange {
                    exchange_symbols.entry(exch)
                        .or_insert_with(HashSet::new)
                        .insert(symbol);
                }
            }
        }
    }
    
    // Find symbols that exist on at least two exchanges
    for (exch1, symbols1) in &exchange_symbols {
        for (exch2, symbols2) in &exchange_symbols {
            if exch1 != exch2 {
                // Add intersection of symbols
                for symbol in symbols1.intersection(symbols2) {
                    symbols.insert(symbol.clone());
                }
            }
        }
    }
    
    symbols
}

/// Get symbols that exist on multiple exchanges with more aggressive normalization
pub fn get_normalized_cross_exchange_symbols(app_state: &AppState) -> HashSet<String> {
    let mut symbols = HashSet::with_capacity(1000);
    let mut exchange_symbols: HashMap<Exchange, HashSet<String>> = HashMap::new();
    
    // Map for normalized symbol tracking
    let mut normalized_map: HashMap<String, HashSet<(Exchange, String)>> = HashMap::with_capacity(1000);
    
    // First pass: collect all symbols by exchange and create normalized mapping
    for price_entry in app_state.price_data.iter() {
        let full_symbol = price_entry.key();
        
        // Skip entries without proper exchange prefix
        if !full_symbol.contains(':') {
            continue;
        }
        
        let parts: Vec<&str> = full_symbol.split(':').collect();
        if parts.len() != 2 {
            continue;
        }
        
        let exchange_str = parts[0];
        let original_symbol = parts[1].to_string();
        
        // Aggressively normalize by:
        // 1. Converting to uppercase
        // 2. Removing all separators (_, -, etc.)
        // 3. Standardizing common quote currencies
        let normalized = original_symbol
            .to_uppercase()
            .replace('_', "")
            .replace('-', "");
            
        // Parse exchange
        let exchange = match exchange_str {
            "PHEMEX" => Some(Exchange::Phemex),
            "LBANK" => Some(Exchange::LBank),
            "XTCOM" => Some(Exchange::XtCom),
            "TAPBIT" => Some(Exchange::TapBit),
            "HBIT" => Some(Exchange::Hbit),
            "BATONEX" => Some(Exchange::Batonex),
            "COINCATCH" => Some(Exchange::CoinCatch),
            _ => None
        };
        
        if let Some(exch) = exchange {
            // Add to exchange-specific set (original format)
            exchange_symbols
                .entry(exch)
                .or_insert_with(HashSet::new)
                .insert(original_symbol.clone());
                
            // Add to normalized mapping
            normalized_map
                .entry(normalized)
                .or_insert_with(HashSet::new)
                .insert((exch, original_symbol));
        }
    }
    
    // Second pass: find symbols that exist on multiple exchanges after normalization
    for (normalized, exchanges_and_symbols) in &normalized_map {
        // Count unique exchanges for this normalized symbol
        let unique_exchanges: HashSet<Exchange> = exchanges_and_symbols
            .iter()
            .map(|(exch, _)| *exch)
            .collect();
            
        if unique_exchanges.len() >= 2 {
            // Add each original symbol to our result
            for (_, original) in exchanges_and_symbols {
                symbols.insert(original.clone());
            }
        }
    }
    
    symbols
}

/// Get target cross-exchange symbols for focused scanning
pub fn get_target_cross_exchange_symbols(app_state: &AppState) -> HashSet<String> {
    // Implementation similar to get_normalized_cross_exchange_symbols 
    // but filtered to target tokens
    let mut symbols = HashSet::with_capacity(100);
    
    // Get all symbols that exist on multiple exchanges
    let all_symbols = get_normalized_cross_exchange_symbols(app_state);
    
    // Filter to only include target tokens
    for symbol in all_symbols {
        let upper_symbol = symbol.to_uppercase();
        if TARGET_TOKENS.iter().any(|&token| token == upper_symbol) {
            symbols.insert(symbol);
        }
    }
    
    symbols
}

/// Process cross-exchange arbitrage opportunities for all symbols
pub fn process_cross_exchange_arbitrage(
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Vec<CrossExchangeArb> {
    let mut all_opportunities = Vec::new();
    
    // Get symbols available on multiple exchanges
    let cross_symbols = get_cross_exchange_symbols(app_state);
    
    // Track how many cross-exchange checks we're doing
    app_state.increment_cross_exchange_checks(cross_symbols.len() as u64);
    
    // If there are no cross-symbols, return early
    if cross_symbols.is_empty() {
        return all_opportunities;
    }
    
    for symbol in cross_symbols {
        // Find opportunities for this symbol
        let opportunities = find_cross_exchange_opportunities(
            app_state,
            &symbol,
            exchange_fees
        );
        
        // Add to all opportunities
        all_opportunities.extend(opportunities);
    }
    
    // Sort by profit (descending)
    all_opportunities.sort_by(|a, b| {
        b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    all_opportunities
}

/// Find cross-exchange opportunities for a single symbol
pub fn find_cross_exchange_opportunities(
    app_state: &AppState,
    symbol: &str,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Vec<CrossExchangeArb> {
    let mut opportunities = Vec::new();
    
    // Generate all exchange pairs
    let exchanges = [Exchange::Phemex, Exchange::LBank, Exchange::XtCom, 
                     Exchange::TapBit, Exchange::Hbit, Exchange::Batonex, 
                     Exchange::CoinCatch];
    
    // Check each pair of exchanges
    for &buy_exchange in &exchanges {
        // Skip exchanges with open circuit breakers
        if init_error_tracker().is_circuit_open(buy_exchange) {
            continue;
        }
        
        for &sell_exchange in &exchanges {
            // Skip exchanges with open circuit breakers
            if init_error_tracker().is_circuit_open(sell_exchange) {
                continue;
            }
            
            // Skip same exchange
            if buy_exchange == sell_exchange {
                continue;
            }
            
            // Form exchange-specific symbols
            let buy_symbol = format!("{}:{}", buy_exchange, symbol);
            let sell_symbol = format!("{}:{}", sell_exchange, symbol);
            
            // Get price data for both exchanges
            if let (Some(buy_data), Some(sell_data)) = (
                app_state.price_data.get(&buy_symbol),
                app_state.price_data.get(&sell_symbol)
            ) {
                let buy_price = buy_data.best_ask;  // Buy at ask price
                let sell_price = sell_data.best_bid; // Sell at bid price
                
                // Fast path: Apply normalization for initial check
                let normalized_buy_price = normalize_price(buy_exchange, symbol, buy_price);
                let normalized_sell_price = normalize_price(sell_exchange, symbol, sell_price);
                
                // Skip if no gross profit possible after normalization
                if normalized_sell_price <= normalized_buy_price {
                    continue;
                }
                
                // Get orderbook data for slippage calculation
                let buy_orderbook = get_orderbook_for_exchange(app_state, &buy_symbol);
                let sell_orderbook = get_orderbook_for_exchange(app_state, &sell_symbol);
                
                // Compute potential profit with slippage consideration
                if let Some(opportunity) = compute_cross_exchange_profit_with_slippage(
                    symbol,
                    buy_exchange,
                    sell_exchange,
                    buy_price,
                    sell_price,
                    buy_orderbook,
                    sell_orderbook,
                    exchange_fees
                ) {
                    // Add additional validation
                    if passes_sanity_checks(
                        symbol,
                        buy_exchange,
                        sell_exchange,
                        opportunity.buy_price,
                        opportunity.sell_price,
                        opportunity.net_profit_pct
                    ) && is_new_opportunity(symbol, buy_exchange, sell_exchange) {
                        // Ensure this is a significant opportunity worth noting
                        if opportunity.net_profit_pct >= get_config().arbitrage.min_profit_threshold_pct {
                            opportunities.push(opportunity);
                        }
                    }
                }
            }
        }
    }
    
    opportunities
}

/// Process cross-exchange arbitrage with improved symbol mapping
pub fn process_mapped_cross_exchange_arbitrage(
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
) -> Vec<CrossExchangeArb> {
    let mut all_opportunities = Vec::new();
    
    // Group symbols by normalized representation
    let mut normalized_symbols: HashMap<String, Vec<String>> = HashMap::with_capacity(100);
    
    // First, gather data from all exchanges and normalize symbols by removing separators
    for entry in app_state.price_data.iter() {
        let full_symbol = entry.key().clone();
        let parts: Vec<&str> = full_symbol.split(':').collect();
        
        if parts.len() == 2 {
            // Normalize by removing '-' and '_' and converting to uppercase
            let normalized = parts[1]
                .replace('-', "")
                .replace('_', "")
                .to_uppercase();
            
            normalized_symbols
                .entry(normalized)
                .or_insert_with(Vec::new)
                .push(full_symbol);
        }
    }

    // Find symbols that exist on multiple exchanges
    let mut multi_exchange_symbols = Vec::new();
    for (normalized, symbols) in &normalized_symbols {
        if symbols.len() >= 2 {
            multi_exchange_symbols.push((normalized.clone(), symbols.clone()));
        }
    }
    
    if multi_exchange_symbols.is_empty() {
        return all_opportunities;
    }
    
    // Track number of checks
    app_state.increment_cross_exchange_checks(multi_exchange_symbols.len() as u64);
    
    // Process each normalized symbol
    for (normalized, exchange_symbols) in multi_exchange_symbols {
        let mut price_data = HashMap::new();
        
        for exchange_symbol in &exchange_symbols {
            if let Some(data) = app_state.price_data.get(exchange_symbol) {
                price_data.insert(exchange_symbol.clone(), data.value().clone());
            }
        }
        
        for i in 0..exchange_symbols.len() {
            let buy_symbol = &exchange_symbols[i];
            
            if !price_data.contains_key(buy_symbol) {
                continue;
            }
            
            let buy_exchange_str = buy_symbol.split(':').next().unwrap_or("");
            let buy_price = price_data[buy_symbol].best_ask;
            
            if buy_price <= 0.0 {
                continue;
            }
            
            let buy_exchange = match buy_exchange_str {
                "PHEMEX" => Exchange::Phemex,
                "LBANK" => Exchange::LBank,
                "XTCOM" => Exchange::XtCom,
                "TAPBIT" => Exchange::TapBit,
                "HBIT" => Exchange::Hbit,
                "BATONEX" => Exchange::Batonex,
                "COINCATCH" => Exchange::CoinCatch,
                _ => continue,
            };
            
            // Skip exchanges with open circuit breakers
            if init_error_tracker().is_circuit_open(buy_exchange) {
                continue;
            }
            
            for j in 0..exchange_symbols.len() {
                if i == j {
                    continue;
                }
                
                let sell_symbol = &exchange_symbols[j];
                
                if !price_data.contains_key(sell_symbol) {
                    continue;
                }
                
                let sell_exchange_str = sell_symbol.split(':').next().unwrap_or("");
                let sell_price = price_data[sell_symbol].best_bid;
                
                if sell_price <= 0.0 {
                    continue;
                }
                
                let sell_exchange = match sell_exchange_str {
                    "PHEMEX" => Exchange::Phemex,
                    "LBANK" => Exchange::LBank,
                    "XTCOM" => Exchange::XtCom,
                    "TAPBIT" => Exchange::TapBit,
                    "HBIT" => Exchange::Hbit,
                    "BATONEX" => Exchange::Batonex,
                    "COINCATCH" => Exchange::CoinCatch,
                    _ => continue,
                };
                
                // Skip exchanges with open circuit breakers
                if init_error_tracker().is_circuit_open(sell_exchange) {
                    continue;
                }
                
                // Normalize prices for fast path check
                let normalized_buy_price = normalize_price(buy_exchange, &normalized, buy_price);
                let normalized_sell_price = normalize_price(sell_exchange, &normalized, sell_price);
                
                // Skip if no gross profit possible after normalization
                if normalized_sell_price <= normalized_buy_price {
                    continue;
                }
                
                // Get orderbook data for slippage calculation 
                let buy_orderbook = get_orderbook_for_exchange(app_state, buy_symbol);
                let sell_orderbook = get_orderbook_for_exchange(app_state, sell_symbol);
                
                if let Some(opportunity) = compute_cross_exchange_profit_with_slippage(
                    &normalized,
                    buy_exchange,
                    sell_exchange,
                    buy_price,
                    sell_price,
                    buy_orderbook,
                    sell_orderbook,
                    exchange_fees
                ) {
                    // Add the new validation steps
                    if passes_sanity_checks(
                        &normalized,
                        buy_exchange,
                        sell_exchange,
                        opportunity.buy_price,
                        opportunity.sell_price,
                        opportunity.net_profit_pct
                    ) && is_new_opportunity(&normalized, buy_exchange, sell_exchange) {
                        all_opportunities.push(opportunity);
                    }
                }
            }
        }
    }
    
    all_opportunities.sort_by(|a, b| {
        b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    all_opportunities
}

/// Process a subset of symbols for parallel scanning
pub fn process_mapped_cross_exchange_arbitrage_subset(
    app_state: &AppState,
    exchange_fees: &HashMap<Exchange, ExchangeFees>,
    symbols: &HashSet<String>
) -> Vec<CrossExchangeArb> {
    let mut all_opportunities = Vec::with_capacity(10);
    
    // Group symbols by normalized representation
    let mut normalized_symbols: HashMap<String, Vec<String>> = HashMap::with_capacity(100);
    
    // First, gather data from all exchanges and normalize symbols by removing separators
    for entry in app_state.price_data.iter() {
        let full_symbol = entry.key().clone();
        let parts: Vec<&str> = full_symbol.split(':').collect();
        
        if parts.len() == 2 {
            // Normalize by removing '-' and '_' and converting to uppercase
            let normalized = parts[1]
                .replace('-', "")
                .replace('_', "")
                .to_uppercase();
            
            // Only process symbols in our subset
            if symbols.contains(&normalized) {
                normalized_symbols
                    .entry(normalized)
                    .or_insert_with(Vec::new)
                    .push(full_symbol);
            }
        }
    }

    // Find symbols that exist on multiple exchanges
    let mut multi_exchange_symbols = Vec::new();
    for (normalized, symbols) in &normalized_symbols {
        if symbols.len() >= 2 {
            multi_exchange_symbols.push((normalized.clone(), symbols.clone()));
        }
    }
    
    if multi_exchange_symbols.is_empty() {
        return all_opportunities;
    }
    
    // Track number of checks
    app_state.increment_cross_exchange_checks(multi_exchange_symbols.len() as u64);
    
    // Process each normalized symbol
    for (normalized, exchange_symbols) in multi_exchange_symbols {
        let mut price_data = HashMap::new();
        
        for exchange_symbol in &exchange_symbols {
            if let Some(data) = app_state.price_data.get(exchange_symbol) {
                price_data.insert(exchange_symbol.clone(), data.value().clone());
            }
        }
        
        for i in 0..exchange_symbols.len() {
            let buy_symbol = &exchange_symbols[i];
            
            if !price_data.contains_key(buy_symbol) {
                continue;
            }
            
            let buy_exchange_str = buy_symbol.split(':').next().unwrap_or("");
            let buy_price = price_data[buy_symbol].best_ask;
            
            if buy_price <= 0.0 {
                continue;
            }
            
            let buy_exchange = match buy_exchange_str {
                "PHEMEX" => Exchange::Phemex,
                "LBANK" => Exchange::LBank,
                "XTCOM" => Exchange::XtCom,
                "TAPBIT" => Exchange::TapBit,
                "HBIT" => Exchange::Hbit,
                "BATONEX" => Exchange::Batonex,
                "COINCATCH" => Exchange::CoinCatch,
                _ => continue,
            };
            
            // Skip exchanges with open circuit breakers
            if init_error_tracker().is_circuit_open(buy_exchange) {
                continue;
            }
            
            for j in 0..exchange_symbols.len() {
                if i == j {
                    continue;
                }
                
                let sell_symbol = &exchange_symbols[j];
                
                if !price_data.contains_key(sell_symbol) {
                    continue;
                }
                
                let sell_exchange_str = sell_symbol.split(':').next().unwrap_or("");
                let sell_price = price_data[sell_symbol].best_bid;
                
                if sell_price <= 0.0 {
                    continue;
                }
                
                let sell_exchange = match sell_exchange_str {
                    "PHEMEX" => Exchange::Phemex,
                    "LBANK" => Exchange::LBank,
                    "XTCOM" => Exchange::XtCom,
                    "TAPBIT" => Exchange::TapBit,
                    "HBIT" => Exchange::Hbit,
                    "BATONEX" => Exchange::Batonex,
                    "COINCATCH" => Exchange::CoinCatch,
                    _ => continue,
                };
                
                // Skip exchanges with open circuit breakers
                if init_error_tracker().is_circuit_open(sell_exchange) {
                    continue;
                }
                
                // Normalize prices for fast path check
                let normalized_buy_price = normalize_price(buy_exchange, &normalized, buy_price);
                let normalized_sell_price = normalize_price(sell_exchange, &normalized, sell_price);
                
                // Skip if no gross profit possible after normalization
                if normalized_sell_price <= normalized_buy_price {
                    continue;
                }
                
                // Get orderbook data for slippage calculation 
                let buy_orderbook = get_orderbook_for_exchange(app_state, buy_symbol);
                let sell_orderbook = get_orderbook_for_exchange(app_state, sell_symbol);
                
                if let Some(opportunity) = compute_cross_exchange_profit_with_slippage(
                    &normalized,
                    buy_exchange,
                    sell_exchange,
                    buy_price,
                    sell_price,
                    buy_orderbook,
                    sell_orderbook,
                    exchange_fees
                ) {
                    // Add the new validation steps
                    if passes_sanity_checks(
                        &normalized,
                        buy_exchange,
                        sell_exchange,
                        opportunity.buy_price,
                        opportunity.sell_price,
                        opportunity.net_profit_pct
                    ) && is_new_opportunity(&normalized, buy_exchange, sell_exchange) {
                        all_opportunities.push(opportunity);
                    }
                }
            }
        }
    }
    
    all_opportunities.sort_by(|a, b| {
        b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap_or(std::cmp::Ordering::Equal)
    });
    
    all_opportunities
}

/// Implementation of build_exchange_fees function
pub fn build_exchange_fees() -> HashMap<Exchange, ExchangeFees> {
    let config = get_config();
    let mut fees = HashMap::new();
    
    // For each exchange in the config, create appropriate fee structure
    for (exchange_name, exchange_config) in &config.exchanges {
        // Parse exchange from name
        if let Ok(exchange) = exchange_name.parse::<Exchange>() {
            fees.insert(
                exchange,
                ExchangeFees::new(
                    exchange,
                    exchange_config.maker_fee_pct / 100.0,
                    exchange_config.taker_fee_pct / 100.0
                )
            );
        }
    }
    
    // Add fallbacks for any missing exchanges
    if !fees.contains_key(&Exchange::Phemex) {
        fees.insert(
            Exchange::Phemex,
            ExchangeFees::new(
                Exchange::Phemex,
                0.001, // 0.1% maker
                0.0006 // 0.06% taker
            )
        );
    }
    
    // Add other exchanges as needed
    for exchange in [
        Exchange::LBank, Exchange::XtCom, Exchange::TapBit, 
        Exchange::Hbit, Exchange::Batonex, Exchange::CoinCatch
    ] {
        if !fees.contains_key(&exchange) {
            fees.insert(
                exchange,
                ExchangeFees::new(
                    exchange,
                    0.001, // Default 0.1% maker
                    0.0006 // Default 0.06% taker
                )
            );
        }
    }
    
    fees
}