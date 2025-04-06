use crate::core::*;
use std::collections::{HashSet, HashMap};
use lazy_static::lazy_static;
use log::info;

lazy_static! {
    static ref TOP_TOKENS_SET: HashSet<&'static str> = {
        let mut set = HashSet::new();
        for &token in TOP_TOKENS.iter() {
            set.insert(token);
        }
        set
    };
}

/// Ensure a symbol has an exchange prefix
pub fn ensure_exchange_prefix(symbol: &str, default_exchange: &str) -> String {
    if symbol.contains(':') {
        // Already has an exchange prefix
        symbol.to_string()
    } else {
        // Add the default exchange prefix
        format!("{}:{}", default_exchange, symbol.to_uppercase())
    }
}

/// Parse a Phemex perpetual symbol into base and quote tokens.
/// Example: 'BTCUSDT' -> Some(("BTC", "USDT"))
pub fn parse_perpetual_symbol(symbol: &str) -> Option<(String, String)> {
    // Perpetual symbols don't have a prefix
    if symbol.starts_with('s') || symbol.starts_with("SYN-") {
        return None;
    }
    
    // For USDT-margined perpetuals, we know the quote token is USDT
    if symbol.ends_with("USDT") {
        // Check if the symbol starts with any of our TOP_TOKENS
        for &base_token in TOP_TOKENS.iter() {
            if symbol.starts_with(base_token) && base_token != "USDT" {
                let potential_match = format!("{}{}", base_token, "USDT");
                if potential_match == symbol {
                    return Some((base_token.to_string(), "USDT".to_string()));
                }
            }
        }
    }
    
    // If we couldn't parse it as a USDT-margined contract, try generic parsing
    for &quote_token in TOP_TOKENS.iter().rev() {
        if symbol.ends_with(quote_token) {
            let base_token = &symbol[..symbol.len() - quote_token.len()];
            if TOP_TOKENS_SET.contains(base_token) {
                return Some((base_token.to_string(), quote_token.to_string()));
            }
        }
    }
    
    None
}

/// Parse any symbol type (spot, perpetual, or synthetic) into base and quote tokens
pub fn parse_any_symbol(symbol: &str) -> Option<(String, String)> {
    if symbol.starts_with('s') {
        // Format: "sBTCUSDT" -> Some(("BTC", "USDT"))
        parse_perpetual_symbol(&symbol[1..])
    } else if symbol.starts_with("SYN-") {
        // Format: "SYN-BASE-QUOTE"
        let parts: Vec<&str> = symbol.split('-').collect();
        if parts.len() == 3 {
            Some((parts[1].to_string(), parts[2].to_string()))
        } else {
            None
        }
    } else {
        parse_perpetual_symbol(symbol)
    }
}

/// Determine if a pair of tokens should be included based on the active focus mode
pub fn should_include_pair(base_token: &str, quote_token: &str) -> bool {
    // Simplified function that only uses TOP_TOKENS_SET
    TOP_TOKENS_SET.contains(base_token) && TOP_TOKENS_SET.contains(quote_token)
}

/// Safe conversion from string to float
pub fn safe_parse_f64(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

/// Apply price scale to convert integer price to float
pub fn apply_price_scale(price: f64, scale: i32) -> f64 {
    price / 10_f64.powi(scale)
}


/// Analyze which tokens exist on which exchanges to help debug cross-exchange issues
pub fn analyze_exchange_token_distribution(app_state: &AppState) {
    // Maps to track tokens per exchange
    let mut exchange_tokens: HashMap<String, HashSet<String>> = HashMap::new();
    
    // Process all price data entries
    for entry in app_state.price_data.iter() {
        let full_symbol = entry.key();
        let parts: Vec<&str> = full_symbol.split(':').collect();
        
        if parts.len() == 2 {
            let exchange = parts[0].to_string();
            let symbol = parts[1].to_string();
            
            // Add to the exchange's token set
            exchange_tokens
                .entry(exchange)
                .or_insert_with(HashSet::new)
                .insert(symbol);
        }
    }
    
    // Log token counts by exchange
    info!("===== TOKEN DISTRIBUTION ANALYSIS =====");
    
    // Get a list of all exchanges
    let exchanges: Vec<String> = exchange_tokens.keys().cloned().collect();
    
    for exchange in &exchanges {
        let tokens = exchange_tokens.get(exchange).unwrap();
        info!("{} has {} tokens", exchange, tokens.len());
    }
    
    // Find tokens that are on each exchange but not on others
    if exchanges.len() > 1 {
        for (i, exchange1) in exchanges.iter().enumerate() {
            let tokens1 = exchange_tokens.get(exchange1).unwrap();
            
            for exchange2 in exchanges.iter().skip(i + 1) {
                let tokens2 = exchange_tokens.get(exchange2).unwrap();
                
                // Find tokens in exchange1 but not in exchange2
                let mut only_in_1 = Vec::new();
                // Find tokens in exchange2 but not in exchange1
                let mut only_in_2 = Vec::new();
                // Find common tokens
                let mut common = Vec::new();
                
                for token in tokens1 {
                    if tokens2.contains(token) {
                        common.push(token);
                    } else {
                        only_in_1.push(token);
                    }
                }
                
                for token in tokens2 {
                    if !tokens1.contains(token) {
                        only_in_2.push(token);
                    }
                }
                
                // Print helpful information about token overlap
                info!("{} and {} have {} tokens in common", exchange1, exchange2, common.len());
                
                // Show a sample of tokens that could be on both exchanges but aren't
                if !only_in_1.is_empty() && !only_in_2.is_empty() {
                    info!("Token format examples that might need normalization:");
                    
                    // Show top 5 tokens from each exchange that might be the same but differently formatted
                    for i in 0..std::cmp::min(5, only_in_1.len()) {
                        info!("  {} only on {}: {}", i+1, exchange1, only_in_1[i]);
                    }
                    
                    for i in 0..std::cmp::min(5, only_in_2.len()) {
                        info!("  {} only on {}: {}", i+1, exchange2, only_in_2[i]);
                    }
                }
            }
        }
    }
    
    info!("=======================================");
}

