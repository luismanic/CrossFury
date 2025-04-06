// network/api_client.rs - Optimized API client functions

use crate::core::*;
use crate::utils::should_include_pair;
use crate::token_lists::TARGET_TOKENS;
use log::{info, error};
use serde_json::{json};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Extract base token from a symbol (e.g., "BTCUSDT" -> "BTC")
fn extract_base_token(symbol: &str) -> String {
    let quote_currencies = ["USDT", "USD", "BTC", "ETH", "USDC", "BNB"];
    
    for &quote in &quote_currencies {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            return symbol[..symbol.len() - quote.len()].to_string();
        }
    }
    
    // If we can't extract, return a default
    "UNKNOWN".to_string()
}

/// Extract quote token from a symbol (e.g., "BTCUSDT" -> "USDT")
fn extract_quote_token(symbol: &str) -> String {
    let quote_currencies = ["USDT", "USD", "BTC", "ETH", "USDC", "BNB"];
    
    for &quote in &quote_currencies {
        if symbol.ends_with(quote) && symbol.len() > quote.len() {
            return quote.to_string();
        }
    }
    
    // If we can't extract, return a default
    "USDT".to_string()
}

/// Get appropriate price scale for a token
fn get_token_price_scale(base_token: &str) -> i32 {
    match base_token {
        "BTC" => 1,
        "ETH" => 2,
        "BNB" | "SOL" => 3,
        "SUI" | "LINK" => 4,
        "XRP" | "ADA" => 5,
        "DOGE" => 7,
        "FARTCOIN" | "SHIB" | "PEPE" => 8,
        _ => 5, // Default scale
    }
}

/// Load cached products from a JSON file if it exists
pub async fn load_cached_products(cache_file: &str) -> Result<HashMap<String, Product>, AppError> {
    let path = Path::new(cache_file);
    if !path.exists() {
        return Ok(HashMap::new());
    }
    
    match fs::read_to_string(path) {
        Ok(data) => {
            let products: HashMap<String, Product> = serde_json::from_str(&data)?;
            info!("Loaded products from cache: {} ({} products)", cache_file, products.len());
            Ok(products)
        }
        Err(err) => {
            error!("Error loading products cache {}: {}", cache_file, err);
            Ok(HashMap::new())
        }
    }
}

/// Save products to a cache file
pub async fn save_cached_products(products: &HashMap<String, Product>, cache_file: &str) -> Result<(), AppError> {
    let json_data = serde_json::to_string(products)?;
    match fs::write(cache_file, json_data) {
        Ok(_) => {
            info!("Saved products to cache: {}", cache_file);
            Ok(())
        }
        Err(err) => {
            error!("Error saving products cache {}: {}", cache_file, err);
            Err(AppError::IoError(err))
        }
    }
}

/// Get perpetual trading products with improved symbol_to_tokens mapping
pub async fn get_perpetual_products(app_state: &AppState) -> Result<HashMap<String, Product>, AppError> {
    // First try to load from cache
    let cached_products = load_cached_products(PERP_PRODUCTS_CACHE_FILE).await?;
    if !cached_products.is_empty() {
        // In focus mode, we should return the filtered list
        if ACTIVE_FOCUS_MODE {
            // Filter the cached products to only include FOCUS_PERP_TOKENS
            let mut filtered_products = HashMap::new();
            for (symbol, product) in cached_products {
                if should_include_pair(&product.base_token, &product.quote_token) {
                    app_state.price_scales.insert(symbol.clone(), product.price_scale);
                    
                    // Update symbol_to_tokens mapping
                    app_state.symbol_to_tokens.insert(
                        symbol.clone(), 
                        (product.base_token.clone(), product.quote_token.clone())
                    );
                    
                    filtered_products.insert(symbol, product);
                }
            }
            
            info!(
                "Filtered {} perpetual products from cache with FOCUS_PERP_TOKENS",
                filtered_products.len()
            );
            return Ok(filtered_products);
        }
        
        // Restore price scales and token mappings
        for (symbol, product) in &cached_products {
            app_state.price_scales.insert(symbol.clone(), product.price_scale);
            
            // Update symbol_to_tokens mapping
            app_state.symbol_to_tokens.insert(
                symbol.clone(), 
                (product.base_token.clone(), product.quote_token.clone())
            );
        }
        
        return Ok(cached_products);
    }
    
    // Dynamically generate perpetual contracts from TARGET_TOKENS
    let perpetual_contracts: Vec<serde_json::Value> = TARGET_TOKENS.iter()
        .map(|&token| {
            let base_token = extract_base_token(token);
            let quote_token = extract_quote_token(token);
            let price_scale = get_token_price_scale(&base_token);
            
            // Set qty_scale based on token importance
            let qty_scale = match base_token.as_str() {
                "BTC" | "ETH" => 3,
                "BNB" => 2,
                "XRP" | "LINK" => 1,
                _ => 0,
            };
            
            json!({
                "symbol": token,
                "priceScale": price_scale,
                "qtyScale": qty_scale,
                "baseToken": base_token,
                "quoteToken": quote_token
            })
        })
        .collect();

    info!("Generated {} perpetual contracts from TARGET_TOKENS", perpetual_contracts.len());
    
    // Process the perpetual contracts
    let mut filtered_products = HashMap::new();
    for contract in perpetual_contracts {
        let symbol = contract["symbol"].as_str().unwrap_or("").to_string();
        let base_token = contract["baseToken"].as_str().unwrap_or("").to_string();
        let quote_token = contract["quoteToken"].as_str().unwrap_or("").to_string();
        
        // Special case for SHIB1000USDT -> convert to SHIB/USDT
        let (base_token, symbol) = if symbol == "SHIB1000USDT" {
            ("SHIB".to_string(), "SHIBUSDT".to_string())
        } else {
            (base_token, symbol)
        };
        
        // Filter based on mode
        if should_include_pair(&base_token, &quote_token) {
            let price_scale = contract["priceScale"].as_i64().unwrap_or(8) as i32;
            
            // Store the price scale for later use
            app_state.price_scales.insert(symbol.clone(), price_scale);
            
            // Update symbol_to_tokens mapping
            app_state.symbol_to_tokens.insert(
                symbol.clone(), 
                (base_token.clone(), quote_token.clone())
            );
            
            let product = Product {
                symbol: symbol.clone(),
                base_token,
                quote_token,
                status: "Listed".to_string(), // Default status for hardcoded contracts
                price_scale,
                qty_scale: contract["qtyScale"].as_i64().unwrap_or(0) as i32,
                symbol_type: SYMBOL_TYPE_PERPETUAL.to_string(),
            };
            
            filtered_products.insert(symbol, product);
        }
    }
    
    info!(
        "Using {} perpetual products with {}",
        filtered_products.len(),
        if ACTIVE_FOCUS_MODE { "FOCUS_PERP_TOKENS" } else { "TOP_TOKENS" }
    );
    
    // Save to cache for future use
    save_cached_products(&filtered_products, PERP_PRODUCTS_CACHE_FILE).await?;
    
    Ok(filtered_products)
}

/// Function to distribute symbols across connections
pub async fn distribute_symbols(
    symbols: Vec<(String, String)>, 
    max_connections: usize
) -> Vec<Vec<(String, String)>> {
    if symbols.is_empty() {
        return Vec::new();
    }
    
    // Calculate symbols per connection using ceiling division
    let symbols_per_connection = std::cmp::min(
        MAX_SUBSCRIPTIONS_PER_CONNECTION,
        (symbols.len() + max_connections - 1) / max_connections
    );
    
    // Create chunks of symbols
    let mut result = Vec::new();
    
    for chunk_start in (0..symbols.len()).step_by(symbols_per_connection) {
        let chunk_end = std::cmp::min(chunk_start + symbols_per_connection, symbols.len());
        
        // Create a new vector for this chunk and copy the symbols into it
        let chunk = symbols[chunk_start..chunk_end].to_vec();
        
        if !chunk.is_empty() {
            result.push(chunk);
        }
    }
    
    // If we have more chunks than max_connections, merge the smallest ones
    while result.len() > max_connections && result.len() >= 2 {
        // Sort chunks by size (smallest first)
        result.sort_by_key(|chunk| chunk.len());
        
        // Try to merge the two smallest chunks
        let smallest = result.remove(0);
        let second_smallest = result.remove(0);
        
        // Check if merged size is within limit
        let merged_size = smallest.len() + second_smallest.len();
        if merged_size <= MAX_SUBSCRIPTIONS_PER_CONNECTION {
            // Merge the chunks
            let mut merged = smallest;
            merged.extend(second_smallest);
            result.push(merged);
        } else {
            // If they can't be merged, keep the larger one and discard the smaller
            result.push(second_smallest);
        }
    }
    
    // Log distribution details
    let distribution: Vec<usize> = result.iter().map(|chunk| chunk.len()).collect();
    info!(
        "Symbol distribution across {} connections: {:?}",
        result.len(),
        distribution
    );
    
    result
}