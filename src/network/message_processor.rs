// message_processor.rs - Optimized message processing functions with SIMD-accelerated parsing

use crate::core::*;
use crate::utils::ensure_exchange_prefix;
use crate::json_parser::{parse_json, FastOrderbookParser};
use crate::error_handling::{init_error_tracker, record_error, ErrorCategory};
use crate::config::get_config;
use log::{error, debug, warn, info};
use serde_json::{json, Value};
use std::sync::Arc;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::str::FromStr;


// Thread-local parser pools to avoid allocation
lazy_static! {
    static ref ORDERBOOK_PARSERS: Arc<DashMap<String, FastOrderbookParser>> = 
        Arc::new(DashMap::new());
}

/// Normalize exchange-specific symbol formats to a standard format
#[inline(always)]
fn normalize_exchange_symbol(exchange: &str, symbol: &str) -> String {
    match exchange {
        "LBANK" => {
            // LBank uses lowercase with underscore (e.g., "btc_usdt")
            // Convert to uppercase without separator (e.g., "BTCUSDT")
            symbol.replace('_', "").to_uppercase()
        },
        "XTCOM" => {
            // XTCom uses lowercase with underscore (e.g., "btc_usdt")
            // Convert to uppercase without separator (e.g., "BTCUSDT")
            symbol.replace('_', "").to_uppercase()
        },
        "PHEMEX" => {
            // Phemex uses uppercase without separator
            // Just ensure it's uppercase
            symbol.to_uppercase()
        },
        "TAPBIT" => {
            // TapBit uses uppercase with hyphen (e.g., "BTC-USDT")
            // Convert to uppercase without separator (e.g., "BTCUSDT")
            symbol.replace('-', "").to_uppercase()
        },
        "HBIT" => {
            // Hbit uses lowercase with underscore (e.g., "btc_usdt")
            // Convert to uppercase without separator (e.g., "BTCUSDT")
            symbol.replace('_', "").to_uppercase()
        },
        "BATONEX" => {
            // Batonex formatting
            symbol.replace('-', "").replace('_', "").to_uppercase()
        },
        "COINCATCH" => {
            // Coincatch formatting
            symbol.replace('-', "").replace('_', "").to_uppercase()
        },
        _ => {
            // Default normalization
            symbol.to_uppercase()
        }
    }
}

/// Process orderbook update for LBank - extracted for better code organization
#[inline]
fn process_lbank_depth_update(
    connection_id: &str,
    app_state: &AppState,
    value: &Value,
) -> Result<bool, AppError> {
    let pair = value.get("pair").and_then(|p| p.as_str()).unwrap_or("unknown");
    
    // Convert "btc_usdt" to "BTCUSDT"
    let normalized_symbol = normalize_exchange_symbol("LBANK", pair);
    let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "LBANK");
    
    // Use SIMD-accelerated parsing when enabled
    if get_config().features.enable_simd_json {
        let connection_key = connection_id.to_string();
        // Replace
        let mut parser_entry = ORDERBOOK_PARSERS.entry(connection_key);
        let mut parser = parser_entry.or_insert_with(|| FastOrderbookParser::new(20));

// With
let parser = ORDERBOOK_PARSERS.entry(connection_key.clone())
    .or_insert_with(|| FastOrderbookParser::new(20));
            
        if let Some(depth_obj) = value.get("depth") {
            // Get scale from app_state
            let scale = app_state.price_scales.get(&normalized_symbol)
                .map(|entry| *entry.value())
                .unwrap_or(8); // Default to 8
                
            let (best_ask, best_bid, depth_asks, depth_bids) = parser.parse_orderbook(
                depth_obj, "asks", "bids", scale);
                
            if let (Some(best_ask), Some(best_bid)) = (best_ask, best_bid) {
                // Skip if prices are invalid
                if best_ask <= 0.0 || best_bid <= 0.0 {
                    return Ok(false);
                }
                
                let current_time = chrono::Utc::now().timestamp_millis();
                
                // Enqueue update to orderbook processor
                if let Some(tx) = &app_state.orderbook_queue {
                    let update = OrderbookUpdate {
                        symbol: prefixed_symbol.clone(),
                        best_ask,
                        best_bid,
                        timestamp: current_time,
                        scale,
                        is_synthetic: false,
                        leg1: None,
                        leg2: None,
                        depth_asks: Some(depth_asks.to_vec()),
                        depth_bids: Some(depth_bids.to_vec()),
                    };
                    
                    if let Err(e) = tx.send(update) {
                        error!("LBank {}: Failed to send orderbook update: {}", connection_id, e);
                        return Ok(false);
                    } else {
                        debug!("LBank {}: Orderbook updated for {}: bid={}, ask={}", 
                            connection_id, pair, best_bid, best_ask);
                        return Ok(true);
                    }
                } else {
                    // Fallback: update price data directly
                    app_state.price_data.insert(
                        prefixed_symbol,
                        PriceData {
                            best_ask,
                            best_bid,
                            timestamp: current_time,
                            scale,
                            is_synthetic: false,
                            leg1: None,
                            leg2: None,
                            depth_asks: Some(depth_asks.to_vec()),
                            depth_bids: Some(depth_bids.to_vec()),
                        },
                    );
                    return Ok(true);
                }
            }
        }
        return Ok(false);
    } else {
        // Legacy processing without SIMD
        if let Some(depth_obj) = value.get("depth") {
            let empty_vec = vec![];
            let asks = depth_obj.get("asks").and_then(|a| a.as_array()).unwrap_or(&empty_vec);
            let bids = depth_obj.get("bids").and_then(|b| b.as_array()).unwrap_or(&empty_vec);
            
            // Get scale from app_state
            let scale = app_state.price_scales.get(&normalized_symbol)
                .map(|entry| *entry.value())
                .unwrap_or(8); // Default to 8
            
            // Extract best prices
            let best_ask = if !asks.is_empty() {
                match asks[0].as_array() {
                    Some(arr) if !arr.is_empty() => {
                        arr[0].as_f64().unwrap_or(0.0)
                    },
                    _ => 0.0
                }
            } else {
                0.0
            };
            
            let best_bid = if !bids.is_empty() {
                match bids[0].as_array() {
                    Some(arr) if !arr.is_empty() => {
                        arr[0].as_f64().unwrap_or(0.0)
                    },
                    _ => 0.0
                }
            } else {
                0.0
            };
            
            // Skip if prices are invalid
            if best_ask <= 0.0 || best_bid <= 0.0 {
                return Ok(false);
            }
            
            // Extract full depth data for accurate slippage calculation
            let depth_asks = asks.iter()
                .filter_map(|level| {
                    level.as_array().and_then(|arr| {
                        if arr.len() >= 2 {
                            let price = arr[0].as_f64()?;
                            let qty = arr[1].as_f64()?;
                            if price > 0.0 && qty > 0.0 {
                                Some((price, qty))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<(f64, f64)>>();
            
            let depth_bids = bids.iter()
                .filter_map(|level| {
                    level.as_array().and_then(|arr| {
                        if arr.len() >= 2 {
                            let price = arr[0].as_f64()?;
                            let qty = arr[1].as_f64()?;
                            if price > 0.0 && qty > 0.0 {
                                Some((price, qty))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<(f64, f64)>>();
            
            let current_time = chrono::Utc::now().timestamp_millis();
            
            // Enqueue update to orderbook processor
            if let Some(tx) = &app_state.orderbook_queue {
                let update = OrderbookUpdate {
                    symbol: prefixed_symbol.clone(),
                    best_ask,
                    best_bid,
                    timestamp: current_time,
                    scale,
                    is_synthetic: false,
                    leg1: None,
                    leg2: None,
                    depth_asks: Some(depth_asks),
                    depth_bids: Some(depth_bids),
                };
                
                if let Err(e) = tx.send(update) {
                    error!("LBank {}: Failed to send orderbook update: {}", connection_id, e);
                    Ok(false)
                } else {
                    debug!("LBank {}: Orderbook updated for {}: bid={}, ask={}", 
                        connection_id, pair, best_bid, best_ask);
                    Ok(true)
                }
            } else {
                // Fallback: update price data directly
                app_state.price_data.insert(
                    prefixed_symbol,
                    PriceData {
                        best_ask,
                        best_bid,
                        timestamp: current_time,
                        scale,
                        is_synthetic: false,
                        leg1: None,
                        leg2: None,
                        depth_asks: Some(depth_asks),
                        depth_bids: Some(depth_bids),
                    },
                );
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }
}

/// Process orderbook update for XTCom - extracted for better code organization
#[inline]
fn process_xtcom_depth_update(
    connection_id: &str,
    app_state: &AppState,
    data: &Value,
) -> Result<bool, AppError> {
    if let Some(symbol_info) = data.get("s") {
        let pair = symbol_info.as_str().unwrap_or("unknown");
        
        // Check if we have asks and bids data
        if let (Some(asks), Some(bids)) = (data.get("a"), data.get("b")) {
            if let (Some(asks_array), Some(bids_array)) = (asks.as_array(), bids.as_array()) {
                // Convert pair (e.g., "btc_usdt") to symbol format (e.g., "BTCUSDT")
                let normalized_symbol = normalize_exchange_symbol("XTCOM", pair);
                let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "XTCOM");
                
                // Get scale from app_state
                let scale = app_state.price_scales.get(&normalized_symbol)
                    .map(|entry| *entry.value())
                    .unwrap_or(8); // Default to 8
                
                // Get best bid and ask from the arrays
                let best_ask = if !asks_array.is_empty() {
                    asks_array[0][0].as_str().unwrap_or("0")
                        .parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                };
                
                let best_bid = if !bids_array.is_empty() {
                    bids_array[0][0].as_str().unwrap_or("0")
                        .parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                };
                
                // Skip if prices are invalid
                if best_ask <= 0.0 || best_bid <= 0.0 {
                    return Ok(false);
                }
                
                // Extract full depth data for accurate slippage calculation
                let depth_asks = asks_array.iter()
                    .filter_map(|level| {
                        if let Some(level_array) = level.as_array() {
                            if level_array.len() >= 2 {
                                let price = level_array[0].as_str().unwrap_or("0")
                                    .parse::<f64>().unwrap_or(0.0);
                                let quantity = level_array[1].as_str().unwrap_or("0")
                                    .parse::<f64>().unwrap_or(0.0);
                                
                                if price > 0.0 && quantity > 0.0 {
                                    Some((price, quantity))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(f64, f64)>>();
                
                let depth_bids = bids_array.iter()
                    .filter_map(|level| {
                        if let Some(level_array) = level.as_array() {
                            if level_array.len() >= 2 {
                                let price = level_array[0].as_str().unwrap_or("0")
                                    .parse::<f64>().unwrap_or(0.0);
                                let quantity = level_array[1].as_str().unwrap_or("0")
                                    .parse::<f64>().unwrap_or(0.0);
                                
                                if price > 0.0 && quantity > 0.0 {
                                    Some((price, quantity))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(f64, f64)>>();
                
                let current_time = chrono::Utc::now().timestamp_millis();
                
                // Update price data
                if let Some(tx) = &app_state.orderbook_queue {
                    let update = OrderbookUpdate {
                        symbol: prefixed_symbol.clone(),
                        best_ask,
                        best_bid,
                        timestamp: current_time,
                        scale,
                        is_synthetic: false,
                        leg1: None,
                        leg2: None,
                        depth_asks: Some(depth_asks),
                        depth_bids: Some(depth_bids),
                    };
                    
                    if let Err(e) = tx.send(update) {
                        error!("XTCom {}: Failed to send orderbook update: {}", connection_id, e);
                        Ok(false)
                    } else {
                        debug!("XTCom {}: Orderbook updated for {}: bid={}, ask={}", 
                            connection_id, prefixed_symbol, best_bid, best_ask);
                        Ok(true)
                    }
                } else {
                    app_state.price_data.insert(
                        prefixed_symbol,
                        PriceData {
                            best_ask,
                            best_bid,
                            timestamp: current_time,
                            scale,
                            is_synthetic: false,
                            leg1: None,
                            leg2: None,
                            depth_asks: Some(depth_asks),
                            depth_bids: Some(depth_bids),
                        },
                    );
                    Ok(true)
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// Process orderbook update for Phemex - extracted for better code organization
#[inline]
fn process_phemex_orderbook_update(
    connection_id: &str,
    app_state: &AppState,
    symbol: &str, 
    orderbook_p: &Value,
) -> Result<bool, AppError> {
    if let Some(scale_entry) = app_state.price_scales.get(symbol) {
        let scale = *scale_entry.value();
        let divisor = 10_f64.powi(scale);

        // Extract ask and bid arrays
        if let (Some(asks), Some(bids)) = (orderbook_p.get("asks"), orderbook_p.get("bids")) {
            if let (Some(asks_array), Some(bids_array)) = (asks.as_array(), bids.as_array()) {
                // Get previous prices to use as fallback
                let prefixed_symbol = ensure_exchange_prefix(symbol, "PHEMEX");
                let (prev_ask, prev_bid) = if let Some(prev_price) = app_state.price_data.get(&prefixed_symbol) {
                    let prev = prev_price.value();
                    (prev.best_ask, prev.best_bid)
                } else {
                    (0.0, 0.0)
                };
                
                // Get best prices
                let best_ask = if !asks_array.is_empty() {
                    asks_array[0][0]
                        .as_str()
                        .unwrap_or("0")
                        .parse::<f64>()
                        .unwrap_or(0.0) / divisor
                } else {
                    prev_ask
                };
                
                let best_bid = if !bids_array.is_empty() {
                    bids_array[0][0]
                        .as_str()
                        .unwrap_or("0")
                        .parse::<f64>()
                        .unwrap_or(0.0) / divisor
                } else {
                    prev_bid
                };
                
                // Skip if prices are invalid
                if best_ask <= 0.0 || best_bid <= 0.0 {
                    return Ok(false);
                }
                
                // Extract full depth data for accurate slippage calculation
                let depth_asks = asks_array.iter()
                    .filter_map(|level| {
                        if level.as_array().map_or(false, |arr| arr.len() >= 2) {
                            let price = level[0].as_str().unwrap_or("0")
                                .parse::<f64>().unwrap_or(0.0) / divisor;
                            let quantity = level[1].as_str().unwrap_or("0")
                                .parse::<f64>().unwrap_or(0.0);
                            
                            if price > 0.0 && quantity > 0.0 {
                                Some((price, quantity))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(f64, f64)>>();
                    
                let depth_bids = bids_array.iter()
                    .filter_map(|level| {
                        if level.as_array().map_or(false, |arr| arr.len() >= 2) {
                            let price = level[0].as_str().unwrap_or("0")
                                .parse::<f64>().unwrap_or(0.0) / divisor;
                            let quantity = level[1].as_str().unwrap_or("0")
                                .parse::<f64>().unwrap_or(0.0);
                            
                            if price > 0.0 && quantity > 0.0 {
                                Some((price, quantity))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(f64, f64)>>();
                
                let current_time = chrono::Utc::now().timestamp_millis();
                
                // Send to orderbook queue
                if let Some(tx) = &app_state.orderbook_queue {
                    let update = OrderbookUpdate {
                        symbol: prefixed_symbol.clone(),
                        best_ask,
                        best_bid,
                        timestamp: current_time,
                        scale,
                        is_synthetic: false,
                        leg1: None,
                        leg2: None,
                        depth_asks: Some(depth_asks),
                        depth_bids: Some(depth_bids),
                    };
                    
                    if let Err(e) = tx.send(update) {
                        error!("Connection {}: Failed to send orderbook update: {}", connection_id, e);
                        Ok(false)
                    } else {
                        debug!("Connection {}: Orderbook updated for {}: bid={}, ask={}", 
                            connection_id, symbol, best_bid, best_ask);
                        Ok(true)
                    }
                } else {
                    app_state.price_data.insert(
                        prefixed_symbol,
                        PriceData {
                            best_ask,
                            best_bid,
                            timestamp: current_time,
                            scale,
                            is_synthetic: false,
                            leg1: None,
                            leg2: None,
                            depth_asks: Some(depth_asks),
                            depth_bids: Some(depth_bids),
                        },
                    );
                    Ok(true)
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// Try SIMD-accelerated JSON parsing if enabled
fn try_simd_parse(message: &str) -> Result<Value, AppError> {
    if get_config().features.enable_simd_json {
        // Create a mutable copy for SIMD parsing
        let mut message_bytes = message.as_bytes().to_vec();
        
        match parse_json(&mut message_bytes) {
            Ok(value) => Ok(value.into_owned()),
            Err(_) => {
                // Fallback to standard parsing:
                serde_json::from_str(message).map_err(|e| AppError::SerializationError(e))
            }
        }
        
    } else {
        // Use standard parsing
        serde_json::from_str(message).map_err(|e| AppError::SerializationError(e))
    }
}

/// Process incoming WebSocket messages with reliable timestamp tracking
pub async fn process_message(app_state: &AppState, connection_id: &str, message: &str) -> Result<bool, AppError> {
    // Always update connection timestamp first thing
    app_state.update_connection_timestamp(connection_id);
    
    // Increment WebSocket message counter
    app_state.increment_websocket_messages(1);
    
    // Skip detailed processing for pong messages - fast path
    if message.contains("pong") || message.contains("\"status\":\"success\"") {
        if message.contains("pong") {
            debug!("Connection {}: Received pong response", connection_id);
        }
        return Ok(true);
    }
    
    // Extract exchange from connection_id
    let exchange = if connection_id.starts_with("conn-") {
        "PHEMEX"
    } else if connection_id.starts_with("lbank-") {
        "LBANK"
    } else if connection_id.starts_with("xtcom-") {
        "XTCOM"
    } else if connection_id.starts_with("tapbit-") {
        "TAPBIT"
    } else if connection_id.starts_with("hbit-") {
        "HBIT"
    } else if connection_id.starts_with("batonex-") {
        "BATONEX"
    } else if connection_id.starts_with("coincatch-") {
        "COINCATCH"
    } else {
        "UNKNOWN"
    };
    
    // Parse the message
    let message_value: Value = match try_simd_parse(message) {
        Ok(value) => value,
        Err(e) => {
            error!("Connection {}: Failed to parse message: {}", connection_id, e);
            
            // Record parsing error with proper exchange
            if let Ok(exchange_enum) = exchange.parse::<trifury::exchange_types::Exchange>() {
                record_error(
                    exchange_enum,
                    Some(connection_id),
                    &AppError::SerializationError(
                        serde_json::Error::custom(format!("Failed to parse message: {}", e))
                    )
                );
            }
            
            return Err(AppError::SerializationError(
                serde_json::Error::custom(format!("Failed to parse message: {}", e))
            ));
        }
    };
    
    // Process based on message type
    let mut processed = false;
    
    // ====================================================================
    // LBANK SPECIFIC MESSAGE PROCESSING
    // ====================================================================
    if connection_id.starts_with("lbank-") {
        // Process LBank depth data messages
        if let Some(msg_type) = message_value.get("type").and_then(|t| t.as_str()) {
            if msg_type.eq_ignore_ascii_case("depth") {
                processed = process_lbank_depth_update(connection_id, app_state, &message_value)?;
            }
        }
        
        // Handle pings from LBank (they expect a pong response)
        if message_value.get("action").and_then(|a| a.as_str()) == Some("ping") {
            debug!("LBank {}: Received ping, will respond with pong", connection_id);
            processed = true;
        }
    }
    
    // ====================================================================
    // XTCOM SPECIFIC MESSAGE PROCESSING
    // ====================================================================
    else if connection_id.starts_with("xtcom-") && message.contains("depth_update") {
        if let Some(data) = message_value.get("data") {
            processed = process_xtcom_depth_update(connection_id, app_state, data)?;
        }
    }
    
    // ====================================================================
    // PHEMEX (DEFAULT) MESSAGE PROCESSING
    // ====================================================================
    else {
        // Check for orderbook_p updates (most common case)
        if let (Some(orderbook_p), Some(symbol_value)) = (message_value.get("orderbook_p"), message_value.get("symbol")) {
            let symbol = symbol_value.as_str().unwrap_or("");
            processed = process_phemex_orderbook_update(connection_id, app_state, symbol, orderbook_p)?;
        }    
        
        // Handle pong responses
        if is_pong_response(&message_value) {
            if let Some(id) = message_value.get("id").and_then(|i| i.as_u64()) {
                debug!("Connection {}: Received pong response for ID: {}", connection_id, id);
            }
            processed = true;
        }
    }
    
    // Always update timestamp at the end
    app_state.update_connection_timestamp(connection_id);
    
    Ok(processed)
}

/// Helper function to detect if a message is a pong response
#[inline(always)]
fn is_pong_response(value: &Value) -> bool {
    // Case 1: JSON response with result = "pong"
    if value.get("result") == Some(&json!("pong")) {
        return true;
    }
    
    // Case 2: Response with result.status = "success"
    if let Some(result) = value.get("result") {
        if let Some(obj) = result.as_object() {
            if obj.get("status") == Some(&json!("success")) {
                return true;
            }
        }
    }
    
    // Case 3: Response with id and null error
    if value.get("id").is_some() && value.get("error") == Some(&json!(null)) {
        return true;
    }
    
    false
}