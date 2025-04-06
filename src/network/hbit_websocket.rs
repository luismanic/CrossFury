// hbit_websocket.rs - Hbit WebSocket handler implementation

use crate::core::*;
use crate::utils::ensure_exchange_prefix;
use futures::{SinkExt, StreamExt};
use log::{info, error, warn, debug};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use std::sync::Arc;

// Hbit WebSocket URL
const HBIT_WS_URL: &str = "wss://fapi.hibt0.com/v2/ws";

/// Handler for Hbit WebSocket connections
pub async fn hbit_websocket_handler(
    symbols: Vec<String>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    let connection_id = format!("hbit-{}", connection_index + 1);
    app_state.update_connection_timestamp(&connection_id);
    app_state.clear_reconnect_signal(&connection_id);

    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!("Hbit {}: Connecting to {}", connection_id, HBIT_WS_URL);
        
        // Create request with required headers
        let mut request = HBIT_WS_URL.into_client_request()
            .map_err(|e| AppError::WebSocketError(format!("Failed to create request: {}", e)))?;
        
        // Add standard headers
        request.headers_mut().insert(
            "User-Agent", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36".parse().unwrap()
        );

        // Connect with timeout
        match timeout(Duration::from_secs(15), connect_async(request)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Hbit {}: Connection established", connection_id);
                app_state.update_connection_timestamp(&connection_id);
                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));
                let write_clone = write.clone();

                // Start a ping task 
                let ping_connection_id = connection_id.clone();
                let ping_app_state = app_state.clone();
                let ping_task = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(5));
                    
                    loop {
                        interval.tick().await;
                        
                        if ping_app_state.should_reconnect(&ping_connection_id) {
                            error!("Hbit {}: Reconnection signaled, terminating ping task", ping_connection_id);
                            return;
                        }
                        
                        // Hbit doesn't document a specific ping format, using simple heartbeat
                        let ping_msg = json!({
                            "event": "ping"
                        });
                        
                        info!("Hbit {}: Sending heartbeat ping", ping_connection_id);
                        ping_app_state.update_connection_timestamp(&ping_connection_id);
                        
                        let mut writer = write_clone.lock().await;
                        if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                            error!("Hbit {}: Failed to send ping: {}", ping_connection_id, e);
                            break;
                        }
                        
                        // Check if connection is stale
                        let idle_time = ping_app_state.get_connection_idle_time(&ping_connection_id);
                        if idle_time > STALE_CONNECTION_TIMEOUT as u64 {
                            error!("Hbit {}: Connection stale for {}ms, terminating ping task", 
                                  ping_connection_id, idle_time);
                            break;
                        }
                    }
                    
                    warn!("Hbit {}: Ping task terminated", ping_connection_id);
                });

                // Subscribe to symbols for market depth
                let mut subscription_count = 0;
                
                // Process symbols in smaller batches to avoid overwhelming the server
                let mut remaining_symbols = symbols.clone();
                
                while !remaining_symbols.is_empty() {
                    // Take up to 5 symbols at a time
                    let batch_size = std::cmp::min(5, remaining_symbols.len());
                    let batch: Vec<String> = remaining_symbols.drain(..batch_size).collect();
                    
                    for symbol in &batch {
                        // Convert symbol (e.g. "BTCUSDT") to Hbit pair format (lower-case with underscore)
                        let symbol_lower = symbol.to_lowercase();
                        let pair = if symbol_lower.contains('_') {
                            symbol_lower
                        } else {
                            // For symbols like "BTCUSDT", we need "btc_usdt"
                            let quote_currencies = ["usdt", "usd", "btc", "eth", "usdc"];
                            let mut base = symbol_lower.clone();
                            let mut quote = "usdt"; // Default to USDT
                                    
                            for &curr in &quote_currencies {
                                if symbol_lower.ends_with(curr) && symbol_lower.len() > curr.len() {
                                    base = symbol_lower[..symbol_lower.len() - curr.len()].to_string();
                                    quote = curr;
                                    break;
                                }
                            }
                                    
                            format!("{}_{}",  base, quote)
                        };
                        
                        // Subscribe to 10deep for this symbol
                        let sub_msg = json!({
                            "event": "sub",
                            "topic": format!("{}.5deep", pair)
                        });
                        
                        info!("Hbit {}: Subscribing to {}.5deep", connection_id, pair);
                        
                        let mut writer = write.lock().await;
                        if let Err(e) = writer.send(Message::Text(sub_msg.to_string())).await {
                            error!("Hbit {}: Failed to send subscription for {}: {}", connection_id, pair, e);
                            continue;
                        } else {
                            info!("Hbit {}: Subscription sent for {}", connection_id, pair);
                        }
                        
                        // Add a small delay between subscriptions
                        drop(writer);
                        sleep(Duration::from_millis(10)).await;
                    }
                    
                    subscription_count += 1;
                }

                // Main event loop with timeout
                let mut consecutive_timeouts = 0;
                let mut consecutive_errors = 0;
                
                loop {
                    if app_state.should_reconnect(&connection_id) {
                        error!("Hbit {}: Reconnection signaled, breaking main loop", connection_id);
                        break;
                    }
                    
                    app_state.update_connection_timestamp(&connection_id);
                    
                    match timeout(Duration::from_secs(30), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Increment message counter
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            
                            // Parse message as JSON
                            if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                                // Update connection timestamp
                                app_state.update_connection_timestamp(&connection_id);
                                
                                // Check message type
                                if let Some(type_val) = json_msg.get("type") {
                                    if let Some(type_str) = type_val.as_str() {
                                        // Check if it's a depth update (format: btc_usdt.5deep)
                                        if type_str.contains("deep") {
                                            if let Some(data) = json_msg.get("data") {
                                                // Get symbol from data section
                                                if let Some(symbol_str) = data.get("symbol").and_then(|s| s.as_str()) {
                                                    // Convert from "btc_usdt" to "BTCUSDT"
                                                    let normalized_symbol = symbol_str.replace('_', "").to_uppercase();
                                                    let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "HBIT");
                                                    
                                                    // Get timestamp from message
                                                    let timestamp = json_msg.get("ts")
                                                        .and_then(|t| t.as_i64())
                                                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                                                    
                                                    // Get the scale
                                                    let scale = app_state.price_scales.get(&normalized_symbol)
                                                        .map(|entry| *entry.value())
                                                        .unwrap_or(8); // Default scale
                                                    
                                                    // Process the unique Hbit format where asks and bids are alternating price/quantity
                                                    if let (Some(asks_array), Some(bids_array)) = (data.get("asks").and_then(|a| a.as_array()), 
                                                                                                 data.get("bids").and_then(|b| b.as_array())) {
                                                        // Extract depth data from alternating price/quantity values in the arrays
                                                        let mut depth_asks = Vec::new();
                                                        let mut depth_bids = Vec::new();
                                                        
                                                        // Process asks - format is ["price1", "qty1", "price2", "qty2", ...]
                                                        let mut best_ask = 0.0;
                                                        
                                                        for i in (0..asks_array.len()).step_by(2) {
                                                            if i + 1 < asks_array.len() {
                                                                if let (Some(price_str), Some(qty_str)) = (asks_array[i].as_str(), asks_array[i+1].as_str()) {
                                                                    if let (Ok(price), Ok(qty)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                                                                        if price > 0.0 && qty > 0.0 {
                                                                            depth_asks.push((price, qty));
                                                                            
                                                                            // Update best ask (lowest price)
                                                                            if best_ask == 0.0 || price < best_ask {
                                                                                best_ask = price;
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        
                                                        // Process bids - format is ["price1", "qty1", "price2", "qty2", ...]
                                                        let mut best_bid = 0.0;
                                                        
                                                        for i in (0..bids_array.len()).step_by(2) {
                                                            if i + 1 < bids_array.len() {
                                                                if let (Some(price_str), Some(qty_str)) = (bids_array[i].as_str(), bids_array[i+1].as_str()) {
                                                                    if let (Ok(price), Ok(qty)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                                                                        if price > 0.0 && qty > 0.0 {
                                                                            depth_bids.push((price, qty));
                                                                            
                                                                            // Update best bid (highest price)
                                                                            if price > best_bid {
                                                                                best_bid = price;
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        
                                                        // Ensure asks are sorted by price ascending
                                                        depth_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                                                        
                                                        // Ensure bids are sorted by price descending
                                                        depth_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                                                        
                                                        // Skip if prices are invalid
                                                        if best_ask <= 0.0 || best_bid <= 0.0 {
                                                            debug!("Hbit {}: Invalid prices for {}: ask={}, bid={}", 
                                                                connection_id, normalized_symbol, best_ask, best_bid);
                                                            continue;
                                                        }
                                                        
                                                        info!("Hbit {}: Received valid orderbook update for {}: ask={}, bid={}", 
                                                             connection_id, prefixed_symbol, best_ask, best_bid);
                                                        
                                                        // Update app state with orderbook data
                                                        if let Some(tx) = &app_state.orderbook_queue {
                                                            let update = OrderbookUpdate {
                                                                symbol: prefixed_symbol.clone(),
                                                                best_ask,
                                                                best_bid,
                                                                timestamp,
                                                                scale,
                                                                is_synthetic: false,
                                                                leg1: None,
                                                                leg2: None,
                                                                depth_asks: Some(depth_asks),
                                                                depth_bids: Some(depth_bids),
                                                            };
                                                            
                                                            if let Err(e) = tx.send(update) {
                                                                error!("Hbit {}: Failed to send orderbook update: {}", connection_id, e);
                                                            } else {
                                                                debug!("Hbit {}: Enqueued price update for {}: ask={}, bid={}", 
                                                                     connection_id, prefixed_symbol, best_ask, best_bid);
                                                            }
                                                        } else {
                                                            app_state.price_data.insert(
                                                                prefixed_symbol,
                                                                PriceData {
                                                                    best_ask,
                                                                    best_bid,
                                                                    timestamp,
                                                                    scale,
                                                                    is_synthetic: false,
                                                                    leg1: None,
                                                                    leg2: None,
                                                                    depth_asks: Some(depth_asks),
                                                                    depth_bids: Some(depth_bids),
                                                                },
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        } else if type_str == "pong" {
                                            debug!("Hbit {}: Received pong", connection_id);
                                        }
                                    }
                                }
                            }
                        },
                        Ok(Some(Ok(msg))) => {
                            // Handle other message types (Binary, Ping, Pong, etc.)
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            app_state.update_connection_timestamp(&connection_id);
                            
                            match msg {
                                Message::Binary(data) => {
                                    debug!("Hbit {}: Received binary data ({} bytes)", connection_id, data.len());
                                },
                                Message::Ping(data) => {
                                    debug!("Hbit {}: Received Ping, sending Pong", connection_id);
                                    let mut writer = write.lock().await;
                                    if let Err(e) = writer.send(Message::Pong(data)).await {
                                        error!("Hbit {}: Failed to send Pong: {}", connection_id, e);
                                    }
                                },
                                Message::Pong(_) => {
                                    debug!("Hbit {}: Received Pong", connection_id);
                                },
                                Message::Close(frame) => {
                                    info!("Hbit {}: Received Close frame: {:?}", connection_id, frame);
                                    break;
                                },
                                _ => {
                                    debug!("Hbit {}: Received other message type", connection_id);
                                }
                            }
                        },
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            error!("Hbit {}: WebSocket error: {}", connection_id, e);
                            if consecutive_errors >= 3 {
                                error!("Hbit {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("Hbit {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_timeouts += 1;
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            
                            // Only log a warning if idle time exceeds a threshold
                            if idle_time > 10000 {
                                warn!("Hbit {}: Read timeout - idle for {}ms", connection_id, idle_time);
                                
                                // Try to send a ping to keep the connection alive
                                if consecutive_timeouts == 1 {
                                    let ping_msg = json!({
                                        "event": "ping"
                                    });
                                    
                                    warn!("Hbit {}: Sending emergency ping", connection_id);
                                    let mut writer = write.lock().await;
                                    if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                                        error!("Hbit {}: Failed to send emergency ping: {}", connection_id, e);
                                    }
                                }
                            } else {
                                // For expected short idle periods, just log at debug level
                                debug!("Hbit {}: Short read timeout - idle for {}ms", 
                                      connection_id, idle_time);
                                
                                // Reset consecutive_timeouts since this is normal behavior
                                consecutive_timeouts = 0;
                            }
                            
                            if consecutive_timeouts >= 3 {
                                error!("Hbit {}: Too many consecutive timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }
                    
                    // Check connection staleness
                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("Hbit {}: Connection stale ({}ms), forcing reconnect", connection_id, idle_time);
                        break;
                    }
                    
                    // Yield occasionally to avoid monopolizing the thread
                    if subscription_count % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                
                // Clean up the ping task
                ping_task.abort();
                error!("Hbit {}: Session ended, reconnecting...", connection_id);
            },
            Ok(Err(e)) => {
                error!("Hbit {}: Failed to connect: {}", connection_id, e);
                retry_count += 1;
            },
            Err(_) => {
                error!("Hbit {}: Connection timeout", connection_id);
                retry_count += 1;
            }
        }
        
        // Handle reconnection with exponential backoff
        let delay = f64::min(0.5 * 1.5f64.powi(retry_count as i32), MAX_RECONNECT_DELAY);
        
        info!("Hbit {}: Reconnecting in {:.2} seconds (attempt {}/{})", 
             connection_id, delay, retry_count + 1, max_retries);
        
        app_state.update_connection_timestamp(&connection_id);
        sleep(Duration::from_secs_f64(delay)).await;
        retry_count += 1;
    }
    
    error!("Hbit {}: Failed to maintain connection after {} retries", connection_id, max_retries);
    Ok(())
}