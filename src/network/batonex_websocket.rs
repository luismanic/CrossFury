// batonex_websocket.rs - Batonex WebSocket handler implementation

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

// Batonex WebSocket URL
const BATONEX_WS_URL: &str = "wss://wsapi.batonex.com/openapi/quote/ws/v2";

/// Handler for Batonex WebSocket connections
pub async fn batonex_websocket_handler(
    symbols: Vec<String>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    let connection_id = format!("batonex-{}", connection_index + 1);
    app_state.update_connection_timestamp(&connection_id);
    app_state.clear_reconnect_signal(&connection_id);

    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!("Batonex {}: Connecting to {}", connection_id, BATONEX_WS_URL);
        
        // Create request with required headers
        let mut request = BATONEX_WS_URL.into_client_request()
            .map_err(|e| AppError::WebSocketError(format!("Failed to create request: {}", e)))?;
        
        // Add standard headers
        request.headers_mut().insert(
            "User-Agent", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36".parse().unwrap()
        );

        // Connect with timeout
        match timeout(Duration::from_secs(15), connect_async(request)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Batonex {}: Connection established", connection_id);
                app_state.update_connection_timestamp(&connection_id);
                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));
                
                // Add a dedicated ping task
                let ping_connection_id = connection_id.clone();
                let ping_app_state = app_state.clone();
                let ping_write_clone = write.clone();
                let ping_task = tokio::spawn(async move {
                    // Set a shorter ping interval - reduce from default to improve stability
                    let mut interval = tokio::time::interval(Duration::from_secs(5)); // Shorter interval (5s)
                    
                    loop {
                        interval.tick().await;
                        
                        if ping_app_state.should_reconnect(&ping_connection_id) {
                            error!("Batonex {}: Reconnection signaled, terminating ping task", ping_connection_id);
                            return;
                        }
                        
                        // Send a ping message
                        let ping_msg = json!({
                            "ping": chrono::Utc::now().timestamp_millis()
                        });
                        
                        info!("Batonex {}: Sending heartbeat ping", ping_connection_id);
                        ping_app_state.update_connection_timestamp(&ping_connection_id);
                        
                        let mut writer = ping_write_clone.lock().await;
                        if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                            error!("Batonex {}: Failed to send ping: {}", ping_connection_id, e);
                            break;
                        }
                    }
                    
                    warn!("Batonex {}: Ping task terminated", ping_connection_id);
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
                        // Convert symbol for Batonex format (typically BTCUSDT)
                        let symbol_upper = symbol.to_uppercase();
                        
                        // Create subscription message
                        let sub_msg = json!({
                            "topic": "depth",
                            "event": "sub",
                            "params": {
                                "binary": false,
                                "symbol": symbol_upper
                            }
                        });
                        
                        info!("Batonex {}: Subscribing to depth for {}", connection_id, symbol_upper);
                        
                        let mut writer = write.lock().await;
                        if let Err(e) = writer.send(Message::Text(sub_msg.to_string())).await {
                            error!("Batonex {}: Failed to send subscription for {}: {}", connection_id, symbol_upper, e);
                            continue;
                        } else {
                            info!("Batonex {}: Subscription sent for {}", connection_id, symbol_upper);
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
                        error!("Batonex {}: Reconnection signaled, breaking main loop", connection_id);
                        break;
                    }
                    
                    app_state.update_connection_timestamp(&connection_id);
                    
                    // Reduced timeout from 30 seconds to 10 seconds
                    match timeout(Duration::from_secs(10), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Increment message counter
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            
                            // Parse message as JSON
                            if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                                // Update connection timestamp
                                app_state.update_connection_timestamp(&connection_id);
                                
                                // Check if it's a depth update
                                if let Some(topic) = json_msg.get("topic").and_then(|t| t.as_str()) {
                                    if topic == "depth" {
                                        if let Some(data) = json_msg.get("data") {
                                            // Get symbol from data section
                                            if let Some(symbol_str) = data.get("s").and_then(|s| s.as_str()) {
                                                // Use the symbol directly (it should be in the format we want)
                                                let symbol = symbol_str.to_uppercase();
                                                let prefixed_symbol = ensure_exchange_prefix(&symbol, "BATONEX");
                                                
                                                // Get timestamp from data
                                                let timestamp = data.get("t")
                                                    .and_then(|t| t.as_i64())
                                                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                                                
                                                // Get the scale (default to 8 if not found)
                                                let scale = app_state.price_scales.get(&symbol)
                                                    .map(|entry| *entry.value())
                                                    .unwrap_or(8);
                                                
                                                // Process bids and asks arrays
                                                if let (Some(bids_array), Some(asks_array)) = (
                                                    data.get("b").and_then(|b| b.as_array()),
                                                    data.get("a").and_then(|a| a.as_array())
                                                ) {
                                                    // Extract depth data
                                                    let mut depth_bids = Vec::new();
                                                    let mut depth_asks = Vec::new();
                                                    
                                                    // Process bids - format is [["price1", "qty1"], ["price2", "qty2"], ...]
                                                    let mut best_bid = 0.0;
                                                    
                                                    for bid in bids_array {
                                                        if let Some(bid_array) = bid.as_array() {
                                                            if bid_array.len() >= 2 {
                                                                if let (Some(price_str), Some(qty_str)) = (
                                                                    bid_array[0].as_str(),
                                                                    bid_array[1].as_str()
                                                                ) {
                                                                    if let (Ok(price), Ok(qty)) = (
                                                                        price_str.parse::<f64>(),
                                                                        qty_str.parse::<f64>()
                                                                    ) {
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
                                                    }
                                                    
                                                    // Process asks - format is [["price1", "qty1"], ["price2", "qty2"], ...]
                                                    let mut best_ask = 0.0;
                                                    
                                                    for ask in asks_array {
                                                        if let Some(ask_array) = ask.as_array() {
                                                            if ask_array.len() >= 2 {
                                                                if let (Some(price_str), Some(qty_str)) = (
                                                                    ask_array[0].as_str(),
                                                                    ask_array[1].as_str()
                                                                ) {
                                                                    if let (Ok(price), Ok(qty)) = (
                                                                        price_str.parse::<f64>(),
                                                                        qty_str.parse::<f64>()
                                                                    ) {
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
                                                    }
                                                    
                                                    // Ensure bids are sorted by price descending
                                                    depth_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                                                    
                                                    // Ensure asks are sorted by price ascending
                                                    depth_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                                                    
                                                    // Skip if prices are invalid
                                                    if best_ask <= 0.0 || best_bid <= 0.0 {
                                                        debug!("Batonex {}: Invalid prices for {}: ask={}, bid={}", 
                                                            connection_id, symbol, best_ask, best_bid);
                                                        continue;
                                                    }
                                                    
                                                    info!("Batonex {}: Received valid orderbook update for {}: ask={}, bid={}", 
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
                                                            error!("Batonex {}: Failed to send orderbook update: {}", connection_id, e);
                                                        } else {
                                                            debug!("Batonex {}: Enqueued price update for {}: ask={}, bid={}", 
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
                                    debug!("Batonex {}: Received binary data ({} bytes)", connection_id, data.len());
                                },
                                Message::Ping(data) => {
                                    debug!("Batonex {}: Received Ping, sending Pong", connection_id);
                                    let mut writer = write.lock().await;
                                    if let Err(e) = writer.send(Message::Pong(data)).await {
                                        error!("Batonex {}: Failed to send Pong: {}", connection_id, e);
                                    }
                                },
                                Message::Pong(_) => {
                                    debug!("Batonex {}: Received Pong", connection_id);
                                },
                                Message::Close(frame) => {
                                    info!("Batonex {}: Received Close frame: {:?}", connection_id, frame);
                                    break;
                                },
                                _ => {
                                    debug!("Batonex {}: Received other message type", connection_id);
                                }
                            }
                        },
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            error!("Batonex {}: WebSocket error: {}", connection_id, e);
                            if consecutive_errors >= 3 {
                                error!("Batonex {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("Batonex {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_timeouts += 1;
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            warn!("Batonex {}: Read timeout - idle for {}ms", connection_id, idle_time);
                            
                            // Try to send a ping to keep the connection alive
                            if consecutive_timeouts == 1 {
                                // Send a ping message
                                warn!("Batonex {}: Sending emergency ping", connection_id);
                                let ping_msg = json!({
                                    "ping": chrono::Utc::now().timestamp_millis()
                                });
                                
                                let mut writer = write.lock().await;
                                if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                                    error!("Batonex {}: Failed to send emergency ping: {}", connection_id, e);
                                }
                            }
                            
                            // Reduced from 3 to 2
                            if consecutive_timeouts >= 2 {
                                error!("Batonex {}: Too many consecutive timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }
                    
                    // Check connection staleness
                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("Batonex {}: Connection stale ({}ms), forcing reconnect", connection_id, idle_time);
                        break;
                    }
                    
                    // Yield occasionally to avoid monopolizing the thread
                    if subscription_count % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                
                // Clean up the ping task before reconnecting
                ping_task.abort();
                error!("Batonex {}: Session ended, reconnecting...", connection_id);
            },
            Ok(Err(e)) => {
                error!("Batonex {}: Failed to connect: {}", connection_id, e);
                retry_count += 1;
            },
            Err(_) => {
                error!("Batonex {}: Connection timeout", connection_id);
                retry_count += 1;
            }
        }
        
        // Handle reconnection with exponential backoff
        let delay = f64::min(0.5 * 1.5f64.powi(retry_count as i32), MAX_RECONNECT_DELAY);
        
        info!("Batonex {}: Reconnecting in {:.2} seconds (attempt {}/{})", 
             connection_id, delay, retry_count + 1, max_retries);
        
        app_state.update_connection_timestamp(&connection_id);
        sleep(Duration::from_secs_f64(delay)).await;
        retry_count += 1;
    }
    
    error!("Batonex {}: Failed to maintain connection after {} retries", connection_id, max_retries);
    Ok(())
}