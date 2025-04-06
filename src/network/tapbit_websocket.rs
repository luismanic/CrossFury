// tapbit_websocket.rs - TapBit WebSocket handler implementation with authentication

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
use hmac::{Hmac, Mac};
use sha2::Sha256;
use hex;

// TapBit WebSocket URL
const TAPBIT_WS_URL: &str = "wss://ws-openapi.tapbit.com/stream/ws";

// TapBit API credentials
const TAPBIT_API_KEY: &str = "d792e1ac10aa8dfedadf68c3d0b036a5";
const TAPBIT_API_SECRET: &str = "55a7e6510cf94621abb03c7a340f6f1a";

// Request path for signature generation
const TAPBIT_REQUEST_PATH: &str = "/stream/ws";

/// Generate HMAC-SHA256 signature for TapBit API
fn generate_tapbit_signature(timestamp: &str, method: &str, request_path: &str, body: &str) -> Result<String, AppError> {
    // Concatenate the parameters - this is the format that TapBit requires
    let message = format!("{}{}{}{}", timestamp, method, request_path, body);
    
    info!("Signature input: {}", message);
    
    // Create HMAC-SHA256 instance
    let mut mac = Hmac::<Sha256>::new_from_slice(TAPBIT_API_SECRET.as_bytes())
        .map_err(|e| AppError::Other(format!("Failed to create HMAC: {}", e)))?;
    
    // Update with message
    mac.update(message.as_bytes());
    
    // Finalize and convert to hex
    let result = mac.finalize();
    let signature = hex::encode(result.into_bytes());
    
    Ok(signature)
}

/// Handler for TapBit WebSocket connections
pub async fn tapbit_websocket_handler(
    symbols: Vec<String>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    let connection_id = format!("tapbit-{}", connection_index + 1);
    app_state.update_connection_timestamp(&connection_id);
    app_state.clear_reconnect_signal(&connection_id);

    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!("TapBit {}: Connecting to {}", connection_id, TAPBIT_WS_URL);
        
        // Create request with authentication headers
        let timestamp = format!("{:.3}", chrono::Utc::now().timestamp_millis() as f64 / 1000.0);
        let method = "GET";
        let request_path = TAPBIT_REQUEST_PATH;
        let body = "";

        // Create HMAC-SHA256 signature
        let signature = generate_tapbit_signature(&timestamp, method, request_path, body)?;
        
        info!("TapBit {}: Generated authentication timestamp: {}, signature: {}", 
             connection_id, timestamp, signature);
        
        // Add authentication headers
        let mut request = TAPBIT_WS_URL.into_client_request()
            .map_err(|e| AppError::WebSocketError(format!("Failed to create request: {}", e)))?;

        request.headers_mut().insert(
            "ACCESS-KEY", 
            TAPBIT_API_KEY.parse().unwrap()
        );
        
        request.headers_mut().insert(
            "ACCESS-SIGN", 
            signature.parse().unwrap()
        );
        
        request.headers_mut().insert(
            "ACCESS-TIMESTAMP", 
            timestamp.parse().unwrap()
        );
        
        request.headers_mut().insert(
            "Content-Type", 
            "application/json".parse().unwrap()
        );
        
        // Add user agent header for good measure
        request.headers_mut().insert(
            "User-Agent", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36".parse().unwrap()
        );
        
        // Log the full request headers for debugging
        let headers_debug = request.headers().iter()
            .map(|(name, value)| format!("{}={:?}", name, value))
            .collect::<Vec<String>>()
            .join(", ");
        
        info!("TapBit {}: Connection request headers: {}", connection_id, headers_debug);
        
        // Connect with timeout
        match timeout(Duration::from_secs(15), connect_async(request)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("TapBit {}: Connection established", connection_id);
                app_state.update_connection_timestamp(&connection_id);
                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));

                // Subscribe to symbols for market depth
                let mut subscription_count = 0;
                
                // Process symbols in batches to avoid overwhelming the server
                let mut remaining_symbols = symbols.clone();
                
                while !remaining_symbols.is_empty() {
                    // Take up to 5 symbols at a time to respect rate limits
                    let batch_size = std::cmp::min(5, remaining_symbols.len());
                    let batch: Vec<String> = remaining_symbols.drain(..batch_size).collect();
                    
                    // Convert symbols to TapBit format and create subscription args
                    let mut args = Vec::new();
                    for symbol in &batch {
                        // Format: convert "BTCUSDT" to "BTC-SWAP" for TapBit
                        let tapbit_symbol = convert_to_tapbit_format(symbol);
                        
                        // Topic format: "usdt/orderBook.{instrument_id}.[depth]"
                        // Using depth 5 for efficiency
                        args.push(format!("usdt/orderBook.{}.5", tapbit_symbol));
                    }
                    
                    if !args.is_empty() {
                        // Create subscription message
                        let sub_msg = json!({
                            "op": "subscribe",
                            "args": args
                        });
                        
                        info!("TapBit {}: Subscribing batch {} with {} symbols: {}", 
                            connection_id, subscription_count + 1, args.len(), sub_msg.to_string());
                        
                        let mut writer = write.lock().await;
                        if let Err(e) = writer.send(Message::Text(sub_msg.to_string())).await {
                            error!("TapBit {}: Failed to send subscription: {}", connection_id, e);
                            break;
                        } else {
                            info!("TapBit {}: Subscription sent successfully", connection_id);
                            subscription_count += 1;
                        }
                        
                        // Wait between subscription batches to avoid hitting rate limits
                        drop(writer); // Release lock before sleeping
                        sleep(Duration::from_millis(500)).await;
                    }
                }

                // Main event loop
                let mut consecutive_timeouts = 0;
                let mut consecutive_errors = 0;
                
                loop {
                    if app_state.should_reconnect(&connection_id) {
                        error!("TapBit {}: Reconnection signaled, breaking main loop", connection_id);
                        break;
                    }
                    
                    app_state.update_connection_timestamp(&connection_id);
                    
                    match timeout(Duration::from_secs(10), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Increment message counter
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            
                            // Update timestamp on message receipt
                            app_state.update_connection_timestamp(&connection_id);
                            
                            // Handle ping message (TapBit sends "ping" text frames)
                            if text == "ping" {
                                debug!("TapBit {}: Received ping, sending pong", connection_id);
                                let mut writer = write.lock().await;
                                if let Err(e) = writer.send(Message::Text("pong".to_string())).await {
                                    error!("TapBit {}: Failed to send pong: {}", connection_id, e);
                                }
                                continue;
                            }
                            
                            // Log the received message for debugging
                            debug!("TapBit {}: Received message: {}", connection_id, text);
                            
                            // Process the message as orderbook data
                            if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                                // Check for error response
                                if let Some(code) = json_msg.get("code") {
                                    let error_msg = json_msg.get("msg").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                                    error!("TapBit {}: Error response: code={}, msg={}", 
                                          connection_id, code, error_msg);
                                    continue;
                                }
                                
                                // Process orderbook data
                                if let (Some(topic), Some(data)) = (json_msg.get("topic"), json_msg.get("data")) {
                                    let topic_str = topic.as_str().unwrap_or("");
                                    
                                    // Check if it's an orderbook update
                                    if topic_str.starts_with("usdt/orderBook.") {
                                        // Extract symbol from topic
                                        let parts: Vec<&str> = topic_str.split('.').collect();
                                        if parts.len() >= 2 {
                                            let tapbit_symbol = parts[1];
                                            
                                            // Convert symbol format
                                            let normalized_symbol = convert_from_tapbit_format(tapbit_symbol);
                                            let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "TAPBIT");
                                            
                                            // Process the data array
                                            if let Some(data_array) = data.as_array() {
                                                for update in data_array {
                                                    // Process bids and asks
                                                    if let (Some(bids), Some(asks)) = (update.get("bids"), update.get("asks")) {
                                                        if let (Some(bids_array), Some(asks_array)) = (bids.as_array(), asks.as_array()) {
                                                            // Get timestamp
                                                            let timestamp = update.get("timestamp")
                                                                .and_then(|t| t.as_i64())
                                                                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                                                            
                                                            // Extract best bid and ask
                                                            let mut best_bid = 0.0;
                                                            let mut best_ask = 0.0;
                                                            
                                                            // Extract depth data
                                                            let mut depth_bids = Vec::new();
                                                            let mut depth_asks = Vec::new();
                                                            
                                                            // Process bids (highest to lowest)
                                                            for bid in bids_array {
                                                                if let Some(bid_array) = bid.as_array() {
                                                                    if bid_array.len() >= 2 {
                                                                        if let (Some(price_str), Some(quantity_str)) = 
                                                                              (bid_array[0].as_str(), bid_array[1].as_str()) {
                                                                            if let (Ok(price), Ok(quantity)) = 
                                                                                  (price_str.parse::<f64>(), quantity_str.parse::<f64>()) {
                                                                                if price > 0.0 && quantity > 0.0 {
                                                                                    // Update best bid (first bid is highest)
                                                                                    if best_bid == 0.0 {
                                                                                        best_bid = price;
                                                                                    }
                                                                                    
                                                                                    // Add to depth data
                                                                                    depth_bids.push((price, quantity));
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            
                                                            // Process asks (lowest to highest)
                                                            for ask in asks_array {
                                                                if let Some(ask_array) = ask.as_array() {
                                                                    if ask_array.len() >= 2 {
                                                                        if let (Some(price_str), Some(quantity_str)) = 
                                                                              (ask_array[0].as_str(), ask_array[1].as_str()) {
                                                                            if let (Ok(price), Ok(quantity)) = 
                                                                                  (price_str.parse::<f64>(), quantity_str.parse::<f64>()) {
                                                                                if price > 0.0 && quantity > 0.0 {
                                                                                    // Update best ask (first ask is lowest)
                                                                                    if best_ask == 0.0 {
                                                                                        best_ask = price;
                                                                                    }
                                                                                    
                                                                                    // Add to depth data
                                                                                    depth_asks.push((price, quantity));
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            
                                                            // Check if we have valid data
                                                            if best_bid > 0.0 && best_ask > 0.0 {
                                                                info!("TapBit {}: Valid orderbook update for {}: ask={}, bid={}", 
                                                                     connection_id, prefixed_symbol, best_ask, best_bid);
                                                                
                                                                // Get scale (default to 8 if not found)
                                                                let scale = app_state.price_scales.get(&normalized_symbol)
                                                                    .map(|entry| *entry.value())
                                                                    .unwrap_or(8);
                                                                
                                                                // Update orderbook data
                                                                if let Some(tx) = &app_state.orderbook_queue {
                                                                    let update = OrderbookUpdate {
                                                                        symbol: prefixed_symbol.clone(), // Clone here to prevent ownership issues
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
                                                                        error!("TapBit {}: Failed to send orderbook update: {}", connection_id, e);
                                                                    }
                                                                } else {
                                                                    app_state.price_data.insert(
                                                                        prefixed_symbol.clone(), // Clone here to prevent ownership issues
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
                                    }
                                }
                            }
                        },
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            // Handle protocol-level ping frames
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            app_state.update_connection_timestamp(&connection_id);
                            
                            let mut writer = write.lock().await;
                            if let Err(e) = writer.send(Message::Pong(data)).await {
                                error!("TapBit {}: Failed to send Pong: {}", connection_id, e);
                            }
                        },
                        Ok(Some(Ok(msg))) => {
                            // Handle other message types
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            app_state.update_connection_timestamp(&connection_id);
                            
                            match msg {
                                Message::Binary(data) => {
                                    debug!("TapBit {}: Received binary data ({} bytes)", connection_id, data.len());
                                },
                                Message::Pong(_) => {
                                    debug!("TapBit {}: Received Pong", connection_id);
                                },
                                Message::Close(frame) => {
                                    info!("TapBit {}: Received Close frame: {:?}", connection_id, frame);
                                    break;
                                },
                                _ => {}
                            }
                        },
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            error!("TapBit {}: WebSocket error: {}", connection_id, e);
                            if consecutive_errors >= 3 {
                                error!("TapBit {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("TapBit {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_timeouts += 1;
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            warn!("TapBit {}: Read timeout - idle for {}ms", connection_id, idle_time);
                            
                            // Send pong message to keep connection alive
                            if consecutive_timeouts == 1 {
                                warn!("TapBit {}: Sending emergency pong", connection_id);
                                let mut writer = write.lock().await;
                                if let Err(e) = writer.send(Message::Text("pong".to_string())).await {
                                    error!("TapBit {}: Failed to send pong: {}", connection_id, e);
                                }
                            }
                            
                            if consecutive_timeouts >= 3 {
                                error!("TapBit {}: Too many consecutive timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }
                    
                    // Check connection staleness
                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("TapBit {}: Connection stale ({}ms), forcing reconnect", connection_id, idle_time);
                        break;
                    }
                    
                    // Yield occasionally to avoid monopolizing the thread
                    if subscription_count % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                
                error!("TapBit {}: Session ended, reconnecting...", connection_id);
            },
            Ok(Err(e)) => {
                error!("TapBit {}: Failed to connect: {}", connection_id, e);
                retry_count += 1;
            },
            Err(_) => {
                error!("TapBit {}: Connection timeout", connection_id);
                retry_count += 1;
            }
        }
        
        // Handle reconnection with exponential backoff
        let delay = f64::min(0.5 * 1.5f64.powi(retry_count as i32), MAX_RECONNECT_DELAY);
        
        info!("TapBit {}: Reconnecting in {:.2} seconds (attempt {}/{})", 
             connection_id, delay, retry_count + 1, max_retries);
        
        app_state.update_connection_timestamp(&connection_id);
        sleep(Duration::from_secs_f64(delay)).await;
        retry_count += 1;
    }
    
    error!("TapBit {}: Failed to maintain connection after {} retries", connection_id, max_retries);
    Ok(())
}

/// Convert from standard symbol format to TapBit format
/// Example: "BTCUSDT" -> "BTC-SWAP"
fn convert_to_tapbit_format(symbol: &str) -> String {
    // Handle common conversions
    match symbol.to_uppercase().as_str() {
        "BTCUSDT" => "BTC-SWAP".to_string(),
        "ETHUSDT" => "ETH-SWAP".to_string(),
        "SOLUSDT" => "SOL-SWAP".to_string(),
        "BNBUSDT" => "BNB-SWAP".to_string(),
        "ADAUSDT" => "ADA-SWAP".to_string(),
        "DOGEUSDT" => "DOGE-SWAP".to_string(),
        "XRPUSDT" => "XRP-SWAP".to_string(),
        "LINKUSDT" => "LINK-SWAP".to_string(),
        "AVAXUSDT" => "AVAX-SWAP".to_string(),
        "DOTUSDT" => "DOT-SWAP".to_string(),
        "MATICUSDT" => "MATIC-SWAP".to_string(),
        _ => {
            // For other symbols, extract the base token
            let base_token = if symbol.to_uppercase().ends_with("USDT") {
                &symbol[0..symbol.len() - 4]
            } else {
                symbol
            };
            
            // Format as BASE-SWAP
            format!("{}-SWAP", base_token.to_uppercase())
        }
    }
}

/// Convert from TapBit format to standard symbol format
/// Example: "BTC-SWAP" -> "BTCUSDT"
fn convert_from_tapbit_format(symbol: &str) -> String {
    // Extract base token from format BASE-SWAP
    if let Some(base_token) = symbol.split('-').next() {
        // Format as BASEUSDT
        format!("{}USDT", base_token.to_uppercase())
    } else {
        // Fallback if we can't parse properly
        symbol.to_string()
    }
}