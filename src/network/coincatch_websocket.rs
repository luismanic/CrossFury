// coincatch_websocket.rs - Coin Catch WebSocket handler implementation

use crate::core::*;
use crate::utils::ensure_exchange_prefix;
use futures::{SinkExt, StreamExt};
use log::{info, error, warn, debug};
use serde_json::{json, Value};
use std::time::Duration;
use std::collections::HashSet;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use std::sync::Arc;

// Coin Catch WebSocket URL
const COINCATCH_WS_URL: &str = "wss://ws.coincatch.com/public/v1/stream";

// List of likely supported symbols on Coin Catch
const LIKELY_SUPPORTED_SYMBOLS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT",
    "DOTUSDT", "LTCUSDT", "AVAXUSDT", "LINKUSDT", "ATOMUSDT", "ETCUSDT", "EOSUSDT"
];

// Known unsupported symbols based on error logs
const KNOWN_UNSUPPORTED_SYMBOLS: &[&str] = &[
    "BERAUSDT", "XECUSDT", "FTMUSDT", "MATICUSDT", "BONKUSDT", "SCUSDT", "HYPEUSDT", 
    "OMGUSDT", "STMXUSDT", "SPXUSDT", "HNTUSDT", "DAIUSDT", "AUCTIONUSDT", "RLCUSDT", 
    "CVCUSDT", "WALUSDT", "BALUSDT", "XTZUSDT", "DASHUSDT", "MUBARAKUSDT", "USDTUSDT", 
    "ZECUSDT", "WAVESUSDT", "PARTIUSDT", "AUDIOUSDT", "BROCCOLIUSDT", "ORCAUSDT", 
    "BATUSDT", "CROUSDT", "STPTUSDT", "UMAUSDT", "LENDUSDT", "USDCUSDT", "FUNUSDT", 
    "LAYERUSDT", "ONEUSDT", "LEOUSDT", "YFIUSDT", "PENDLEUSDT", "MIOTAUSDT", "RENUSDT",
    "NEIROUSDT", "OKBUSDT", "JELLYUSDT", "UXLINKUSDT", "CELRUSDT", "XMRUSDT",
];

/// Handler for Coin Catch WebSocket connections
pub async fn coincatch_websocket_handler(
    symbols: Vec<String>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    let connection_id = format!("coincatch-{}", connection_index + 1);
    app_state.update_connection_timestamp(&connection_id);
    app_state.clear_reconnect_signal(&connection_id);

    // Store rejected symbols to avoid repeatedly logging them
    let rejected_symbols: Mutex<HashSet<String>> = Mutex::new(
        KNOWN_UNSUPPORTED_SYMBOLS.iter().map(|s| s.to_string()).collect()
    );
    
    // Track confirmed symbols
    let confirmed_symbols: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    
    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!("Coin Catch {}: Connecting to {}", connection_id, COINCATCH_WS_URL);
        
        // Create request with required headers
        let mut request = COINCATCH_WS_URL.into_client_request()
            .map_err(|e| AppError::WebSocketError(format!("Failed to create request: {}", e)))?;
        
        // Add standard headers
        request.headers_mut().insert(
            "User-Agent", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36".parse().unwrap()
        );

        // Connect with timeout
        match timeout(Duration::from_secs(15), connect_async(request)).await {
            Ok(Ok((ws_stream, _))) => {
                info!("Coin Catch {}: Connection established", connection_id);
                app_state.update_connection_timestamp(&connection_id);
                let (write, mut read) = ws_stream.split();
                let write = Arc::new(Mutex::new(write));
                let write_clone = write.clone();

                // Start a ping task (Coin Catch requires ping every 30 seconds)
                let ping_connection_id = connection_id.clone();
                let ping_app_state = app_state.clone();
                let ping_task = tokio::spawn(async move {
                    // Set ping interval to 10 seconds
                    let mut interval = tokio::time::interval(Duration::from_secs(10));
                    
                    loop {
                        interval.tick().await;
                        
                        if ping_app_state.should_reconnect(&ping_connection_id) {
                            error!("Coin Catch {}: Reconnection signaled, terminating ping task", ping_connection_id);
                            return;
                        }
                        
                        // Send a ping message
                        info!("Coin Catch {}: Sending heartbeat ping", ping_connection_id);
                        ping_app_state.update_connection_timestamp(&ping_connection_id);
                        
                        let mut writer = write_clone.lock().await;
                        if let Err(e) = writer.send(Message::Text("ping".to_string())).await {
                            error!("Coin Catch {}: Failed to send ping: {}", ping_connection_id, e);
                            break;
                        }
                        
                        // Check if connection is stale
                        let idle_time = ping_app_state.get_connection_idle_time(&ping_connection_id);
                        if idle_time > STALE_CONNECTION_TIMEOUT as u64 {
                            error!("Coin Catch {}: Connection stale for {}ms, terminating ping task", 
                                  ping_connection_id, idle_time);
                            break;
                        }
                    }
                    
                    warn!("Coin Catch {}: Ping task terminated", ping_connection_id);
                });

                // Subscribe to symbols that are likely to be supported
                let mut subscription_count = 0;
                
                // Create a filtered list of symbols to try
                let mut symbols_to_try = Vec::new();
                
                // First add known good symbols
                for &common_symbol in LIKELY_SUPPORTED_SYMBOLS {
                    let rejected = {
                        let rejected_guard = rejected_symbols.lock().await;
                        rejected_guard.contains(common_symbol)
                    };
                    
                    let confirmed = {
                        let confirmed_guard = confirmed_symbols.lock().await;
                        confirmed_guard.contains(common_symbol)
                    };
                    
                    if !rejected && !confirmed {
                        symbols_to_try.push(common_symbol.to_string());
                    }
                }
                
                // Then try user symbols that aren't in the blocked list
                for symbol in &symbols {
                    let upper_symbol = symbol.to_uppercase();
                    let rejected = {
                        let rejected_guard = rejected_symbols.lock().await;
                        rejected_guard.contains(&upper_symbol)
                    };
                    
                    let confirmed = {
                        let confirmed_guard = confirmed_symbols.lock().await;
                        confirmed_guard.contains(&upper_symbol)
                    };
                    
                    let is_likely = LIKELY_SUPPORTED_SYMBOLS.contains(&upper_symbol.as_str());
                    
                    if !rejected && !confirmed && !is_likely {
                        symbols_to_try.push(upper_symbol);
                    }
                }
                
                info!("Coin Catch {}: Prepared {} filtered symbols for subscription", 
                     connection_id, symbols_to_try.len());
                
                // Process one symbol at a time
                for symbol in symbols_to_try {
                    // Create subscription message with EXACT format from documentation
                    let sub_msg = json!({
                        "op": "subscribe",
                        "args": [
                            {
                                "instType": "mc",
                                "channel": "books5",
                                "instId": symbol
                            }
                        ]
                    });
                    
                    debug!("Coin Catch {}: Subscribing to {}", connection_id, symbol);
                    
                    let mut writer = write.lock().await;
                    if let Err(e) = writer.send(Message::Text(sub_msg.to_string())).await {
                        error!("Coin Catch {}: Failed to send subscription for {}: {}", 
                              connection_id, symbol, e);
                        continue;
                    }
                    
                    // Wait between subscriptions
                    drop(writer);
                    sleep(Duration::from_millis(5)).await;
                    
                    subscription_count += 1;
                }

                // Main event loop with timeout
                let mut consecutive_timeouts = 0;
                let mut consecutive_errors = 0;
                
                loop {
                    if app_state.should_reconnect(&connection_id) {
                        error!("Coin Catch {}: Reconnection signaled, breaking main loop", connection_id);
                        break;
                    }
                    
                    app_state.update_connection_timestamp(&connection_id);
                    
                    match timeout(Duration::from_secs(10), read.next()).await {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Increment message counter
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            
                            // Handle plain text "pong" response
                            if text == "pong" {
                                debug!("Coin Catch {}: Received pong response", connection_id);
                                app_state.update_connection_timestamp(&connection_id);
                                continue;
                            }
                            
                            // Update connection timestamp on message receipt
                            app_state.update_connection_timestamp(&connection_id);
                            
                            // Parse message as JSON
                            if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                                // Check for subscription confirmation
                                if let Some(event) = json_msg.get("event").and_then(|e| e.as_str()) {
                                    if event == "subscribe" {
                                        if let Some(arg) = json_msg.get("arg") {
                                            let channel = arg.get("channel").and_then(|c| c.as_str()).unwrap_or("");
                                            let inst_id = arg.get("instId").and_then(|i| i.as_str()).unwrap_or("");
                                            info!("Coin Catch {}: Successfully subscribed to {} for {}", 
                                                connection_id, channel, inst_id);
                                                
                                            // Add to confirmed symbols list
                                            let mut confirmed_guard = confirmed_symbols.lock().await;
                                            confirmed_guard.insert(inst_id.to_string());
                                        }
                                        continue; // This is just a confirmation, not actual data
                                    }
                                }
                                
                                // Check for error responses
                                if let Some(code) = json_msg.get("code") {
                                    let code_value = code.as_i64().unwrap_or(0);
                                    let error_msg = json_msg.get("msg").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                                    
                                    // Extract the symbol from the error message for "doesn't exist" errors
                                    if code_value == 30001 && error_msg.contains("doesn't exist") {
                                        if let Some(inst_id_start) = error_msg.find("instId:") {
                                            let symbol_part = &error_msg[inst_id_start..];
                                            if let Some(symbol_end) = symbol_part.find(" doesn't exist") {
                                                let symbol = symbol_part[7..symbol_end].to_string();
                                                
                                                // Add to rejected symbols to avoid future attempts
                                                {
                                                    let mut rejected_guard = rejected_symbols.lock().await;
                                                    if !rejected_guard.contains(&symbol) {
                                                        // Log only the first time we see this symbol rejected
                                                        warn!("Coin Catch {}: Symbol {} doesn't exist, will not try again", 
                                                             connection_id, symbol);
                                                        rejected_guard.insert(symbol);
                                                    }
                                                }
                                                
                                                // Don't log the error further
                                                continue;
                                            }
                                        }
                                    }
                                    
                                    // Log other errors normally
                                    error!("Coin Catch {}: Error response: code={}, msg={}", 
                                         connection_id, code, error_msg);
                                    continue;
                                }
                                
                                // Process order book data
                                if let Some(arg) = json_msg.get("arg") {
                                    if let Some(channel) = arg.get("channel").and_then(|c| c.as_str()) {
                                        if channel.starts_with("books") {
                                            // Extract relevant info from the message
                                            let inst_id = arg.get("instId").and_then(|i| i.as_str()).unwrap_or("");
                                            
                                            // Get normalized symbol
                                            let normalized_symbol = inst_id.to_string();
                                            let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "COINCATCH");
                                            
                                            // Process the order book data
                                            if let Some(data) = json_msg.get("data") {
                                                if let (Some(asks_array), Some(bids_array)) = (
                                                    data.get("asks").and_then(|a| a.as_array()),
                                                    data.get("bids").and_then(|b| b.as_array())
                                                ) {
                                                    // Get timestamp from data or use current time
                                                    let timestamp = data.get("ts")
                                                        .and_then(|t| t.as_str())
                                                        .and_then(|s| s.parse::<i64>().ok())
                                                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                                                    
                                                    // Get scale (default to 8 if not found)
                                                    let scale = app_state.price_scales.get(&normalized_symbol)
                                                        .map(|entry| *entry.value())
                                                        .unwrap_or(8);
                                                    
                                                    // Extract depth data
                                                    let mut depth_asks = Vec::new();
                                                    let mut depth_bids = Vec::new();
                                                    
                                                    // Process asks - format is [price, qty]
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
                                                    
                                                    // Process bids - format is [price, qty]
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
                                                    
                                                    // Ensure asks are sorted by price ascending
                                                    depth_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                                                    
                                                    // Ensure bids are sorted by price descending
                                                    depth_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                                                    
                                                    // Skip if prices are invalid
                                                    if best_ask <= 0.0 || best_bid <= 0.0 {
                                                        debug!("Coin Catch {}: Invalid prices for {}: ask={}, bid={}", 
                                                            connection_id, inst_id, best_ask, best_bid);
                                                        continue;
                                                    }
                                                    
                                                    // Add to confirmed symbols if we haven't already
                                                    {
                                                        let mut confirmed_guard = confirmed_symbols.lock().await;
                                                        if !confirmed_guard.contains(&normalized_symbol) {
                                                            confirmed_guard.insert(normalized_symbol.clone());
                                                            info!("Coin Catch {}: Received data for symbol {}", 
                                                                connection_id, normalized_symbol);
                                                        }
                                                    }
                                                    
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
                                                            error!("Coin Catch {}: Failed to send orderbook update: {}", connection_id, e);
                                                        } else {
                                                            debug!("Coin Catch {}: Enqueued price update for {}: ask={}, bid={}", 
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
                            // Handle other message types
                            app_state.increment_websocket_messages(1);
                            consecutive_timeouts = 0;
                            consecutive_errors = 0;
                            app_state.update_connection_timestamp(&connection_id);
                            
                            match msg {
                                Message::Binary(data) => {
                                    debug!("Coin Catch {}: Received binary data ({} bytes)", connection_id, data.len());
                                },
                                Message::Ping(data) => {
                                    debug!("Coin Catch {}: Received Ping, sending Pong", connection_id);
                                    let mut writer = write.lock().await;
                                    if let Err(e) = writer.send(Message::Pong(data)).await {
                                        error!("Coin Catch {}: Failed to send Pong: {}", connection_id, e);
                                    }
                                },
                                Message::Pong(_) => {
                                    debug!("Coin Catch {}: Received Pong", connection_id);
                                },
                                Message::Close(frame) => {
                                    info!("Coin Catch {}: Received Close frame: {:?}", connection_id, frame);
                                    break;
                                },
                                _ => {
                                    debug!("Coin Catch {}: Received other message type", connection_id);
                                }
                            }
                        },
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            error!("Coin Catch {}: WebSocket error: {}", connection_id, e);
                            if consecutive_errors >= 3 {
                                error!("Coin Catch {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("Coin Catch {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_timeouts += 1;
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            warn!("Coin Catch {}: Read timeout - idle for {}ms", connection_id, idle_time);
                            
                            // Try to send a ping to keep the connection alive
                            if consecutive_timeouts == 1 {
                                warn!("Coin Catch {}: Sending emergency ping", connection_id);
                                let mut writer = write.lock().await;
                                if let Err(e) = writer.send(Message::Text("ping".to_string())).await {
                                    error!("Coin Catch {}: Failed to send emergency ping: {}", connection_id, e);
                                }
                            }
                            
                            if consecutive_timeouts >= 3 {
                                error!("Coin Catch {}: Too many consecutive timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }
                    
                    // Check connection staleness
                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("Coin Catch {}: Connection stale ({}ms), forcing reconnect", connection_id, idle_time);
                        break;
                    }
                    
                    // Yield occasionally to avoid monopolizing the thread
                    if subscription_count % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                
                // Clean up the ping task
                ping_task.abort();
                
                // Log summary of supported symbols
                let confirmed_count = {
                    let confirmed_guard = confirmed_symbols.lock().await;
                    confirmed_guard.len()
                };
                
                info!("Coin Catch {}: Session ended with {} confirmed working symbols", 
                     connection_id, confirmed_count);
                
                break;  // Exit the retry loop if we successfully connected and processed
            },
            Ok(Err(e)) => {
                error!("Coin Catch {}: Failed to connect: {}", connection_id, e);
                retry_count += 1;
            },
            Err(_) => {
                error!("Coin Catch {}: Connection timeout", connection_id);
                retry_count += 1;
            }
        }
        
        // Handle reconnection with exponential backoff
        let delay = f64::min(0.5 * 1.5f64.powi(retry_count as i32), MAX_RECONNECT_DELAY);
        
        info!("Coin Catch {}: Reconnecting in {:.2} seconds (attempt {}/{})", 
             connection_id, delay, retry_count + 1, max_retries);
        
        app_state.update_connection_timestamp(&connection_id);
        sleep(Duration::from_secs_f64(delay)).await;
        retry_count += 1;
    }
    
    Ok(())
}