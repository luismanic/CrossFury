// network/websocket.rs - WebSocket connection handling

use crate::core::*;
use crate::network::message_processor::process_message;
use crate::utils::ensure_exchange_prefix;
use futures::{SinkExt, StreamExt};
use log::{info, error, warn, debug};
use serde_json::json;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};


/// Establish a WebSocket connection for a set of symbols
pub async fn websocket_handler(
    symbols_info: Vec<(String, String)>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    // Generate connection ID from the index
    let connection_id = format!("conn-{}", connection_index + 1);
    
    // Initialize connection timestamp
    app_state.update_connection_timestamp(&connection_id);
    
    // Initialize reconnection signal to false
    app_state.clear_reconnect_signal(&connection_id);
    
    // Log the WebSocket URL for debugging
    info!("Connection {}: Connecting to WebSocket URL: {}", connection_id, WEBSOCKET_URL);
    
    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!(
            "Connection {}: Establishing WebSocket connection for {} symbols",
            connection_id,
            symbols_info.len()
        );

        // Update connection timestamp at connection attempt
        app_state.update_connection_timestamp(&connection_id);
        app_state.clear_reconnect_signal(&connection_id);

        // Attempt to establish the WebSocket connection
        let connection_result = connect_async(WEBSOCKET_URL).await;
        
        match connection_result {
            Ok((ws_stream, _)) => {
                info!("Connection {}: WebSocket connection established", connection_id);
                info!("Connection {}: Starting main WebSocket event loop", connection_id);
                
                // Update connection timestamp after successful connection
                app_state.update_connection_timestamp(&connection_id);
                
                // Split the stream and wrap the writer in a mutex for concurrent access
                let (write, mut read) = ws_stream.split();
                let write = std::sync::Arc::new(Mutex::new(write));
                let write_clone = write.clone();
                
                // Start a ping task
                let ping_connection_id = connection_id.clone();
                let ping_app_state = app_state.clone();

                let ping_task = tokio::spawn(async move {
                    // Log when the ping task starts
                    info!("Connection {}: Ping task started", ping_connection_id);
                    
                    let mut ping_count = 0;
                    // Use 3 second interval for pings (reduced from 5)
                    let mut interval = tokio::time::interval(Duration::from_secs(3));
                    
                    // Send an immediate ping at startup
                    ping_count += 1;
                    let ping_id = ping_app_state.get_next_request_id().await;
                    let ping_msg = json!({
                        "id": ping_id,
                        "method": "server.ping",
                        "params": []
                    });
                    
                    info!("Connection {}: Sending immediate startup ping with ID {}", 
                        ping_connection_id, ping_id);
                    
                    // Update timestamp for ping activity
                    ping_app_state.update_connection_timestamp(&ping_connection_id);
                    
                    // Get a lock on the writer and send the ping
                    let mut writer = write_clone.lock().await;
                    if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                        error!("Connection {}: Failed to send startup ping: {}", ping_connection_id, e);
                        return; // Exit the ping task if initial ping fails
                    }
                    drop(writer); // Explicitly release the lock
                    
                    loop {
                        // Wait for the next interval tick
                        interval.tick().await;
                        ping_count += 1;
                        
                        // Check connection health
                        let idle_time = ping_app_state.get_connection_idle_time(&ping_connection_id);
                        
                        if idle_time > STALE_CONNECTION_TIMEOUT as u64 {
                            warn!("Connection {}: No messages for {}ms (threshold: {}ms)", 
                                ping_connection_id, idle_time, STALE_CONNECTION_TIMEOUT);
                        }
                        
                        // Check if reconnection was signaled
                        if ping_app_state.should_reconnect(&ping_connection_id) {
                            error!("Connection {}: Reconnection signaled, terminating ping task", 
                                  ping_connection_id);
                            return;
                        }
                        
                        // Send ping
                        let ping_id = ping_app_state.get_next_request_id().await;
                        let ping_msg = json!({
                            "id": ping_id,
                            "method": "server.ping",
                            "params": []
                        });
                        
                        // Log every ping
                        info!("Connection {}: Sending heartbeat ping {} with ID {}", 
                            ping_connection_id, ping_count, ping_id);
                        
                        // Update timestamp for ping activity
                        ping_app_state.update_connection_timestamp(&ping_connection_id);
                        
                        // Get a lock on the writer and send the ping
                        let mut writer = write_clone.lock().await;
                        if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                            error!("Connection {}: Failed to send ping {}: {}", ping_connection_id, ping_count, e);
                            break;
                        }
                        drop(writer); // Explicitly release the lock
                    }
    
                    warn!("Connection {}: Ping task terminated after {} pings", ping_connection_id, ping_count);
                });

                // NEW CODE: Subscribe to symbols after connection is established
                info!("Connection {}: Subscribing to {} symbols", connection_id, symbols_info.len());
                for (symbol, _) in &symbols_info {
                    // Create subscription message
                    let sub_id = app_state.get_next_request_id().await;
                    let subscribe_msg = json!({
                        "id": sub_id,
                        "method": "orderbook_p.subscribe",
                        "params": [symbol]
                    });
                    
                    // Log the subscription request
                    info!("Connection {}: Subscribing to orderbook for {}", connection_id, symbol);
                    
                    // Send subscription request
                    let mut writer = write.lock().await;
                    if let Err(e) = writer.send(Message::Text(subscribe_msg.to_string())).await {
                        error!("Connection {}: Failed to send subscription for {}: {}", connection_id, symbol, e);
                    } else {
                        info!("Connection {}: Subscription sent for {}", connection_id, symbol);
                    }
                    // Release the lock before sleeping
                    drop(writer);
                    
                    // Short delay between subscriptions to avoid overwhelming the server
                    sleep(Duration::from_millis(5)).await;
                }
                
                // Main event loop with timeout
                let mut consecutive_timeouts = 0;
                loop {
                    // Check if reconnection was signaled
                    if app_state.should_reconnect(&connection_id) {
                        error!("Connection {}: Reconnection signaled, breaking main event loop", 
                              connection_id);
                        break;
                    }
                    
                    // Update connection timestamp in main loop
                    app_state.update_connection_timestamp(&connection_id);
                    
                    match timeout(Duration::from_secs(10), read.next()).await { // Reduced timeout from 15s to 10s
                        Ok(Some(Ok(Message::Text(text)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            // Reset consecutive timeout counter since we got a message
                            consecutive_timeouts = 0;
                            
                            // NEW CODE: Improved logging for debug purposes
                            info!("Connection {}: Received message: {}", connection_id, text);
                            
                            // Update connection timestamp immediately on message
                            app_state.update_connection_timestamp(&connection_id);
                            
                            // Process message with proper connection ID
                            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Check for orderbook_p updates (most common case for Phemex)
                                if json_value.get("orderbook_p").is_some() && json_value.get("symbol").is_some() {
                                    let symbol = json_value["symbol"].as_str().unwrap_or("").to_string();
                                    
                                    if let Some(scale_entry) = app_state.price_scales.get(&symbol) {
                                        let scale = *scale_entry.value();
                                        let divisor = 10_f64.powi(scale);
                                        
                                        // Use proper JSON array handling
                                        if let Some(asks) = json_value["orderbook_p"].get("asks") {
                                            if let Some(bids) = json_value["orderbook_p"].get("bids") {
                                                if let (Some(asks_array), Some(bids_array)) = (asks.as_array(), bids.as_array()) {
                                                    // Process if either asks or bids is non-empty
                                                    let best_ask = if !asks_array.is_empty() {
                                                        asks_array[0][0]
                                                            .as_str()
                                                            .unwrap_or("0")
                                                            .parse::<f64>()
                                                            .unwrap_or(0.0) / divisor
                                                    } else {
                                                        0.0
                                                    };
                                                    
                                                    let best_bid = if !bids_array.is_empty() {
                                                        bids_array[0][0]
                                                            .as_str()
                                                            .unwrap_or("0")
                                                            .parse::<f64>()
                                                            .unwrap_or(0.0) / divisor
                                                    } else {
                                                        0.0
                                                    };
                                                    
                                                    // Extract full depth data
                                                    let depth_asks = asks_array.iter()
                                                        .filter_map(|level| {
                                                            if let Some(level_array) = level.as_array() {
                                                                if level_array.len() >= 2 {
                                                                    let price = level_array[0].as_str().unwrap_or("0")
                                                                        .parse::<f64>().unwrap_or(0.0) / divisor;
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
                                                                        .parse::<f64>().unwrap_or(0.0) / divisor;
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
                                                    
                                                    if best_ask > 0.0 && best_bid > 0.0 {
                                                        let current_time = chrono::Utc::now().timestamp_millis();
                                                        let prefixed_symbol = ensure_exchange_prefix(&symbol, "PHEMEX");
                                                        
                                                        // Log the price update
                                                        info!("Connection {}: Received valid orderbook update for {}: ask={}, bid={}", 
                                                            connection_id, prefixed_symbol, best_ask, best_bid);
                                                        
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
                                                                error!("Connection {}: Failed to send orderbook update: {}", 
                                                                      connection_id, e);
                                                            } else {
                                                                debug!("Connection {}: Enqueued price update for {}: ask={}, bid={}", 
                                                                     connection_id, symbol, best_ask, best_bid);
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
                                                                    // Just add these two lines:
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
                                } else {
                                    // For other message types, use the standard processor
                                    if let Err(e) = process_message(&app_state, &connection_id, &text).await {
                                        warn!("Connection {}: Error processing message: {}", connection_id, e);
                                        // Continue instead of breaking
                                    }
                                }
                            } else {
                                // If we couldn't parse as JSON, still try to process through standard processor
                                if let Err(e) = process_message(&app_state, &connection_id, &text).await {
                                    warn!("Connection {}: Error processing message: {}", connection_id, e);
                                    // Continue instead of breaking
                                }
                            }
                        },
                        Ok(Some(Ok(Message::Binary(data)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            // Handle binary data messages
                            consecutive_timeouts = 0;
                            debug!("Connection {}: Received binary data message ({} bytes)", 
                                 connection_id, data.len());
                            
                            // Update connection timestamp
                            app_state.update_connection_timestamp(&connection_id);
                        },
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            consecutive_timeouts = 0;
                            info!("Connection {}: Received Ping, sending Pong", connection_id);
                            
                            // Update connection timestamp
                            app_state.update_connection_timestamp(&connection_id);
                            
                            // Respond with Pong
                            let mut writer = write.lock().await;
                            if let Err(e) = writer.send(Message::Pong(data.clone())).await {
                                error!("Connection {}: Failed to send Pong: {}", connection_id, e);
                            }
                        },
                        Ok(Some(Ok(Message::Pong(_)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            consecutive_timeouts = 0;
                            debug!("Connection {}: Received Pong response", connection_id);
                            
                            // Update connection timestamp
                            app_state.update_connection_timestamp(&connection_id);
                        },
                        Ok(Some(Ok(Message::Frame(_)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            consecutive_timeouts = 0;
                            debug!("Connection {}: Received frame message", connection_id);
                        
                            // Update connection timestamp
                            app_state.update_connection_timestamp(&connection_id);
                        },
                        Ok(Some(Ok(Message::Close(frame)))) => {
                            // Add message counting here
                            app_state.increment_websocket_messages(1);
                            
                            info!("Connection {}: Received close frame: {:?}", connection_id, frame);
                            break;
                        },
                        Ok(Some(Err(e))) => {
                            error!("Connection {}: WebSocket error: {}", connection_id, e);
                            if e.to_string().contains("timeout") || e.to_string().contains("timed out") {
                                consecutive_timeouts += 1;
                            }
                            if consecutive_timeouts >= 3 {
                                error!("Connection {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("Connection {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_timeouts += 1;
                            
                            // Check idle time with our new atomic tracking
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            
                            warn!("Connection {}: WebSocket read timeout - idle for {}ms", 
                                connection_id, idle_time);
                            
                            // Send emergency ping
                            let ping_id = app_state.get_next_request_id().await;
                            let ping_msg = json!({
                                "id": ping_id,
                                "method": "server.ping",
                                "params": []
                            });
                            
                            info!("Connection {}: Sending emergency ping with ID {}", connection_id, ping_id);
                            
                            // Update timestamp
                            app_state.update_connection_timestamp(&connection_id);
                            
                            let mut writer = write.lock().await;
                            if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                                error!("Connection {}: Failed to send emergency ping: {}", connection_id, e);
                                break;
                            }
                            
                            // Give a short window to receive a response
                            sleep(Duration::from_millis(500)).await; // Reduced from 1000ms
                            
                            if consecutive_timeouts >= 3 {
                                error!("Connection {}: Too many consecutive timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }
                    
                    // Check if the connection is stale based on our atomic timestamp tracking
                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    
                    // Force reconnect for completely stale connections
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("Connection {}: Connection completely stale ({}ms), forcing reconnect", 
                            connection_id, idle_time);
                        break;
                    }
                } // End of loop
                
                // Abort the ping task
                ping_task.abort();
                
                error!(
                    "Connection {}: WebSocket session ended, reconnecting...",
                    connection_id
                );
            },
            Err(e) => {
                error!("Connection {}: Failed to connect: {}", connection_id, e);
                retry_count += 1;
            }
        }
        
        // Reset connection to healthy before reconnecting
        app_state.clear_reconnect_signal(&connection_id);
        
        // Handle reconnection with exponential backoff
        retry_count += 1;
        
        // Fix: Use f64::min instead of std::cmp::min for floating point values
        let delay = f64::min(
            RECONNECT_DELAY_BASE * 1.5f64.powi(retry_count as i32),
            MAX_RECONNECT_DELAY
        );
        
        info!(
            "Connection {}: Reconnecting in {:.2} seconds (attempt {}/{})",
            connection_id,
            delay,
            retry_count,
            max_retries
        );
        
        // Update timestamp before sleep
        app_state.update_connection_timestamp(&connection_id);
        
        sleep(Duration::from_secs_f64(delay)).await;
    }
    
    error!(
        "Connection {}: Failed to maintain WebSocket connection after {} retries",
        connection_id, max_retries
    );
    
    Ok(())
}