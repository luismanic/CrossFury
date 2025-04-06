// lbank_websocket.rs - LBANK WebSocket connection handler
// Updated to follow LBANK's WebSocket API details

use crate::core::*;
use futures::{SinkExt, StreamExt};
use log::{info, error, warn, debug};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use uuid::Uuid;
use crate::utils::ensure_exchange_prefix;

const LBKEX_WS_URL: &str = "wss://www.lbkex.net/ws/V2/";

/// Handler for LBANK WebSocket connections.
pub async fn lbank_websocket_handler(
    symbols: Vec<String>,
    connection_index: usize,
    app_state: AppState,
) -> Result<(), AppError> {
    let connection_id = format!("lbank-{}", connection_index + 1);
    app_state.update_connection_timestamp(&connection_id);
    app_state.clear_reconnect_signal(&connection_id);

    let mut retry_count = 0;
    let max_retries = 10;

    while retry_count < max_retries {
        info!("LBKEX {}: Connecting to {} (attempt {}/{})", connection_id, LBKEX_WS_URL, retry_count + 1, max_retries);
        let req = LBKEX_WS_URL.into_client_request()
            .map_err(|e| AppError::WebSocketError(format!("Failed to create request: {}", e)))?;
        let request = req;

        match connect_async(request).await {
            Ok((ws_stream, response)) => {
                info!("LBKEX {}: Connection established with status {}", connection_id, response.status());
                app_state.update_connection_timestamp(&connection_id);
                let (write, mut read) = ws_stream.split();
                let write = std::sync::Arc::new(Mutex::new(write));
                let write_clone = write.clone();

                // Start a ping task per LBANK guidelines
                let ping_connection_id = connection_id.clone();
                let ping_app_state = app_state.clone();
                let ping_task = tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(10)).await;
                        let ping_id = Uuid::new_v4().to_string();
                        let ping_msg = json!({
                            "action": "ping",
                            "ping": ping_id
                        });
                        info!("LBKEX {}: Sending ping with id {}", ping_connection_id, ping_id);
                        ping_app_state.update_connection_timestamp(&ping_connection_id);
                        let mut writer = write_clone.lock().await;
                        if let Err(e) = writer.send(Message::Text(ping_msg.to_string())).await {
                            error!("LBKEX {}: Failed to send ping: {}", ping_connection_id, e);
                            break;
                        }
                    }
                });

                // Subscribe to symbols in batches
                for symbol in &symbols {
                    // Convert symbol (e.g. "BTCUSDT") to LBank pair format (lower-case with underscore)
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

                    // MODIFIED: Changed from "request" to "subscribe" action for continuous updates
                    let sub_req = json!({
                        "action": "subscribe",
                        "subscribe": "depth",
                        "depth": "10",
                        "pair": pair
                    });
                    
                    info!("LBKEX {}: Subscribing with: {}", connection_id, sub_req.to_string());
                    let mut writer = write.lock().await;
                    if let Err(e) = writer.send(Message::Text(sub_req.to_string())).await {
                        error!("LBKEX {}: Failed to subscribe for {}: {}", connection_id, symbol, e);
                        continue;
                    } else {
                        info!("LBKEX {}: Subscription sent for {}", connection_id, symbol);
                    }
                    drop(writer);
                    sleep(Duration::from_millis(1)).await;
                }

                // Main event loop
                let mut consecutive_errors = 0;
                loop {
                    if app_state.should_reconnect(&connection_id) {
                        error!("LBKEX {}: Reconnection signaled, breaking event loop", connection_id);
                        break;
                    }
                    app_state.update_connection_timestamp(&connection_id);
                    match timeout(Duration::from_secs(30), read.next()).await {
                        Ok(Some(Ok(msg))) => {
                            // Add this at the beginning:
                            app_state.increment_websocket_messages(1);
                            consecutive_errors = 0;
                            match msg {
                                Message::Text(text) => {
                                    // Count WebSocket message
                                    app_state.increment_websocket_messages(1);
                                    consecutive_errors = 0;
                                    
                                    // NEW CODE: Improved logging for debug purposes
                                    info!("LBKEX {}: Received message: {}", connection_id, text);
                                    
                                    // Process LBank message and extract depth data
                                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                                        // Process ping messages
                                        if let Some(action) = val.get("action").and_then(|a| a.as_str()) {
                                            if action.eq_ignore_ascii_case("ping") {
                                                let ping_id = val.get("ping").and_then(|p| p.as_str()).unwrap_or("");
                                                let pong_msg = json!({
                                                    "action": "pong",
                                                    "pong": ping_id
                                                });
                                                debug!("LBKEX {}: Responding to ping with pong", connection_id);
                                                let mut writer = write.lock().await;
                                                if let Err(e) = writer.send(Message::Text(pong_msg.to_string())).await {
                                                    error!("LBKEX {}: Failed to send pong: {}", connection_id, e);
                                                }
                                                continue;
                                            }
                                        }
                                        
                                        // FIXED: Process depth messages from LBANK's specific format
                                        if let Some(msg_type) = val.get("type").and_then(|t| t.as_str()) {
                                            if msg_type.eq_ignore_ascii_case("depth") {
                                                let pair = val.get("pair").and_then(|p| p.as_str()).unwrap_or("");
                                                
                                                // Access the depth object directly
                                                if let Some(depth_obj) = val.get("depth") {
                                                    // Get asks and bids arrays
                                                    let asks_opt = depth_obj.get("asks").and_then(|a| a.as_array());
                                                    let bids_opt = depth_obj.get("bids").and_then(|b| b.as_array());
                                                    
                                                    // Convert pair (e.g., "btc_usdt") to symbol format (e.g., "BTCUSDT")
                                                    let normalized_symbol = pair.replace('_', "").to_uppercase();
                                                    let prefixed_symbol = ensure_exchange_prefix(&normalized_symbol, "LBANK");
                                                    
                                                    // Get scale from app_state
                                                    let scale = app_state.price_scales.get(&normalized_symbol)
                                                        .map(|entry| *entry.value())
                                                        .unwrap_or(8); // Default to 8
                                                    
                                                    // Parse LBANK's specific format for best ask price
                                                    let best_ask = if let Some(asks) = asks_opt {
                                                        if !asks.is_empty() {
                                                            if let Some(first_ask) = asks.get(0) {
                                                                if let Some(first_ask_array) = first_ask.as_array() {
                                                                    if first_ask_array.len() >= 1 {
                                                                        first_ask_array[0].as_str()
                                                                            .unwrap_or("0")
                                                                            .parse::<f64>()
                                                                            .unwrap_or(0.0)
                                                                    } else {
                                                                        0.0
                                                                    }
                                                                } else {
                                                                    0.0
                                                                }
                                                            } else {
                                                                0.0
                                                            }
                                                        } else {
                                                            0.0
                                                        }
                                                    } else {
                                                        0.0
                                                    };
                                                    
                                                    // Parse LBANK's specific format for best bid price
                                                    let best_bid = if let Some(bids) = bids_opt {
                                                        if !bids.is_empty() {
                                                            if let Some(first_bid) = bids.get(0) {
                                                                if let Some(first_bid_array) = first_bid.as_array() {
                                                                    if first_bid_array.len() >= 1 {
                                                                        first_bid_array[0].as_str()
                                                                            .unwrap_or("0")
                                                                            .parse::<f64>()
                                                                            .unwrap_or(0.0)
                                                                    } else {
                                                                        0.0
                                                                    }
                                                                } else {
                                                                    0.0
                                                                }
                                                            } else {
                                                                0.0
                                                            }
                                                        } else {
                                                            0.0
                                                        }
                                                    } else {
                                                        0.0
                                                    };
                                                    
                                                    // Extract full depth data (also adjusting for LBANK's format)
                                                    let depth_asks = if let Some(asks) = asks_opt {
                                                        asks.iter()
                                                            .filter_map(|level| {
                                                                level.as_array().and_then(|arr| {
                                                                    if arr.len() >= 2 {
                                                                        let price_str = arr[0].as_str()?;
                                                                        let qty_str = arr[1].as_str()?;
                                                                        
                                                                        let price = price_str.parse::<f64>().ok()?;
                                                                        let qty = qty_str.parse::<f64>().ok()?;
                                                                        
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
                                                            .collect::<Vec<(f64, f64)>>()
                                                    } else {
                                                        Vec::new()
                                                    };
                                                    
                                                    let depth_bids = if let Some(bids) = bids_opt {
                                                        bids.iter()
                                                            .filter_map(|level| {
                                                                level.as_array().and_then(|arr| {
                                                                    if arr.len() >= 2 {
                                                                        let price_str = arr[0].as_str()?;
                                                                        let qty_str = arr[1].as_str()?;
                                                                        
                                                                        let price = price_str.parse::<f64>().ok()?;
                                                                        let qty = qty_str.parse::<f64>().ok()?;
                                                                        
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
                                                            .collect::<Vec<(f64, f64)>>()
                                                    } else {
                                                        Vec::new()
                                                    };
                                                    
                                                    if best_ask > 0.0 && best_bid > 0.0 {
                                                        let current_time = chrono::Utc::now().timestamp_millis();
                                                        
                                                        // Log successful price update
                                                        info!("LBKEX {}: Received valid orderbook update for {}: ask={}, bid={}", 
                                                            connection_id, prefixed_symbol, best_ask, best_bid);
                                                        
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
                                                                error!("LBKEX {}: Failed to send orderbook update: {}", connection_id, e);
                                                            } else {
                                                                debug!("LBKEX {}: Orderbook updated for {}: bid={}, ask={}", 
                                                                    connection_id, pair, best_bid, best_ask);
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
                                                        }
                                                    } else {
                                                        warn!("LBKEX {}: Invalid prices for {}: ask={}, bid={}", 
                                                             connection_id, pair, best_ask, best_bid);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                Message::Ping(data) => {
                                    info!("LBKEX {}: Received Ping, sending Pong", connection_id);
                                    app_state.update_connection_timestamp(&connection_id);
                                    app_state.increment_websocket_messages(1);
                                    let mut writer = write.lock().await;
                                    if let Err(e) = writer.send(Message::Pong(data)).await {
                                        error!("LBKEX {}: Failed to send Pong: {}", connection_id, e);
                                    }
                                },
                                Message::Pong(_) => {
                                    debug!("LBKEX {}: Received Pong", connection_id);
                                    app_state.update_connection_timestamp(&connection_id);
                                    app_state.increment_websocket_messages(1);
                                },
                                Message::Binary(_) => {
                                    app_state.update_connection_timestamp(&connection_id);
                                    app_state.increment_websocket_messages(1);
                                },
                                Message::Close(frame) => {
                                    info!("LBKEX {}: Received close frame: {:?}", connection_id, frame);
                                    break;
                                },
                                _ => {}
                            }
                        },
                        Ok(Some(Err(e))) => {
                            consecutive_errors += 1;
                            error!("LBKEX {}: WebSocket error: {}", connection_id, e);
                            if consecutive_errors >= 3 {
                                error!("LBKEX {}: Too many consecutive errors, reconnecting", connection_id);
                                break;
                            }
                        },
                        Ok(None) => {
                            info!("LBKEX {}: WebSocket stream ended", connection_id);
                            break;
                        },
                        Err(_) => {
                            consecutive_errors += 1;
                            let idle_time = app_state.get_connection_idle_time(&connection_id);
                            warn!("LBKEX {}: Read timeout - idle for {}ms", connection_id, idle_time);
                            if consecutive_errors >= 3 {
                                error!("LBKEX {}: Too many timeouts, reconnecting", connection_id);
                                break;
                            }
                        }
                    }

                    let idle_time = app_state.get_connection_idle_time(&connection_id);
                    if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                        error!("LBKEX {}: Connection stale ({}ms), forcing reconnect", connection_id, idle_time);
                        break;
                    }
                }

                ping_task.abort();
                error!("LBKEX {}: WebSocket session ended, reconnecting...", connection_id);
            },
            Err(e) => {
                error!("LBKEX {}: Failed to connect: {}", connection_id, e);
            }
        }

        retry_count += 1;
        let delay = f64::min(0.3 * 1.5f64.powi(retry_count as i32), 3.0);
        info!("LBKEX {}: Reconnecting in {:.2} seconds (attempt {}/{})", connection_id, delay, retry_count, max_retries);
        app_state.update_connection_timestamp(&connection_id);
        sleep(Duration::from_secs_f64(delay)).await;
    }

    error!("LBKEX {}: Failed to maintain connection after {} retries", connection_id, max_retries);
    Ok(())
}