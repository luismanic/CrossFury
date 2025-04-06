// utils/json_parser.rs - SIMD-accelerated JSON parsing utilities

use simd_json::{self, BorrowedValue};
use std::sync::OnceLock;
use log::info;

// Global flag for SIMD initialization
static SIMD_INITIALIZED: OnceLock<bool> = OnceLock::new();

/// Initialize SIMD JSON parser features
pub fn init_simd_json() -> bool {
    if let Some(initialized) = SIMD_INITIALIZED.get() {
        return *initialized;
    }
    
    // Check for SIMD capability - we want at least SSE4.2
    let has_simd = std::is_x86_feature_detected!("sse4.2");
    
    SIMD_INITIALIZED.set(has_simd).unwrap();
    
    if has_simd {
        info!("SIMD JSON parsing enabled with SSE4.2 support");
    } else {
        info!("SIMD JSON parsing not available - using standard parser");
    }
    
    has_simd
}

/// Parse JSON with SIMD acceleration for maximum performance
#[inline(always)]
pub fn parse_json(data: &mut [u8]) -> Result<BorrowedValue, simd_json::Error> {
    if SIMD_INITIALIZED.get().copied().unwrap_or(false) {
        simd_json::from_slice(data)
    } else {
        // Initialize if not done yet
        if init_simd_json() {
            simd_json::from_slice(data)
        } else {
            // Use standard parsing internally
            let value = serde_json::from_slice::<serde_json::Value>(data)?;
            let json_str = serde_json::to_string(&value)?;
            let mut json_bytes = json_str.into_bytes();
            simd_json::from_slice(&mut json_bytes)
        }
    }
}

/// Optimization for orderbook parsing - pre-allocate and avoid string creation
pub struct FastOrderbookParser {
    asks_buffer: Vec<(f64, f64)>,
    bids_buffer: Vec<(f64, f64)>,
    depth_capacity: usize,
}

impl FastOrderbookParser {
    pub fn new(depth_capacity: usize) -> Self {
        Self {
            asks_buffer: Vec::with_capacity(depth_capacity),
            bids_buffer: Vec::with_capacity(depth_capacity),
            depth_capacity,
        }
    }
    
    /// Parse orderbook with pre-allocated buffers for zero allocation in hot path
    pub fn parse_orderbook<'a>(
        &'a mut self, 
        data: &'a serde_json::Value, 
        asks_key: &str, 
        bids_key: &str,
        scale: i32
    ) -> (Option<f64>, Option<f64>, &[(f64, f64)], &[(f64, f64)]) {
        // Clear buffers but maintain capacity
        self.asks_buffer.clear();
        self.bids_buffer.clear();
        
        let divisor = 10_f64.powi(scale);
        let mut best_ask = None;
        let mut best_bid = None;
        
        // Parse asks with zero allocations
        if let Some(asks) = data.get(asks_key).and_then(|v| v.as_array()) {
            for level in asks {
                if let Some(level_array) = level.as_array() {
                    if level_array.len() >= 2 {
                        let price_string = level_array[0].as_f64().map(|f| f.to_string());
                        let qty_string = level_array[1].as_f64().map(|f| f.to_string());
                        if let (Some(price_str), Some(qty_str)) = (
                            level_array[0].as_str().or_else(|| price_string.as_ref().map(|s| s.as_str())),
                            level_array[1].as_str().or_else(|| qty_string.as_ref().map(|s| s.as_str()))
                        ) {
                            if let (Ok(price), Ok(qty)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                                if price > 0.0 && qty > 0.0 {
                                    let scaled_price = price / divisor;
                                    self.asks_buffer.push((scaled_price, qty));
                                    
                                    // Update best ask (lowest price)
                                    if best_ask.is_none() || scaled_price < best_ask.unwrap() {
                                        best_ask = Some(scaled_price);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Parse bids with zero allocations
        if let Some(bids) = data.get(bids_key).and_then(|v| v.as_array()) {
            for level in bids {
                if let Some(level_array) = level.as_array() {
                    if level_array.len() >= 2 {
                        if let (Some(price_str), Some(qty_str)) = (
                            level_array[0].as_str().or_else(|| level_array[0].as_f64().map(|f| f.to_string().as_str())), 
                            level_array[1].as_str().or_else(|| level_array[1].as_f64().map(|f| f.to_string().as_str()))
                        ) {
                            if let (Ok(price), Ok(qty)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                                if price > 0.0 && qty > 0.0 {
                                    let scaled_price = price / divisor;
                                    self.bids_buffer.push((scaled_price, qty));
                                    
                                    // Update best bid (highest price)
                                    if best_bid.is_none() || scaled_price > best_bid.unwrap() {
                                        best_bid = Some(scaled_price);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // Sort asks by price ascending
        self.asks_buffer.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Sort bids by price descending
        self.bids_buffer.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Return references to avoid copies
        (best_ask, best_bid, &self.asks_buffer, &self.bids_buffer)
    }
}