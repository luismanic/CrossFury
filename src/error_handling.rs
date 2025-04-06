// error_handling.rs - Enhanced error handling system

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, Duration};
use dashmap::DashMap;
use log::{info, error, warn};
use crate::core::AppError;
use crate::exchange_types::Exchange;
use crate::config::get_config;

/// Detailed error categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    // Network errors
    ConnectionFailure,
    Timeout,
    SocketClosed,
    
    // Protocol errors
    MessageParsing,
    InvalidResponse,
    Authentication,
    RateLimitExceeded,
    
    // Exchange errors
    ExchangeRejection,
    SymbolInvalid,
    ServiceUnavailable,
    
    // Internal errors
    InternalFailure,
    
    // Unknown/other
    Unknown,
}

impl From<&AppError> for ErrorCategory {
    fn from(error: &AppError) -> Self {
        match error {
            AppError::WebSocketError(msg) => {
                if msg.contains("timeout") || msg.contains("timed out") {
                    ErrorCategory::Timeout
                } else if msg.contains("connection") || msg.contains("connect") {
                    ErrorCategory::ConnectionFailure
                } else if msg.contains("closed") {
                    ErrorCategory::SocketClosed
                } else {
                    ErrorCategory::Unknown
                }
            },
            AppError::RequestError(req_err) => {
                if req_err.is_timeout() {
                    ErrorCategory::Timeout
                } else if req_err.is_connect() {
                    ErrorCategory::ConnectionFailure
                } else {
                    ErrorCategory::Unknown
                }
            },
            AppError::SerializationError(_) => ErrorCategory::MessageParsing,
            AppError::IoError(_) => ErrorCategory::InternalFailure,
            AppError::CsvError(_) => ErrorCategory::InternalFailure,
            AppError::MissingPriceData(_) => ErrorCategory::SymbolInvalid,
            AppError::TimeoutError => ErrorCategory::Timeout,
            AppError::ConnectionError(_) => ErrorCategory::ConnectionFailure,
            AppError::Other(msg) => {
                if msg.contains("rate limit") || msg.contains("too many requests") {
                    ErrorCategory::RateLimitExceeded
                } else if msg.contains("auth") {
                    ErrorCategory::Authentication
                } else if msg.contains("rejected") || msg.contains("invalid") {
                    ErrorCategory::ExchangeRejection
                } else if msg.contains("symbol") {
                    ErrorCategory::SymbolInvalid
                } else if msg.contains("unavailable") || msg.contains("maintenance") {
                    ErrorCategory::ServiceUnavailable
                } else {
                    ErrorCategory::Unknown
                }
            },
        }
    }
}

/// Exchange-specific error tracking and circuit breaker
pub struct ErrorTracker {
    // Track errors by exchange and category
    error_counts: DashMap<(Exchange, ErrorCategory), AtomicUsize>,
    
    // Track when errors started for each exchange
    error_start_times: DashMap<Exchange, Instant>,
    
    // Track circuit breaker status
    circuit_breakers: DashMap<Exchange, CircuitBreakerStatus>,
    
    // Track error rate per connection
    connection_errors: DashMap<String, Vec<(Instant, ErrorCategory)>>,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerStatus {
    pub is_open: bool,
    pub opened_at: Instant,
    pub reopening_at: Instant,
    pub error_threshold_crossed: bool,
    pub trigger_category: ErrorCategory,
}

impl ErrorTracker {
    pub fn new() -> Self {
        Self {
            error_counts: DashMap::new(),
            error_start_times: DashMap::new(),
            circuit_breakers: DashMap::new(),
            connection_errors: DashMap::new(),
        }
    }
    
    /// Record an error and potentially trigger circuit breaker
    pub fn record_error(
        &self,
        exchange: Exchange,
        category: ErrorCategory,
        connection_id: Option<&str>,
        error: &AppError,
    ) {
        // Update error count
        let counter = self.error_counts
            .entry((exchange, category))
            .or_insert_with(|| AtomicUsize::new(0));
            
        counter.fetch_add(1, Ordering::SeqCst);
        
        // Record error start time if first error
        self.error_start_times
            .entry(exchange)
            .or_insert_with(|| Instant::now());
            
        // Record connection-specific error if connection_id provided
        if let Some(conn_id) = connection_id {
            let mut errors = self.connection_errors
                .entry(conn_id.to_string())
                .or_insert_with(Vec::new);
                
            // Keep last 10 errors per connection
            if errors.len() >= 10 {
                errors.remove(0);
            }
            
            errors.push((Instant::now(), category));
        }
        
        // Check circuit breaker conditions
        let should_trigger = self.should_trigger_circuit_breaker(exchange, category);
        
        if should_trigger && get_config().features.enable_circuit_breakers {
            // Open circuit breaker
            let backoff_time = self.calculate_backoff_time(exchange, category);
            
            self.circuit_breakers.insert(exchange, CircuitBreakerStatus {
                is_open: true,
                opened_at: Instant::now(),
                reopening_at: Instant::now() + backoff_time,
                error_threshold_crossed: true,
                trigger_category: category,
            });
            
            error!("Circuit breaker OPENED for {:?} due to too many {:?} errors. Will reopen in {:?}",
                exchange, category, backoff_time);
        }
    }
    
    /// Check if circuit breaker should be triggered based on error patterns
    fn should_trigger_circuit_breaker(&self, exchange: Exchange, category: ErrorCategory) -> bool {
        // Already open
        if let Some(breaker) = self.circuit_breakers.get(&exchange) {
            if breaker.is_open {
                return false;
            }
        }
        
        // Get error count
        let count = self.error_counts
            .get(&(exchange, category))
            .map(|counter| counter.load(Ordering::SeqCst))
            .unwrap_or(0);
            
        // Get error window
        let window = if let Some(start_time) = self.error_start_times.get(&exchange) {
            start_time.elapsed().as_secs()
        } else {
            0
        };
        
        // Different thresholds for different error categories
        match category {
            ErrorCategory::ConnectionFailure | ErrorCategory::Timeout => {
                // More than 5 connection failures in 60 seconds
                count >= 5 && window <= 60
            },
            ErrorCategory::RateLimitExceeded => {
                // Any rate limit exceeded
                count >= 1
            },
            ErrorCategory::Authentication => {
                // More than 2 auth failures
                count >= 2
            },
            ErrorCategory::ServiceUnavailable => {
                // Any service unavailable error
                count >= 1
            },
            _ => {
                // For other errors, need more occurrences
                count >= 10 && window <= 120
            }
        }
    }
    
    /// Calculate backoff time based on error category and history
    fn calculate_backoff_time(&self, exchange: Exchange, category: ErrorCategory) -> Duration {
        match category {
            ErrorCategory::RateLimitExceeded => {
                // Rate limits require longer backoff
                Duration::from_secs(300) // 5 minutes
            },
            ErrorCategory::ServiceUnavailable => {
                // Service issues require moderate backoff
                Duration::from_secs(180) // 3 minutes
            },
            ErrorCategory::Authentication => {
                // Auth issues require short backoff (could be temporary)
                Duration::from_secs(60) // 1 minute
            },
            ErrorCategory::ConnectionFailure | ErrorCategory::Timeout => {
                // Network issues use adaptive backoff
                let consecutive_failures = self.count_consecutive_errors(exchange, category);
                let base_time = 30; // 30 seconds base
                let max_time = 300; // 5 minutes max
                
                let backoff_time = base_time * consecutive_failures.min(10);
                Duration::from_secs(backoff_time.min(max_time))
            },
            _ => {
                // Default backoff
                Duration::from_secs(60) // 1 minute
            }
        }
    }
    
    /// Count consecutive errors of the same category
    fn count_consecutive_errors(&self, exchange: Exchange, category: ErrorCategory) -> u64 {
        let mut count = 0;
        
        // Look through all connections for this exchange
        for entry in self.connection_errors.iter() {
            let connection_id = entry.key();
            
            // Only consider connections for this exchange
            if !connection_id.starts_with(&exchange.to_string().to_lowercase()) {
                continue;
            }
            
            let errors = entry.value();
            
            // Check the last 5 errors
            for i in (0..errors.len().min(5)).rev() {
                if errors[i].1 == category {
                    count += 1;
                } else {
                    break; // Stop at first different error
                }
            }
        }
        
        count
    }
    
    /// Check if a circuit breaker is open for an exchange
    pub fn is_circuit_open(&self, exchange: Exchange) -> bool {
        if let Some(breaker) = self.circuit_breakers.get(&exchange) {
            if breaker.is_open {
                // Check if it's time to close the breaker
                if Instant::now() >= breaker.reopening_at {
                    // Close the breaker
                    self.close_circuit_breaker(exchange);
                    return false;
                }
                return true;
            }
        }
        
        false
    }
    
    /// Close a circuit breaker
    fn close_circuit_breaker(&self, exchange: Exchange) {
        if let Some(mut breaker) = self.circuit_breakers.get_mut(&exchange) {
            if breaker.is_open {
                breaker.is_open = false;
                info!("Circuit breaker CLOSED for {:?}. Normal operations resuming.", exchange);
                
                // Reset error counts for this exchange
                for entry in self.error_counts.iter_mut() {
                    let (ex, _) = entry.key();
                    if *ex == exchange {
                        entry.store(0, Ordering::SeqCst);
                    }
                }
                
                // Reset error start time
                self.error_start_times.remove(&exchange);
            }
        }
    }
    
    /// Get recommended reconnection delay based on error patterns
    pub fn get_recommended_reconnect_delay(
        &self,
        exchange: Exchange,
        connection_id: &str,
        retry_count: usize,
    ) -> Duration {
        // If circuit breaker is open, use longer delay
        if self.is_circuit_open(exchange) {
            if let Some(breaker) = self.circuit_breakers.get(&exchange) {
                let remaining = breaker.reopening_at.duration_since(Instant::now());
                
                // Return remaining time until circuit reopens
                if !remaining.is_zero() {
                    return remaining;
                }
            }
        }
        
        // Get error history for this connection
        let recent_errors = self.connection_errors
            .get(connection_id)
            .map(|errors| errors.clone())
            .unwrap_or_default();
            
        // Calculate adaptive backoff based on error patterns
        if recent_errors.is_empty() {
            // Default exponential backoff
            let base_delay = get_config().connection.reconnect_delay_base_secs;
            let max_delay = get_config().connection.max_reconnect_delay_secs;
            
            let delay = base_delay * 1.5f64.powi(retry_count as i32);
            Duration::from_secs_f64(delay.min(max_delay))
        } else {
            // Check for patterns
            let mut has_rate_limiting = false;
            let mut has_auth_errors = false;
            let mut connection_failures = 0;
            
            for (_, category) in &recent_errors {
                match category {
                    ErrorCategory::RateLimitExceeded => has_rate_limiting = true,
                    ErrorCategory::Authentication => has_auth_errors = true,
                    ErrorCategory::ConnectionFailure => connection_failures += 1,
                    _ => {}
                }
            }
            
            if has_rate_limiting {
                // Rate limiting requires longer backoff
                Duration::from_secs(5 + retry_count as u64 * 10)
            } else if has_auth_errors {
                // Auth errors require fixed backoff to allow refresh
                Duration::from_secs(30)
            } else if connection_failures > 3 {
                // Multiple connection failures suggest network issues
                Duration::from_secs(10 + retry_count as u64 * 5)
            } else {
                // Standard exponential backoff
                let base_delay = get_config().connection.reconnect_delay_base_secs;
                let max_delay = get_config().connection.max_reconnect_delay_secs;
                
                let delay = base_delay * 1.5f64.powi(retry_count as i32);
                Duration::from_secs_f64(delay.min(max_delay))
            }
        }
    }
}

// Global instance
pub static ERROR_TRACKER: std::sync::OnceLock<ErrorTracker> = std::sync::OnceLock::new();

// Initialize the error tracker
pub fn init_error_tracker() -> &'static ErrorTracker {
    ERROR_TRACKER.get_or_init(|| ErrorTracker::new())
}

// Helper function to record errors with proper classification
pub fn record_error(
    exchange: Exchange, 
    connection_id: Option<&str>,
    error: &AppError
) {
    let category = ErrorCategory::from(error);
    let tracker = init_error_tracker();
    tracker.record_error(exchange, category, connection_id, error);
}