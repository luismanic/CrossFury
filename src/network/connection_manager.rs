// network/connection_manager.rs - Optimized connection health management

use crate::core::*;
use log::{info, error, debug};
use tokio::time::{interval, Duration};
use std::sync::atomic::Ordering;

/// Run a dedicated health manager for all connections
pub async fn run_connection_health_manager(app_state: AppState) {
    // Check connection health every 5 seconds (increased from 3)
    let mut interval = interval(Duration::from_secs(5));
    
    info!("Starting connection health manager");
    
    loop {
        interval.tick().await;
        
        // Check global message activity
        let global_idle_time = app_state.get_global_idle_time();
        
        if global_idle_time > STALE_CONNECTION_TIMEOUT as u64 {
            error!("GLOBAL CONNECTION ALERT: No messages for {}ms (threshold: {}ms)",
                  global_idle_time, STALE_CONNECTION_TIMEOUT);
                  
            // Reset all connections if globally stale
            if global_idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                error!("GLOBAL CONNECTION EMERGENCY: Signaling all connections to reconnect");
                
                // Signal all connections to reconnect
                for conn_entry in app_state.connection_timestamps.iter() {
                    let conn_id = conn_entry.key().clone();
                    app_state.signal_reconnect(&conn_id);
                }
            }
        } else {
            debug!("Global connection health: Last message {}ms ago", global_idle_time);
        }
        
        // Check individual connection activity - only log problematic connections
        for conn_entry in app_state.connection_timestamps.iter() {
            let conn_id = conn_entry.key();
            let idle_time = app_state.get_connection_idle_time(conn_id);
            
            if idle_time > STALE_CONNECTION_TIMEOUT as u64 {
                error!("Connection {} ALERT: No messages for {}ms (threshold: {}ms)", 
                      conn_id, idle_time, STALE_CONNECTION_TIMEOUT);
                
                // Signal reconnection for stale connections
                if idle_time > FORCE_RECONNECT_TIMEOUT as u64 {
                    error!("Connection {} EMERGENCY: Forcing reconnection after {}ms idle", 
                          conn_id, idle_time);
                    app_state.signal_reconnect(conn_id);
                }
            }
        }
        
        // Log WebSocket and price update rates - but less frequently
        static mut LOG_COUNTER: u32 = 0;
        unsafe {
            LOG_COUNTER += 1;
            if LOG_COUNTER % 12 == 0 { // Only log every ~1 minute (changed from 10*3s to 12*5s)
                let websocket_msgs = app_state.websocket_messages.load(Ordering::Relaxed);
                let price_updates = app_state.price_updates.load(Ordering::Relaxed);
                let cross_exchange_checks = app_state.cross_exchange_checks.load(Ordering::Relaxed);
                let profitable_opportunities = app_state.profitable_opportunities.load(Ordering::Relaxed);
                
                info!("Connection stats: WS msgs={}, Price updates={}, Cross-exchange checks={}, Profitable opps={}", 
                      websocket_msgs, price_updates, cross_exchange_checks, profitable_opportunities);
            }
        }
        
        // Yield to allow other tasks to run
        tokio::task::yield_now().await;
    }
}