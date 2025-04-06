// terminal_log.rs - Optimized metrics display for terminal with profitable opportunity counter

use crate::core::*;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::io::{self, Write};
use tokio::time::interval;
use colored::*;

/// Structure to track and display metrics
pub struct MetricsTracker {
    last_price_updates: u64,
    last_ws_messages: u64,
    last_cross_exchange_checks: u64,
    last_profitable_opps: u64,
    start_time: Instant,
    last_report_time: Instant,
}

impl MetricsTracker {
    pub fn new() -> Self {
        Self {
            last_price_updates: 0,
            last_ws_messages: 0,
            last_cross_exchange_checks: 0,
            last_profitable_opps: 0,
            start_time: Instant::now(),
            last_report_time: Instant::now(),
        }
    }

    fn clear_terminal() {
        // First try ANSI escape codes
        print!("\x1B[2J\x1B[1;1H");
        // Then print multiple newlines as a fallback
        println!("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");
        // Ensure output is flushed
        let _ = io::stdout().flush();
    }

    fn format_rate(count: u64, elapsed_secs: f64) -> String {
        let rate = count as f64 / elapsed_secs;
        if rate >= 1000.0 {
            format!("{:.1}k/sec", rate / 1000.0)
        } else {
            format!("{:.1}/sec", rate)
        }
    }
}

/// Run a clean terminal metrics display that updates every 10 seconds
pub async fn run_clean_metrics_display(app_state: AppState) {
    // Set up the interval for 10 seconds (increased from 5 for VPS optimization)
    let mut interval_timer = interval(Duration::from_secs(10));
    let mut tracker = MetricsTracker::new();
    
    // Clear terminal on start
    MetricsTracker::clear_terminal();
    
    println!("{}", "TriFury Cross-Exchange Arbitrage Scanner - Metrics Display".bold().green());
    println!("{}", "========================================".green());
    println!("Starting metrics collection...");
    
    // Ensure all initial output is flushed
    let _ = io::stdout().flush();
    
    loop {
        interval_timer.tick().await;
        
        // Get current counts
        let current_price_updates = app_state.price_updates.load(Ordering::Relaxed);
        let current_ws_messages = app_state.websocket_messages.load(Ordering::Relaxed);
        let current_cross_exchange = app_state.cross_exchange_checks.load(Ordering::Relaxed);
        let current_profitable_opps = app_state.profitable_opportunities.load(Ordering::Relaxed);
        
        // Calculate differences since last report
        let price_updates_diff = current_price_updates.saturating_sub(tracker.last_price_updates);
        let ws_messages_diff = current_ws_messages.saturating_sub(tracker.last_ws_messages);
        let cross_exchange_diff = current_cross_exchange.saturating_sub(tracker.last_cross_exchange_checks);
        let profitable_opps_diff = current_profitable_opps.saturating_sub(tracker.last_profitable_opps);
        
        // Update last values
        tracker.last_price_updates = current_price_updates;
        tracker.last_ws_messages = current_ws_messages;
        tracker.last_cross_exchange_checks = current_cross_exchange;
        tracker.last_profitable_opps = current_profitable_opps;
        
        // Calculate elapsed time
        let elapsed = tracker.last_report_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        tracker.last_report_time = Instant::now();
        
        // Calculate total runtime
        let total_runtime = tracker.start_time.elapsed();
        let runtime_hours = total_runtime.as_secs() / 3600;
        let runtime_minutes = (total_runtime.as_secs() % 3600) / 60;
        let runtime_seconds = total_runtime.as_secs() % 60;
        
        // Calculate rates
        let price_updates_rate = MetricsTracker::format_rate(price_updates_diff, elapsed_secs);
        let ws_messages_rate = MetricsTracker::format_rate(ws_messages_diff, elapsed_secs);
        let cross_exchange_rate = MetricsTracker::format_rate(cross_exchange_diff, elapsed_secs);
        let profitable_opps_rate = MetricsTracker::format_rate(profitable_opps_diff, elapsed_secs);
        
        // Get active tokens count
        let price_data_count = app_state.price_data.len();
        
        // Clear terminal and display metrics
        MetricsTracker::clear_terminal();
        
        println!("{}", "TriFury Cross-Exchange Arbitrage Scanner - Metrics Display".bold().green());
        println!("{}", "========================================".green());
        
        println!("{}: {}h {}m {}s", 
                "Runtime".bold().blue(), 
                runtime_hours, 
                runtime_minutes, 
                runtime_seconds);
        
        println!("\n{}", "LAST 10 SECONDS:".bold().yellow());
        
        // Show cross-exchange checks prominently
        println!("- {}: {} ({}) ", 
                "Cross-Exchange Checks".bold(), 
                cross_exchange_diff.to_string().green().bold(), 
                cross_exchange_rate.green());
        
        // Add profitable opportunities counter
        println!("- {}: {} ({}) ", 
                "Profitable Opportunities".bold(), 
                profitable_opps_diff.to_string().green().bold(), 
                profitable_opps_rate.green());
        
        println!("- {}: {} ({}) ", 
                "Price Updates".bold(), 
                price_updates_diff.to_string().cyan(), 
                price_updates_rate.cyan());
        
        println!("- {}: {} ({}) ", 
                "WebSocket Messages".bold(), 
                ws_messages_diff.to_string().cyan(), 
                ws_messages_rate.cyan());
        
        println!("\n{}", "SYSTEM STATUS:".bold().yellow());
        println!("- {}: {}", 
                "Price Data Count".bold(), 
                price_data_count.to_string().cyan());
        
        // Global connection status
        let global_idle_time = app_state.get_global_idle_time();
        let status_color = if global_idle_time < 5000 {
            "green".to_string()
        } else if global_idle_time < 15000 {
            "yellow".to_string()
        } else {
            "red".to_string()
        };
        
        println!("- {}: {} ms ({})", 
                "Last WebSocket Activity".bold(), 
                global_idle_time.to_string(), 
                if status_color == "green" { 
                    "HEALTHY".green().bold() 
                } else if status_color == "yellow" { 
                    "WARNING".yellow().bold() 
                } else { 
                    "STALE".red().bold() 
                });
        
        // Performance metrics
        let price_updates_per_ws = if ws_messages_diff > 0 {
            price_updates_diff as f64 / ws_messages_diff as f64
        } else {
            0.0
        };
        
        println!("- {}: {:.2}", 
                "Price Updates per WS Message".bold(), 
                price_updates_per_ws);
                
        // Show total opportunities found
        println!("- {}: {}", 
                "Total Profitable Opportunities".bold(), 
                current_profitable_opps.to_string().green().bold());

        println!("\n{}", "Press Ctrl+C to exit".dimmed());
        
        // Force flush stdout to ensure display updates
        if let Err(e) = io::stdout().flush() {
            eprintln!("Error flushing stdout: {}", e);
        }
        
        // Make sure to yield to other tasks
        tokio::task::yield_now().await;
    }
}