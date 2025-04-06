// exchange_types.rs - Common structures and enums for exchange integrations

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, str::FromStr};

/// Exchange identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Phemex,
    LBank,
    XtCom,
    TapBit,
    Hbit,
    Batonex,
    CoinCatch,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Exchange::Phemex => write!(f, "PHEMEX"),
            Exchange::LBank => write!(f, "LBANK"),
            Exchange::XtCom => write!(f, "XTCOM"),
            Exchange::TapBit => write!(f, "TAPBIT"),
            Exchange::Hbit => write!(f, "HBIT"),
            Exchange::Batonex => write!(f, "BATONEX"),
            Exchange::CoinCatch => write!(f, "COINCATCH"),
        }
    }
}

// Added FromStr implementation for Exchange enum
impl FromStr for Exchange {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PHEMEX" => Ok(Exchange::Phemex),
            "LBANK" => Ok(Exchange::LBank),
            "XTCOM" => Ok(Exchange::XtCom),
            "TAPBIT" => Ok(Exchange::TapBit),
            "HBIT" => Ok(Exchange::Hbit),
            "BATONEX" => Ok(Exchange::Batonex),
            "COINCATCH" => Ok(Exchange::CoinCatch),
            _ => Err(format!("Unknown exchange: {}", s)),
        }
    }
}

/// Holds the fee structure for an exchange
#[derive(Debug, Clone)]
pub struct ExchangeFees {
    pub maker_fee: f64,
    pub taker_fee: f64,
    pub exchange: Exchange,
}

impl ExchangeFees {
    pub fn new(exchange: Exchange, maker_fee: f64, taker_fee: f64) -> Self {
        Self {
            exchange,
            maker_fee,
            taker_fee,
        }
    }
}

/// Common orderbook data structure that works across exchanges
#[derive(Debug, Clone)]
pub struct StandardOrderBook {
    pub symbol: String,
    pub exchange: Exchange,
    pub best_bid: f64,
    pub best_ask: f64,
    pub depth_bids: Vec<(f64, f64)>, // (price, quantity)
    pub depth_asks: Vec<(f64, f64)>, // (price, quantity)
    pub timestamp: i64,
}

impl StandardOrderBook {
    /// Create a new standard orderbook with minimal data
    pub fn new_minimal(
        symbol: &str,
        exchange: Exchange, 
        best_bid: f64, 
        best_ask: f64, 
        timestamp: i64
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            exchange,
            best_bid,
            best_ask,
            depth_bids: Vec::new(),
            depth_asks: Vec::new(),
            timestamp,
        }
    }
    
    /// Add depth data to the orderbook
    pub fn with_depth(
        mut self,
        depth_bids: Vec<(f64, f64)>,
        depth_asks: Vec<(f64, f64)>,
    ) -> Self {
        self.depth_bids = depth_bids;
        self.depth_asks = depth_asks;
        self
    }
    
    /// Calculate effective ask price considering depth and slippage
    pub fn effective_ask_price(&self, size_usd: f64) -> f64 {
        if size_usd <= 0.0 || self.depth_asks.is_empty() {
            return self.best_ask;
        }
        
        // For buys, we use the ask side
        let mut remaining_usd = size_usd;
        let mut total_cost = 0.0;
        let mut total_quantity = 0.0;
        
        for &(price, available_quantity) in &self.depth_asks {
            // Skip invalid entries
            if price <= 0.0 || available_quantity <= 0.0 {
                continue;
            }
            
            // Calculate how much we can trade at this level
            let level_value = price * available_quantity;
            let executable_usd = remaining_usd.min(level_value);
            let executable_quantity = executable_usd / price;
            
            total_cost += executable_quantity * price;
            total_quantity += executable_quantity;
            remaining_usd -= executable_usd;
            
            if remaining_usd <= 0.0 || remaining_usd < 0.00001 {
                break;
            }
        }
        
        if total_quantity > 0.0 {
            return total_cost / total_quantity;
        }
        
        self.best_ask
    }
    
    /// Calculate effective bid price considering depth and slippage
    pub fn effective_bid_price(&self, size_usd: f64) -> f64 {
        if size_usd <= 0.0 || self.depth_bids.is_empty() {
            return self.best_bid;
        }
        
        // For sells, we use the bid side
        let mut remaining_usd = size_usd;
        let mut total_cost = 0.0;
        let mut total_quantity = 0.0;
        
        for &(price, available_quantity) in &self.depth_bids {
            // Skip invalid entries
            if price <= 0.0 || available_quantity <= 0.0 {
                continue;
            }
            
            // Calculate how much we can trade at this level
            let level_value = price * available_quantity;
            let executable_usd = remaining_usd.min(level_value);
            let executable_quantity = executable_usd / price;
            
            total_cost += executable_quantity * price;
            total_quantity += executable_quantity;
            remaining_usd -= executable_usd;
            
            if remaining_usd <= 0.0 || remaining_usd < 0.00001 {
                break;
            }
        }
        
        if total_quantity > 0.0 {
            return total_cost / total_quantity;
        }
        
        self.best_bid
    }
    
    /// Calculate slippage as a percentage for ask side
    pub fn calculate_ask_slippage(&self, size_usd: f64) -> f64 {
        let effective_price = self.effective_ask_price(size_usd);
        
        if self.best_ask <= 0.0 || effective_price <= 0.0 {
            return 0.001; // Default slippage
        }
        
        // For buys, slippage means paying more than best ask
        let slippage_pct = effective_price / self.best_ask - 1.0;
        slippage_pct.max(0.0)
    }
    
    /// Calculate slippage as a percentage for bid side
    pub fn calculate_bid_slippage(&self, size_usd: f64) -> f64 {
        let effective_price = self.effective_bid_price(size_usd);
        
        if self.best_bid <= 0.0 || effective_price <= 0.0 {
            return 0.001; // Default slippage
        }
        
        // For sells, slippage means receiving less than best bid
        let slippage_pct = 1.0 - effective_price / self.best_bid;
        slippage_pct.max(0.0)
    }
    
    /// Calculate effective ask price with improved market impact model
    pub fn effective_ask_price_enhanced(&self, size_usd: f64) -> (f64, bool) {
        if size_usd <= 0.0 || self.depth_asks.is_empty() {
            return (self.best_ask, false);
        }
        
        // Validate order book
        let is_valid = self.validate_orderbook_asks();
        if !is_valid {
            // Return best ask with a flag indicating invalid data
            return (self.best_ask, false);
        }
        
        // Sort asks by price (ascending)
        let mut sorted_asks = self.depth_asks.clone();
        sorted_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Apply market impact model
        // For larger orders, impact increases non-linearly
        let mut remaining_usd = size_usd;
        let mut total_cost = 0.0;
        let mut total_quantity = 0.0;
        
        // Track if we have sufficient liquidity
        let mut has_sufficient_liquidity = false;
        
        // Apply increasing impact factor for deeper levels
        for (level_idx, &(price, available_quantity)) in sorted_asks.iter().enumerate() {
            let impact_factor = 1.0 + (level_idx as f64 * 0.005); // Increases with depth
            let adjusted_price = price * impact_factor;
            
            // How much we can execute at this level
            let level_value = adjusted_price * available_quantity;
            let executable_usd = remaining_usd.min(level_value);
            let executable_quantity = executable_usd / adjusted_price;
            
            total_cost += executable_quantity * adjusted_price;
            total_quantity += executable_quantity;
            remaining_usd -= executable_usd;
            
            // If we've filled at least 95% of our order, consider it sufficient
            if remaining_usd <= size_usd * 0.05 {
                has_sufficient_liquidity = true;
                break;
            }
        }
        
        // Calculate effective price
        if total_quantity > 0.0 {
            return (total_cost / total_quantity, has_sufficient_liquidity);
        }
        
        // Fallback to best ask
        (self.best_ask, false)
    }
    
    /// Calculate effective bid price with improved market impact model
    pub fn effective_bid_price_enhanced(&self, size_usd: f64) -> (f64, bool) {
        if size_usd <= 0.0 || self.depth_bids.is_empty() {
            return (self.best_bid, false);
        }
        
        // Validate order book
        let is_valid = self.validate_orderbook_bids();
        if !is_valid {
            // Return best bid with a flag indicating invalid data
            return (self.best_bid, false);
        }
        
        // Sort bids by price (descending)
        let mut sorted_bids = self.depth_bids.clone();
        sorted_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Apply market impact model
        let mut remaining_usd = size_usd;
        let mut total_cost = 0.0;
        let mut total_quantity = 0.0;
        
        // Track if we have sufficient liquidity
        let mut has_sufficient_liquidity = false;
        
        // Apply increasing impact factor for deeper levels
        for (level_idx, &(price, available_quantity)) in sorted_bids.iter().enumerate() {
            let impact_factor = 1.0 - (level_idx as f64 * 0.005); // Decreases with depth
            let adjusted_price = price * impact_factor;
            
            // How much we can execute at this level
            let level_value = adjusted_price * available_quantity;
            let executable_usd = remaining_usd.min(level_value);
            let executable_quantity = executable_usd / adjusted_price;
            
            total_cost += executable_quantity * adjusted_price;
            total_quantity += executable_quantity;
            remaining_usd -= executable_usd;
            
            // If we've filled at least 95% of our order, consider it sufficient
            if remaining_usd <= size_usd * 0.05 {
                has_sufficient_liquidity = true;
                break;
            }
        }
        
        // Calculate effective price
        if total_quantity > 0.0 {
            return (total_cost / total_quantity, has_sufficient_liquidity);
        }
        
        // Fallback to best bid
        (self.best_bid, false)
    }
    
    /// Calculate slippage with enhanced market impact and liquidity check
    pub fn calculate_enhanced_slippage(&self, size_usd: f64, is_buy: bool, symbol: &str) -> (f64, bool) {
        // Adjust trade size based on asset type
        let adjusted_size = self.get_adaptive_size(size_usd, symbol);
        
        if is_buy {
            let (effective_price, has_liquidity) = self.effective_ask_price_enhanced(adjusted_size);
            
            if self.best_ask <= 0.0 || effective_price <= 0.0 {
                return (0.001, false); // Default minimum slippage
            }
            
            // For buys, slippage means paying more than best ask
            let slippage_pct = effective_price / self.best_ask - 1.0;
            (slippage_pct.max(0.0), has_liquidity)
        } else {
            let (effective_price, has_liquidity) = self.effective_bid_price_enhanced(adjusted_size);
            
            if self.best_bid <= 0.0 || effective_price <= 0.0 {
                return (0.001, false); // Default minimum slippage
            }
            
            // For sells, slippage means receiving less than best bid
            let slippage_pct = 1.0 - effective_price / self.best_bid;
            (slippage_pct.max(0.0), has_liquidity)
        }
    }
    
    /// Validate order book ask data with sanity checks
    fn validate_orderbook_asks(&self) -> bool {
        if self.depth_asks.is_empty() {
            return false;
        }
        
        // Check if we have enough price levels
        if self.depth_asks.len() < 3 {
            // Too few levels to make a reliable assessment
            return false;
        }
        
        // Check for price ordering (asks should increase in price)
        let mut sorted_asks = self.depth_asks.clone();
        sorted_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Check if the first price matches our best ask
        if (sorted_asks[0].0 - self.best_ask).abs() / self.best_ask > 0.01 {
            // Best ask doesn't match first level (off by more than 1%)
            return false;
        }
        
        // Check for realistic spread
        if self.best_ask > 0.0 && self.best_bid > 0.0 {
            let spread_pct = (self.best_ask / self.best_bid) - 1.0;
            
            if spread_pct > 0.05 {
                // Spread > 5% is suspicious for most liquid assets
                return false;
            }
        }
        
        // Check for reasonable depth distribution
        // Typically, volume should decrease as we go deeper
        let mut total_volume = 0.0;
        for &(_, qty) in &self.depth_asks {
            if qty <= 0.0 {
                return false; // No zero or negative quantities
            }
            total_volume += qty;
        }
        
        if total_volume <= 0.0 {
            return false;
        }
        
        // Check for unusually thin or thick book
        if self.depth_asks.len() >= 3 {
            // Calculate volume concentration in top 3 levels
            let top_volume = sorted_asks[0].1 + sorted_asks[1].1 + sorted_asks[2].1;
            let concentration = top_volume / total_volume;
            
            // If >95% of volume is in top levels or <5%, it's suspicious
            if concentration > 0.95 || concentration < 0.05 {
                return false;
            }
        }
        
        // Check for unrealistic price jumps
        for i in 1..sorted_asks.len() {
            let price_change = (sorted_asks[i].0 - sorted_asks[i-1].0) / sorted_asks[i-1].0;
            
            // Price jump of >20% between levels is suspicious
            if price_change > 0.2 {
                return false;
            }
        }
        
        true
    }
    
    /// Validate order book bid data with sanity checks
    fn validate_orderbook_bids(&self) -> bool {
        if self.depth_bids.is_empty() {
            return false;
        }
        
        // Check if we have enough price levels
        if self.depth_bids.len() < 3 {
            // Too few levels to make a reliable assessment
            return false;
        }
        
        // Check for price ordering (bids should decrease in price)
        let mut sorted_bids = self.depth_bids.clone();
        sorted_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        
        // Check if the first price matches our best bid
        if (sorted_bids[0].0 - self.best_bid).abs() / self.best_bid > 0.01 {
            // Best bid doesn't match first level (off by more than 1%)
            return false;
        }
        
        // Check for realistic spread
        if self.best_ask > 0.0 && self.best_bid > 0.0 {
            let spread_pct = (self.best_ask / self.best_bid) - 1.0;
            
            if spread_pct > 0.05 {
                // Spread > 5% is suspicious for most liquid assets
                return false;
            }
        }
        
        // Check for reasonable depth distribution
        let mut total_volume = 0.0;
        for &(_, qty) in &self.depth_bids {
            if qty <= 0.0 {
                return false; // No zero or negative quantities
            }
            total_volume += qty;
        }
        
        if total_volume <= 0.0 {
            return false;
        }
        
        // Check for unusually thin or thick book
        if self.depth_bids.len() >= 3 {
            // Calculate volume concentration in top 3 levels
            let top_volume = sorted_bids[0].1 + sorted_bids[1].1 + sorted_bids[2].1;
            let concentration = top_volume / total_volume;
            
            // If >95% of volume is in top levels or <5%, it's suspicious
            if concentration > 0.95 || concentration < 0.05 {
                return false;
            }
        }
        
        // Check for unrealistic price jumps
        for i in 1..sorted_bids.len() {
            let price_change = (sorted_bids[i-1].0 - sorted_bids[i].0) / sorted_bids[i].0;
            
            // Price jump of >20% between levels is suspicious
            if price_change > 0.2 {
                return false;
            }
        }
        
        true
    }
    
    /// Get adaptive trade size based on asset type and market conditions
    fn get_adaptive_size(&self, base_size: f64, symbol: &str) -> f64 {
        // Use smart asset type detection for optimal sizing
        let symbol_upper = symbol.to_uppercase();
        
        // Extract base and quote if possible
        let (base_token, _) = if symbol_upper.contains("USDT") {
            (symbol_upper.replace("USDT", ""), "USDT")
        } else if symbol_upper.contains("USD") {
            (symbol_upper.replace("USD", ""), "USD")  
        } else if symbol_upper.contains("BTC") && !symbol_upper.starts_with("BTC") {
            (symbol_upper.replace("BTC", ""), "BTC")
        } else if symbol_upper.contains("ETH") && !symbol_upper.starts_with("ETH") {
            (symbol_upper.replace("ETH", ""), "ETH")
        } else {
            (symbol_upper.clone(), "")
        };
        
        // Adjust by asset liquidity category
        if base_token == "BTC" || base_token == "ETH" {
            // Tier 1: Highest liquidity - use full size
            base_size
        } else if ["SOL", "BNB", "XRP", "ADA", "DOGE", "MATIC", "LINK"].contains(&base_token.as_str()) {
            // Tier 2: High liquidity - 80% of base size
            base_size * 0.8
        } else if ["AVAX", "DOT", "ATOM", "UNI", "LTC", "FIL", "NEAR", "OP", "ARB"].contains(&base_token.as_str()) {
            // Tier 3: Medium liquidity - 70% of base size
            base_size * 0.7
        } else if ["PEPE", "SHIB", "DOGE", "FLOKI", "WIF", "BONK"].contains(&base_token.as_str()) {
            // Tier 4: Meme coins - may have unusual liquidity profiles
            // Use 60% of base size
            base_size * 0.6
        } else {
            // Tier 5: Lower liquidity or unknown - 50% of base size
            base_size * 0.5
        }
    }
    
    /// Get the effective bid price after accounting for slippage at size
    pub fn effective_bid_price_with_size(&self, size: f64) -> f64 {
        if size <= 0.0 || self.depth_bids.is_empty() {
            return self.best_bid;
        }
        
        let mut remaining_size = size;
        let mut total_cost = 0.0;
        
        for &(price, quantity) in &self.depth_bids {
            let executable = remaining_size.min(quantity);
            total_cost += executable * price;
            remaining_size -= executable;
            
            if remaining_size <= 0.0 {
                break;
            }
        }
        
        if remaining_size > 0.0 {
            // Not enough liquidity, use best available price for remainder
            total_cost += remaining_size * self.depth_bids.last().unwrap_or(&(self.best_bid, 0.0)).0;
        }
        
        total_cost / size
    }
    
    /// Get the effective ask price after accounting for slippage at size
    pub fn effective_ask_price_with_size(&self, size: f64) -> f64 {
        if size <= 0.0 || self.depth_asks.is_empty() {
            return self.best_ask;
        }
        
        let mut remaining_size = size;
        let mut total_cost = 0.0;
        
        for &(price, quantity) in &self.depth_asks {
            let executable = remaining_size.min(quantity);
            total_cost += executable * price;
            remaining_size -= executable;
            
            if remaining_size <= 0.0 {
                break;
            }
        }
        
        if remaining_size > 0.0 {
            // Not enough liquidity, use best available price for remainder
            total_cost += remaining_size * self.depth_asks.last().unwrap_or(&(self.best_ask, 0.0)).0;
        }
        
        total_cost / size
    }
    
    /// Calculate total liquidity within a specified price range
    pub fn calculate_liquidity_in_range(&self, price_pct_range: f64) -> (f64, f64) {
        // Calculate liquidity available within a certain percentage of the midpoint
        let mid_price = (self.best_bid + self.best_ask) / 2.0;
        let lower_bound = mid_price * (1.0 - price_pct_range);
        let upper_bound = mid_price * (1.0 + price_pct_range);
        
        // Calculate bid side liquidity in range
        let mut bid_liquidity = 0.0;
        for &(price, qty) in &self.depth_bids {
            if price >= lower_bound {
                bid_liquidity += price * qty;
            }
        }
        
        // Calculate ask side liquidity in range
        let mut ask_liquidity = 0.0;
        for &(price, qty) in &self.depth_asks {
            if price <= upper_bound {
                ask_liquidity += price * qty;
            }
        }
        
        (bid_liquidity, ask_liquidity)
    }
}

/// Represents a cross-exchange arbitrage opportunity
#[derive(Debug, Clone)]
pub struct CrossExchangeArb {
    pub symbol: String,
    pub buy_exchange: Exchange,
    pub sell_exchange: Exchange,
    pub buy_price: f64,
    pub sell_price: f64,
    pub timestamp: i64,
    pub profit_pct: f64,
    pub net_profit_pct: f64,
    pub total_fees_pct: f64,
}

/// Represents a three-way cross-exchange arbitrage path
#[derive(Debug, Clone)]
pub struct ThreeExchangePath {
    pub symbol: String,
    pub exchange1: Exchange,
    pub exchange2: Exchange,
    pub exchange3: Exchange,
    pub price1: f64,
    pub price2: f64,
    pub price3: f64,
    pub timestamp: i64,
    pub profit_pct: f64,
    pub net_profit_pct: f64,
    pub total_fees_pct: f64,
}

// Add to exchange_types.rs

/// Represents a multi-hop arbitrage path across exchanges
#[derive(Debug, Clone)]
pub struct MultiHopArbitragePath {
    pub symbol_path: Vec<String>,
    pub exchange_path: Vec<Exchange>,
    pub price_path: Vec<f64>,
    pub timestamp: i64,
    pub total_profit_pct: f64,
    pub net_profit_pct: f64,
    pub total_fees_pct: f64,
    pub total_slippage_pct: f64,
    pub path_id: String,
}

impl MultiHopArbitragePath {
    /// Create a new multi-hop path with pre-allocated vectors
    pub fn new(path_length: usize) -> Self {
        Self {
            symbol_path: Vec::with_capacity(path_length),
            exchange_path: Vec::with_capacity(path_length),
            price_path: Vec::with_capacity(path_length),
            timestamp: chrono::Utc::now().timestamp_millis(),
            total_profit_pct: 0.0,
            net_profit_pct: 0.0,
            total_fees_pct: 0.0,
            total_slippage_pct: 0.0,
            path_id: String::new(),
        }
    }
    
    /// Add a hop to the path
    pub fn add_hop(&mut self, symbol: String, exchange: Exchange, price: f64) {
        self.symbol_path.push(symbol);
        self.exchange_path.push(exchange);
        self.price_path.push(price);
    }
    
    /// Generate a unique path identifier
    pub fn generate_path_id(&mut self) {
        let mut path_components = Vec::with_capacity(self.exchange_path.len() * 2);
        
        for i in 0..self.exchange_path.len() {
            path_components.push(format!("{}", self.exchange_path[i]));
            path_components.push(self.symbol_path[i].clone());
        }
        
        self.path_id = path_components.join(":");
    }
    
    /// Calculate fees and slippage for the entire path
    pub fn calculate_fees_and_slippage(
        &mut self,
        exchange_fees: &HashMap<Exchange, ExchangeFees>,
        orderbooks: &[Option<&StandardOrderBook>],
        trade_size_usd: f64,
    ) {
        self.total_fees_pct = 0.0;
        self.total_slippage_pct = 0.0;
        
        for i in 0..self.exchange_path.len() {
            // Calculate exchange fees
            let exchange = self.exchange_path[i];
            let fee_pct = exchange_fees
                .get(&exchange)
                .map(|fees| fees.taker_fee * 100.0)
                .unwrap_or(0.1); // Default to 0.1% if unknown
                
            self.total_fees_pct += fee_pct;
            
            // Calculate slippage if orderbook is available
            if i < orderbooks.len() && orderbooks[i].is_some() {
                let orderbook = orderbooks[i].unwrap();
                let (slippage, _) = orderbook.calculate_enhanced_slippage(
                    trade_size_usd,
                    i == 0, // First hop is buy, others are sell-buy pairs
                    &self.symbol_path[i],
                );
                self.total_slippage_pct += slippage * 100.0;
            } else {
                // Apply default slippage estimate
                self.total_slippage_pct += 0.05; // 0.05% default slippage per hop
            }
        }
        
        // Calculate profit and net profit
        let initial_price = self.price_path[0];
        let final_price = *self.price_path.last().unwrap_or(&0.0);
        
        if initial_price > 0.0 && final_price > 0.0 {
            // For circular paths (returning to starting currency)
            if self.symbol_path.first() == self.symbol_path.last() {
                self.total_profit_pct = (final_price / initial_price - 1.0) * 100.0;
            } else {
                // For non-circular paths, calculate compound rates
                let mut compound_rate = 1.0;
                for i in 1..self.price_path.len() {
                    compound_rate *= self.price_path[i] / self.price_path[i-1];
                }
                self.total_profit_pct = (compound_rate - 1.0) * 100.0;
            }
            
            // Calculate net profit after fees and slippage
            self.net_profit_pct = self.total_profit_pct - self.total_fees_pct - self.total_slippage_pct;
        }
    }
}