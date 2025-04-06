use lazy_static::lazy_static;

// Updated list: Top 100 tokens across exchanges.
lazy_static! {
    pub static ref TARGET_TOKENS: Vec<&'static str> = vec![
        "BTCUSDT", "ETHUSDT", "USDTUSDT", "BNBUSDT", "XRPUSDT", "USDCUSDT", "SOLUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT",
        "TONUSDT", "DOTUSDT", "MATICUSDT", "DAIUSDT", "LTCUSDT", "BCHUSDT", "SHIBUSDT", "AVAXUSDT", "LINKUSDT", "XLMUSDT",
        "UNIUSDT", "LEOUSDT", "ATOMUSDT", "OKBUSDT", "ETCUSDT", "XMRUSDT", "FILUSDT", "HBARUSDT", "APTUSDT", "ICPUSDT", "SEIUSDT",
        "NEARUSDT", "VETUSDT", "ARBUSDT", "QNTUSDT", "OPUSDT", "CROUSDT", "AAVEUSDT", "GRTUSDT", "STXUSDT", "ALGOUSDT", "INJUSDT",
        "EGLDUSDT", "FTMUSDT", "EOSUSDT", "XTZUSDT", "IMXUSDT", "FLOWUSDT", "THETAUSDT", "XECUSDT", "AXSUSDT", "SANDUSDT",
        "MANAUSDT", "RUNEUSDT", "NEOUSDT", "CFXUSDT", "KAVAUSDT", "ROSEUSDT", "SUIUSDT", "ZECUSDT", "ENJUSDT", "BATUSDT", "JTOUSDT",
        "LDOUSDT", "SNXUSDT", "GALAUSDT", "CAKEUSDT", "ONEUSDT", "COMPUSDT", "DASHUSDT", "ZILUSDT", "CHZUSDT", "FETUSDT", "JASMYUSDT",
        "DYDXUSDT", "1INCHUSDT", "PEPEUSDT", "GMXUSDT", "ARUSDT", "FARTCOINUSDT", "FLOKIUSDT", "JUPUSDT", "ONDOUSDT", "C98USDT",
        "ENAUSDT", "NEIROUSDT", "BERAUSDT", "TIAUSDT", "NOTUSDT", "ACTUSDT", "AI16ZUSDT", "AIXBTUSDT", "ALCHUSDT", "BIGTIMEUSDT",
        "ARKMUSDT", "UXLINKUSDT", "TRUMPUSDT", "SPXUSDT", "ORCAUSDT", "HYPEUSDT", "ZROUSDT", "LAYERUSDT", "TUTUSDT", "EIGENUSDT",
        "SAFEUSDT", "PARTIUSDT", "WIFUSDT", "TAOUSDT", "WALUSDT", "API3USDT", "BROCCOLIUSDT", "PENDLEUSDT", "APEUSDT", "OMUSDT",
        "MUBARAKUSDT", "POPCATUSDT", "JELLYUSDT", "DOGSUSDT", "ETHFIUSDT", "PEOPLEUSDT", "KAITOUSDT", "KSMUSDT", "BONKUSDT", "PNUTUSDT",
        "FUNUSDT", "STMXUSDT", "HNTUSDT", "LENDUSDT", "STPTUSDT", "CVCUSDT", "MATICUSDT", "AUDIOUSDT", "RLCUSDT", "AUCTIONUSDT",
        "MIOTAUSDT", "MKRUSDT", "KNCUSDT", "ZRXUSDT", "RENUSDT", "BALUSDT", "LRCUSDT", "SUSHIUSDT", "YFIUSDT", "UMAUSDT", "ENSUSDT",
        "BANDUSDT", "CELRUSDT", "OGNUSDT", "SCUSDT", "OMGUSDT", "QTUMUSDT", "IOSTUSDT", "ONTUSDT", "WAVESUSDT", "CRVUSDT",
    ];
}

// Add this function to token_lists.rs
pub fn is_target_token(symbol: &str) -> bool {
    // Normalize the symbol (convert to uppercase, remove exchange prefix)
    let normalized = if symbol.contains(':') {
        let parts: Vec<&str> = symbol.split(':').collect();
        if parts.len() > 1 {
            parts[1].to_uppercase()
        } else {
            symbol.to_uppercase()
        }
    } else {
        symbol.to_uppercase()
    };
    
    TARGET_TOKENS.iter().any(|&token| token == normalized)
}

// Function to check if a token is in our target list
pub fn is_target_token_flexible(symbol: &str) -> bool {
    // Normalize the symbol more aggressively
    let normalized = normalize_symbol(symbol);
    
    // Check for direct match first
    if TARGET_TOKENS.iter().any(|&token| normalize_symbol(token) == normalized) {
        return true;
    }
    
    // Try to match base/quote pairs more flexibly
    let quote_currencies = ["USDT", "USD", "BTC", "ETH", "USDC"];
    
    for &quote in &quote_currencies {
        if normalized.ends_with(quote) {
            let base = &normalized[0..normalized.len() - quote.len()];
            
            // Check if any target token has this base and quote
            for &target in TARGET_TOKENS.iter() {
                let norm_target = normalize_symbol(target);
                if norm_target.starts_with(base) && norm_target.ends_with(quote) {
                    return true;
                }
            }
        }
    }
    
    false
}

// Function to get normalized symbol without exchange prefix
pub fn normalize_symbol(symbol: &str) -> String {
    let symbol_without_prefix = if symbol.contains(':') {
        let parts: Vec<&str> = symbol.split(':').collect();
        if parts.len() > 1 {
            parts[1].to_string()
        } else {
            symbol.to_string()
        }
    } else {
        symbol.to_string()
    };
    
    // Aggressively normalize by removing all separators and converting to uppercase
    symbol_without_prefix
        .replace('_', "")
        .replace('-', "")
        .to_uppercase()
}

// Function to extract exchange from prefixed symbol
pub fn extract_exchange(symbol: &str) -> Option<String> {
    if symbol.contains(':') {
        let parts: Vec<&str> = symbol.split(':').collect();
        if parts.len() > 1 {
            return Some(parts[0].to_string());
        }
    }
    None
}