pub mod adapter;
pub mod connect;
pub mod depth;
pub mod fetcher;

pub use adapter::Event;
use adapter::{Exchange, MarketType, StreamType};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    fmt::{self, Write},
    hash::Hash,
};

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Timeframe::S1 => "1s",
                Timeframe::S5 => "5s",
                Timeframe::S15 => "15s",
                Timeframe::S30 => "30s",
                Timeframe::M1 => "1m",
                Timeframe::M3 => "3m",
                Timeframe::M5 => "5m",
                Timeframe::M15 => "15m",
                Timeframe::M30 => "30m",
                Timeframe::H1 => "1h",
                Timeframe::H2 => "2h",
                Timeframe::H4 => "4h",
            }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Timeframe {
    S1,
    S5,
    S15,
    S30,
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H2,
    H4,
}

impl Timeframe {
    pub const ALL: [Timeframe; 12] = [
        Timeframe::S1,
        Timeframe::S5,
        Timeframe::S15,
        Timeframe::S30,
        Timeframe::M1,
        Timeframe::M3,
        Timeframe::M5,
        Timeframe::M15,
        Timeframe::M30,
        Timeframe::H1,
        Timeframe::H2,
        Timeframe::H4,
    ];

    pub fn to_seconds(self) -> u16 {
        match self {
            Timeframe::S1 => 1,
            Timeframe::S5 => 5,
            Timeframe::S15 => 15,
            Timeframe::S30 => 30,
            Timeframe::M1 => 60,
            Timeframe::M3 => 180,
            Timeframe::M5 => 300,
            Timeframe::M15 => 900,
            Timeframe::M30 => 1800,
            Timeframe::H1 => 3600,
            Timeframe::H2 => 7200,
            Timeframe::H4 => 28800,
        }
    }

    pub fn to_minutes(self) -> u16 {
        self.to_seconds()/60
    }

    pub fn to_milliseconds(self) -> u64 {
        u64::from(self.to_seconds()) * 1000
    }
}

impl From<Timeframe> for f32 {
    fn from(timeframe: Timeframe) -> f32 {
        timeframe.to_milliseconds() as f32
    }
}

impl From<Timeframe> for u64 {
    fn from(timeframe: Timeframe) -> u64 {
        timeframe.to_milliseconds()
    }
}

impl From<u64> for Timeframe {
    fn from(milliseconds: u64) -> Timeframe {
        match milliseconds {
            1_000 => Timeframe::S1,
            5_000 => Timeframe::S5,
            15_000 => Timeframe::S15,
            30_000 => Timeframe::S30,
            60_000 => Timeframe::M1,
            180_000 => Timeframe::M3,
            300_000 => Timeframe::M5,
            900_000 => Timeframe::M15,
            1_800_000 => Timeframe::M30,
            3_600_000 => Timeframe::H1,
            7_200_000 => Timeframe::H2,
            14_400_000 => Timeframe::H4,
            _ => panic!("Invalid timeframe: {milliseconds}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Ticker {
    data: [u64; 2],
    len: u8,
    pub market_type: MarketType,
}

impl Ticker {
    pub fn new<S: AsRef<str>>(ticker: S, market_type: MarketType) -> Self {
        let ticker = ticker.as_ref();
        let base_len = ticker.len();

        assert!(base_len <= 20, "Ticker too long");
        assert!(
            ticker
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "Invalid character in ticker: {ticker:?}"
        );

        let mut data = [0u64; 2];
        let mut len = 0;

        for (i, c) in ticker.bytes().enumerate() {
            let value = match c {
                b'0'..=b'9' => c - b'0',
                b'A'..=b'Z' => c - b'A' + 10,
                b'_' => 36,
                _ => unreachable!(),
            };
            let shift = (i % 10) * 6;
            data[i / 10] |= u64::from(value) << shift;
            len += 1;
        }

        Ticker {
            data,
            len,
            market_type,
        }
    }

    pub fn to_full_symbol_and_type(&self) -> (String, MarketType) {
        let mut result = String::with_capacity(self.len as usize);
        for i in 0..self.len {
            let value = (self.data[i as usize / 10] >> ((i % 10) * 6)) & 0x3F;
            let c = match value {
                0..=9 => (b'0' + value as u8) as char,
                10..=35 => (b'A' + (value as u8 - 10)) as char,
                36 => '_',
                _ => unreachable!(),
            };
            result.push(c);
        }

        (result, self.market_type)
    }

    pub fn display_symbol_and_type(&self) -> (String, MarketType) {
        let mut result = String::with_capacity(self.len as usize);

        for i in 0..self.len {
            let value = (self.data[i as usize / 10] >> ((i % 10) * 6)) & 0x3F;

            if value == 36 {
                break;
            }

            let c = match value {
                0..=9 => (b'0' + value as u8) as char,
                10..=35 => (b'A' + (value as u8 - 10)) as char,
                _ => unreachable!(),
            };
            result.push(c);
        }

        (result, self.market_type)
    }

    pub fn get_market_type(&self) -> MarketType {
        self.market_type
    }
}

impl fmt::Display for Ticker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for i in 0..self.len {
            let value = (self.data[i as usize / 10] >> ((i % 10) * 6)) & 0x3F;
            let c = match value {
                0..=9 => (b'0' + value as u8) as char,
                10..=35 => (b'A' + (value as u8 - 10)) as char,
                36 => '_',
                _ => unreachable!(),
            };
            f.write_char(c)?;
        }

        Ok(())
    }
}

impl From<(String, MarketType)> for Ticker {
    fn from((s, market_type): (String, MarketType)) -> Self {
        Ticker::new(s, market_type)
    }
}

impl From<(&str, MarketType)> for Ticker {
    fn from((s, market_type): (&str, MarketType)) -> Self {
        Ticker::new(s, market_type)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
pub struct TickerInfo {
    pub ticker: Ticker,
    #[serde(rename = "tickSize")]
    pub min_ticksize: f32,
}

impl TickerInfo {
    pub fn get_market_type(&self) -> MarketType {
        self.ticker.market_type
    }

    pub fn is_perps(&self) -> bool {
        self.ticker.market_type == MarketType::LinearPerps
            || self.ticker.market_type == MarketType::InversePerps
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Trade {
    pub time: u64,
    #[serde(deserialize_with = "bool_from_int")]
    pub is_sell: bool,
    pub price: f32,
    pub qty: f32,
}

#[derive(Debug, Clone, Copy)]
pub struct Kline {
    pub time: u64,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub volume: (f32, f32),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct TickerStats {
    pub mark_price: f32,
    pub daily_price_chg: f32,
    pub daily_volume: f32,
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value.as_i64() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => Err(serde::de::Error::custom("expected 0 or 1")),
    }
}

fn de_string_to_f32<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<f32>().map_err(serde::de::Error::custom)
}

fn de_string_to_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<u64>().map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpenInterest {
    pub time: u64,
    pub value: f32,
}

fn str_f32_parse(s: &str) -> f32 {
    s.parse::<f32>().unwrap_or_else(|e| {
        log::error!("Failed to parse float: {}, error: {}", s, e);
        0.0
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct TickMultiplier(pub u16);

impl std::fmt::Display for TickMultiplier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}x", self.0)
    }
}

impl TickMultiplier {
    pub const ALL: [TickMultiplier; 8] = [
        TickMultiplier(1),
        TickMultiplier(2),
        TickMultiplier(5),
        TickMultiplier(10),
        TickMultiplier(25),
        TickMultiplier(50),
        TickMultiplier(100),
        TickMultiplier(200),
    ];

    /// Returns the final tick size after applying the user selected multiplier
    ///
    /// Usually used for price steps in chart scales
    pub fn multiply_with_min_tick_size(&self, ticker_info: TickerInfo) -> f32 {
        let min_tick_size = ticker_info.min_ticksize;

        let multiplier = if let Some(m) = Decimal::from_f32(f32::from(self.0)) {
            m
        } else {
            log::error!("Failed to convert multiplier: {}", self.0);
            return f32::from(self.0) * min_tick_size;
        };

        let decimal_min_tick_size = if let Some(d) = Decimal::from_f32(min_tick_size) {
            d
        } else {
            log::error!("Failed to convert min_tick_size: {}", min_tick_size);
            return f32::from(self.0) * min_tick_size;
        };

        let normalized = multiplier * decimal_min_tick_size.normalize();
        if let Some(tick_size) = normalized.to_f32() {
            let decimal_places = calculate_decimal_places(min_tick_size);
            round_to_decimal_places(tick_size, decimal_places)
        } else {
            log::error!("Failed to calculate tick size for multiplier: {}", self.0);
            f32::from(self.0) * min_tick_size
        }
    }
}

// ticksize rounding helpers
fn calculate_decimal_places(value: f32) -> u32 {
    let s = value.to_string();
    if let Some(decimal_pos) = s.find('.') {
        (s.len() - decimal_pos - 1) as u32
    } else {
        0
    }
}
fn round_to_decimal_places(value: f32, places: u32) -> f32 {
    let factor = 10.0f32.powi(places as i32);
    (value * factor).round() / factor
}
