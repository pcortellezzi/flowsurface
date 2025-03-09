pub mod vendor;

use std::{collections::HashMap, io::BufReader};

use futures::{SinkExt, Stream};
use iced_futures::stream;

use super::{
    de_string_to_f32, setup_tcp_connection, setup_tls_connection, setup_websocket_connection, str_f32_parse,
    Connection, Event, Exchange, Kline, LocalDepthCache, MarketType, OpenInterest, Order, State,
    StreamError, StreamType, Ticker, TickerInfo, TickerStats, Timeframe, Trade, VecLocalDepthCache
};

pub fn connect_market_stream(ticker: Ticker) -> impl Stream<Item = Event> {
    stream::channel(100, async move |mut output| { 
        let _ = output.send(Event::Connected(Exchange::Rithmic, Connection)).await;
    })
}

pub fn connect_kline_stream(
    streams: Vec<(Ticker, Timeframe)>,
    market: MarketType,
) -> impl Stream<Item = super::Event> {
    stream::channel(100, async move |mut output| {
        let _ = output.send(Event::Connected(Exchange::Rithmic, Connection)).await;
    })
}

pub async fn fetch_historical_oi(
    ticker: Ticker,
    range: Option<(u64, u64)>,
    period: Timeframe,
) -> Result<Vec<OpenInterest>, StreamError> {
    Ok(vec![])
}

pub async fn fetch_klines(
    ticker: Ticker,
    timeframe: Timeframe,
    range: Option<(u64, u64)>,
) -> Result<Vec<Kline>, StreamError> {
    Ok(vec![])
}

pub async fn fetch_ticksize(market_type: MarketType) -> Result<HashMap<Ticker, Option<TickerInfo>>, StreamError> {
    Ok(HashMap::from([
        (Ticker::new("NQH5", MarketType::LinearPerps),
         Some(TickerInfo { 
             ticker: Ticker::new("NQH5", MarketType::LinearPerps),
             min_ticksize: 0.25
         }))
    ]))
}

pub async fn fetch_ticker_prices(market: MarketType) -> Result<HashMap<Ticker, TickerStats>, StreamError> {
    Ok(HashMap::from([
        (Ticker::new("NQH5", MarketType::LinearPerps),
         TickerStats {
             mark_price: 20000.0,
             daily_price_chg: 0.1,
             daily_volume: 10.0
         })    
    ]))
}

pub async fn fetch_trades(
    ticker: Ticker,
    from_time: u64,
) -> Result<Vec<Trade>, StreamError> {
    Ok(vec![])
}
