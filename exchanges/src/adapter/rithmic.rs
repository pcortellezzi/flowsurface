use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::Duration;

use iced_futures::{
    futures::{SinkExt, Stream},
    stream,
};
use iced_futures::futures::future::UnwrapOrElse;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::sleep;

use super::{
    super::{
        Exchange, Kline, MarketType, OpenInterest, StreamType, Ticker, TickerInfo, TickerStats,
        Timeframe, Trade,
        depth::{LocalDepthCache, Order, TempLocalDepth},
        str_f32_parse,
    },
    Connection, Event, StreamError,
};

use rithmic_client::{
    api::RithmicConnectionInfo,
    plants::{
        history_plant::{
            RithmicHistoryPlant,
            RithmicHistoryPlantHandle,
        },
        ticker_plant::{
            RithmicTickerPlant,
            RithmicTickerPlantHandle,
        },
    },
    rti::{
        *,
        messages::RithmicMessage,
    },
    ws::RithmicStream,
};

// static TICKER_PLANT: LazyLock<RithmicTickerPlant> = LazyLock::new(|| get_ticker_plant());
// static HISTORY_PLANT: LazyLock<RithmicHistoryPlant> = LazyLock::new(|| get_history_plant());
pub static RTI_CONNECTOR: LazyLock<RithmicConnector> = LazyLock::new(|| RithmicConnector::new());

#[derive(Clone, Default)]
pub struct RithmicConnector {
    rithmic_connection_info: RithmicConnectionInfo,
    ticker_plant: Option<Arc<RithmicTickerPlant>>,
    ticker_plant_handle: Option<RithmicTickerPlantHandle>,
    history_plant: Option<Arc<RithmicHistoryPlant>>,
    history_plant_handle: Option<RithmicHistoryPlantHandle>,
}

impl RithmicConnector {
    fn new() -> RithmicConnector {
        let mut rti_connector = RithmicConnector::default();
        rti_connector.load_rithmic_config();
        rti_connector.connect_ticker_plant();
        rti_connector.connect_history_plant();
        rti_connector
    }

    fn load_rithmic_config(&mut self) {
        let data_dir = dirs_next::config_dir().unwrap_or_else(|| PathBuf::from("."));
        let path = data_dir.join("flowsurface/rithmic.json");
        let mut file = File::open(path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        self.rithmic_connection_info = serde_json::from_str(&contents).unwrap()
    }

    fn connect_ticker_plant(&mut self) {
        let (tx, mut rx) = oneshot::channel::<RithmicTickerPlant>();
        let rc_inf = self.rithmic_connection_info.clone();
        thread::spawn(move || {
            Runtime::new().unwrap().block_on(async {
                let ticker_plant = RithmicTickerPlant::new(&rc_inf).await;
                let ticker_plant_handle = ticker_plant.get_handle();
                let _ = ticker_plant_handle.login().await;
                let _ = tx.send(ticker_plant);
                loop {sleep(Duration::from_secs(1)).await;}
            });
        });
        loop {
            match rx.try_recv() {
                Ok(ticker_plant) => {
                    self.ticker_plant_handle = Some(ticker_plant.get_handle());
                    self.ticker_plant = Some(Arc::new(ticker_plant));
                    return
                }
                _ => {}
            }
        }
    }

    pub fn get_ticker_handle(&self) -> RithmicTickerPlantHandle{
        self.ticker_plant_handle.clone().unwrap()
    }

    pub fn get_history_handle(&self) -> RithmicHistoryPlantHandle{
        self.history_plant_handle.clone().unwrap()
    }

    fn connect_history_plant(&mut self) {
        let (tx, mut rx) = oneshot::channel::<RithmicHistoryPlant>();
        let rc_inf = self.rithmic_connection_info.clone();
        thread::spawn(move || {
            Runtime::new().unwrap().block_on(async {
                let history_plant = RithmicHistoryPlant::new(&rc_inf).await;
                let history_plant_handle = history_plant.get_handle();
                let _ = history_plant_handle.login().await;
                let _ = tx.send(history_plant);
                loop {sleep(Duration::from_secs(1)).await;}
            });
        });
        loop {
            match rx.try_recv() {
                Ok(history_plant) => {
                    self.history_plant_handle = Some(history_plant.get_handle());
                    self.history_plant = Some(Arc::new(history_plant));
                    return
                }
                _ => {}
            }
        }
    }

    pub fn connect_market_stream(&self, ticker: Ticker) -> impl Stream<Item = Event> {
        stream::channel(100, async move |mut output| {
            let mut state = State::Disconnected;

            let (ticker_str, _) = ticker.get_string();
            let Some((symbol_str, exchange_str)) = ticker_str.split_once(":") else { todo!() };

            let exchange = Exchange::Rithmic;

            let mut orderbook: LocalDepthCache = LocalDepthCache::new();
            let mut trades_buffer: Vec<Trade> = Vec::new();

            loop {
                match &mut state {
                    State::Disconnected => {
                        let ticker_plant_handle = self.get_ticker_handle();
                        if let Ok(_) = ticker_plant_handle.subscribe(
                            symbol_str,
                            exchange_str,
                            vec![
                                request_market_data_update::UpdateBits::LastTrade,
                                // request_market_data_update::UpdateBits::Bbo,
                                request_market_data_update::UpdateBits::OrderBook],
                        ).await {
                            state = State::TickerConnected(ticker_plant_handle);
                            let _ = output.send(Event::Connected(exchange, Connection)).await;
                        } else {
                            sleep(Duration::from_secs(1)).await;
                            let _ = output
                                .send(Event::Disconnected(
                                    exchange,
                                    "Failed to connect to websocket".to_string(),
                                ))
                                .await;
                        }
                    }
                    State::TickerConnected(ticker_plant_handle) => {
                        if let Ok(rti_response) = ticker_plant_handle.subscription_receiver.recv().await {
                            match rti_response.message {
                                RithmicMessage::LastTrade(last_trade) => {
                                    if last_trade.presence_bits() & request_market_data_update::UpdateBits::LastTrade as u32 != 0
                                        && symbol_str == last_trade.symbol() && exchange_str == last_trade.exchange() {
                                        let trade = Trade {
                                            time: last_trade.source_ssboe() as u64 * 1000 + last_trade.source_usecs() as u64 / 1000,
                                            is_sell: last_trade.aggressor() == last_trade::TransactionType::Sell,
                                            price: last_trade.trade_price() as f32,
                                            qty: last_trade.trade_size() as f32,
                                        };
                                        trades_buffer.push(trade);
                                    }
                                }
                                RithmicMessage::OrderBook(order_book) => {
                                    if symbol_str == order_book.symbol() && exchange_str == order_book.exchange() {
                                        let last_update_id = orderbook.get_fetch_id();
                                        let depth_local_cache = TempLocalDepth {
                                            last_update_id: order_book.ssboe() as u64 * 1000 + order_book.usecs() as u64 / 1000,
                                            time: order_book.ssboe() as u64 * 1000 + order_book.usecs() as u64 / 1000,
                                            bids: order_book.bid_size.iter().enumerate().map(|(i, x)| Order {
                                                price: order_book.bid_price[i] as f32,
                                                qty: *x as f32
                                            }).collect(),
                                            asks: order_book.ask_size.iter().enumerate().map(|(i, x)| Order {
                                                price: order_book.ask_price[i] as f32,
                                                qty: *x as f32
                                            }).collect(),
                                        };
                                        if order_book.update_type() == order_book::UpdateType::SnapshotImage {
                                            orderbook.fetched(&depth_local_cache);
                                        } else if order_book.update_type() == order_book::UpdateType::Solo {
                                            orderbook.update_depth_cache(&depth_local_cache);
                                        }
                                        let _ = output
                                            .send(Event::DepthReceived(
                                                StreamType::DepthAndTrades { exchange, ticker },
                                                order_book.ssboe() as u64 * 1000 + order_book.usecs() as u64 / 1000,
                                                orderbook.get_depth(),
                                                std::mem::take(&mut trades_buffer).into_boxed_slice(),
                                            ))
                                            .await;
                                    }
                                }
                                RithmicMessage::DepthByOrder(depth_by_order) => {
                                    // log::warn!("{:?}", depth_by_order);
                                }
                                _ => {
                                    log::warn!("message not handled: {:?}", rti_response.message);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
    }

    pub fn connect_kline_stream(&self,
        streams: Vec<(Ticker, Timeframe)>,
        market: MarketType,
    ) -> impl Stream<Item = Event> {
        stream::channel(100, async move |mut output| {
            let mut state = State::Disconnected;

            let exchange = Exchange::Rithmic;

            loop {
                match &mut state {
                    State::Disconnected => {
                        let history_plant_handle = self.get_history_handle();
                        for (ticker, time_frame) in streams.clone() {
                            let (ticker_str, _) = ticker.get_string();
                            let (bar_type, period) = match time_frame.to_string().chars().nth_back(0).unwrap() {
                                's' => (request_time_bar_update::BarType::SecondBar, time_frame.to_seconds()),
                                'm' => (request_time_bar_update::BarType::MinuteBar, time_frame.to_minutes()),
                                'h' => (request_time_bar_update::BarType::MinuteBar, time_frame.to_minutes()),
                                _ => (request_time_bar_update::BarType::WeeklyBar, time_frame.to_minutes()),
                            };
                            log::warn!("{:?}:{:?}", bar_type, period);
                            let Some((symbol_str, exchange_str)) = ticker_str.split_once(":") else { todo!() };
                            let rep = history_plant_handle.subscribe_time_bar(
                                symbol_str, exchange_str,
                                bar_type, period as i32,
                            ).await.unwrap();
                            let RithmicMessage::ResponseTimeBarUpdate(res) = rep.message else { todo!() };
                            log::warn!("{:?}", res);
                        }
                        state = State::HistoryConnected(history_plant_handle);
                        let _ = output.send(Event::Connected(exchange, Connection)).await;
                    }
                    State::HistoryConnected(history_plant_handle) => {
                        if let Ok(rti_response) = history_plant_handle.subscription_receiver.recv().await {
                            match rti_response.message {
                                RithmicMessage::TimeBar(time_bar) => {
                                    log::info!("{:?}", time_bar);
                                    let kline = Kline {
                                        time: time_bar.marker() as u64 * 1000,
                                        open: time_bar.open_price() as f32,
                                        high: time_bar.high_price() as f32,
                                        low: time_bar.low_price() as f32,
                                        close: time_bar.close_price() as f32,
                                        volume: (time_bar.bid_volume() as f32, time_bar.ask_volume() as f32),
                                    };
                                    if let Some((ticker, timeframe)) = streams
                                        .iter()
                                        .find(|(_, tf)| {
                                            // let period = time_bar.period().parse::<u16>().unwrap();
                                            // log::warn!("{:?}", time_bar);
                                            // match time_bar.r#type() {
                                            //     time_bar::BarType::SecondBar => tf.to_seconds() == period,
                                            //     time_bar::BarType::MinuteBar => tf.to_minutes() == period,
                                            //     _ => { false }
                                            // }
                                            tf.to_seconds() == time_bar.period().parse::<u16>().unwrap()
                                        })
                                    {
                                        let _ = output
                                            .send(Event::KlineReceived(
                                                StreamType::Kline {
                                                    exchange,
                                                    ticker: *ticker,
                                                    timeframe: *timeframe
                                                },
                                                kline,
                                            ))
                                            .await;
                                    }
                                }
                                _ => {
                                    log::warn!("message not handled: {:?}", rti_response.message);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
    }

    pub async fn fetch_historical_oi(&self,
        ticker: Ticker,
        range: Option<(u64, u64)>,
        period: Timeframe,
    ) -> Result<Vec<OpenInterest>, StreamError> {
        Ok(vec![])
    }

    pub async fn fetch_klines(&self,
        ticker: Ticker,
        timeframe: Timeframe,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<Kline>, StreamError> {
        let history_plant_handle = self.get_history_handle();

        let (ticker_str, _) = ticker.get_string();
        let Some((symbol_str, exchange_str)) = ticker_str.split_once(":") else { todo!() };

        let (bar_type, period) = match timeframe.to_string().chars().nth_back(0).unwrap() {
            's' => (request_time_bar_replay::BarType::SecondBar, timeframe.to_seconds()),
            'm' => (request_time_bar_replay::BarType::MinuteBar, timeframe.to_minutes()),
            'h' => (request_time_bar_replay::BarType::MinuteBar, timeframe.to_minutes()),
            _ => (request_time_bar_replay::BarType::WeeklyBar, timeframe.to_minutes()),
        };

        let(start, end) = if let Some((start, end)) = range {
            (start, end)
        } else {
            let now = chrono::Utc::now().timestamp() as u64;
            (now - 1000, now)
        };

        let time_bars = history_plant_handle.get_historical_time_bar(
            symbol_str.to_string(),
            exchange_str.to_string(),
            bar_type,
            period as i32,
            start as i32,
            end as i32,
            request_time_bar_replay::Direction::First,
            request_time_bar_replay::TimeOrder::Backwards,
        ).await.unwrap();

        let mut klines = vec![];
        for time_bar in time_bars {
            if time_bar.has_more {
                if let RithmicMessage::ResponseTimeBarReplay(msg) = time_bar.message {
                    klines.push(Kline {
                        time: msg.marker() as u64 * 1000,
                        open: msg.open_price() as f32,
                        high: msg.high_price() as f32,
                        low: msg.low_price() as f32,
                        close: msg.close_price() as f32,
                        volume: (msg.bid_volume() as f32, msg.ask_volume() as f32),
                    });
                }
            }
        }
        Ok(klines)
    }

    pub async fn fetch_ticksize(&self,
        market_type: MarketType
    ) -> Result<HashMap<Ticker, Option<TickerInfo>>, StreamError> {
        Ok(HashMap::from([
            (Ticker::new("NQM5:CME", MarketType::Futures),
             Some(TickerInfo {
                 ticker: Ticker::new("NQM5:CME", MarketType::Futures),
                 min_ticksize: 0.25
             })),
            (Ticker::new("ESM5:CME", MarketType::Futures),
             Some(TickerInfo {
                 ticker: Ticker::new("ESM5:CME", MarketType::Futures),
                 min_ticksize: 0.25
             })),
        ]))
    }

    pub async fn fetch_ticker_prices(&self,
        market: MarketType
    ) -> Result<HashMap<Ticker, TickerStats>, StreamError> {
        let ticker_plant_handle = self.get_ticker_handle();
        let symbols = ticker_plant_handle.search_symbols(
            Some("CME".to_string()),
            Some(request_search_symbols::InstrumentType::Future),
            Some(true)
        ).await.unwrap();
        let _ = ticker_plant_handle.disconnect();
        let mut hash_map: HashMap<Ticker, TickerStats> = HashMap::new();
        for symbol in symbols {
            if let RithmicMessage::ResponseSearchSymbols(response_search_symbol) = symbol.message {
                hash_map.insert(
                    Ticker::new(
                        format!("{}:{}",
                                response_search_symbol.symbol(),
                                response_search_symbol.exchange()),
                        MarketType::Futures
                    ),
                    TickerStats {
                        mark_price: 0.0,
                        daily_price_chg: 0.0,
                        daily_volume: 0.0
                    }
                );
            }
        }
        Ok(hash_map)
    }

    pub async fn fetch_trades(
        ticker: Ticker,
        from_time: u64,
        data_path: PathBuf,
    ) -> Result<Vec<Trade>, StreamError> {
        Ok(vec![])
    }
}

enum State {
    Disconnected,
    // OrderConnected(<RithmicOrderPlant as RithmicStream>::Handle),
    // PnlConnected(<RithmicPnlPlant as RithmicStream>::Handle),
    TickerConnected(<RithmicTickerPlant as RithmicStream>::Handle),
    HistoryConnected(<RithmicHistoryPlant as RithmicStream>::Handle),
}


