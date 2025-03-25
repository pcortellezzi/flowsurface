use std::collections::HashMap;

use crate::style::{self, ICON_FONT, Icon, get_icon_text};
use exchanges::{
    Ticker, TickerStats,
    adapter::{Exchange, MarketType},
};
use iced::{
    Element, Length, Renderer, Size, Task, Theme,
    alignment::{self, Horizontal, Vertical},
    padding,
    widget::{
        Button, Column, Container, Space, Text, button, column, container, row,
        scrollable::{self, AbsoluteOffset},
        text, text_input,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub enum TickerTab {
    All,
    Bybit,
    Binance,
    Rithmic,
    Favorites,
}

#[derive(Clone)]
struct TickerDisplayData {
    display_ticker: String,
    price_change_display: String,
    volume_display: String,
    mark_price_display: String,
    card_color_alpha: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOptions {
    VolumeAsc,
    VolumeDesc,
    ChangeAsc,
    ChangeDesc,
}

#[derive(Debug, Clone)]
pub enum Message {
    ChangeTickersTableTab(TickerTab),
    UpdateSearchQuery(String),
    ChangeSortOption(SortOptions),
    ShowSortingOptions,
    TickerSelected(Ticker, Exchange, String),
    ExpandTickerCard(Option<(Ticker, Exchange)>),
    FavoriteTicker(Exchange, Ticker),
    Scrolled(scrollable::Viewport),
    SetMarketFilter(Option<MarketType>),
}

pub struct TickersTable {
    tickers_info: HashMap<Exchange, Vec<(Ticker, TickerStats)>>,
    combined_tickers: Vec<(Exchange, Ticker, TickerStats, bool)>,
    favorited_tickers: Vec<(Exchange, Ticker)>,
    display_cache: HashMap<(Exchange, Ticker), TickerDisplayData>,
    selected_tab: TickerTab,
    search_query: String,
    show_sort_options: bool,
    selected_sort_option: SortOptions,
    selected_market: Option<MarketType>,
    expand_ticker_card: Option<(Ticker, Exchange)>,
    scroll_offset: AbsoluteOffset,
    is_show: bool,
}

impl TickersTable {
    pub fn new(favorited_tickers: Vec<(Exchange, Ticker)>) -> Self {
        Self {
            tickers_info: HashMap::new(),
            combined_tickers: Vec::new(),
            display_cache: HashMap::new(),
            favorited_tickers,
            selected_tab: TickerTab::All,
            search_query: String::new(),
            show_sort_options: false,
            selected_sort_option: SortOptions::VolumeDesc,
            expand_ticker_card: None,
            scroll_offset: AbsoluteOffset::default(),
            selected_market: None,
            is_show: false,
        }
    }

    pub fn update_table(&mut self, exchange: Exchange, tickers_info: HashMap<Ticker, TickerStats>) {
        self.display_cache.retain(|(ex, _), _| ex != &exchange);

        let tickers_vec: Vec<_> = tickers_info
            .into_iter()
            .map(|(ticker, stats)| {
                self.display_cache.insert(
                    (exchange, ticker),
                    Self::compute_display_data(&ticker, &stats),
                );
                (ticker, stats)
            })
            .collect();

        self.tickers_info.insert(exchange, tickers_vec);
        self.update_combined_tickers();
    }

    fn update_combined_tickers(&mut self) {
        self.combined_tickers.clear();

        self.tickers_info.iter().for_each(|(exchange, tickers)| {
            for (ticker, stats) in tickers {
                let is_fav = self
                    .favorited_tickers
                    .iter()
                    .any(|(ex, tick)| ex == exchange && tick == ticker);
                self.combined_tickers
                    .push((*exchange, *ticker, *stats, is_fav));
            }
        });

        match self.selected_sort_option {
            SortOptions::VolumeDesc => {
                self.combined_tickers
                    .sort_by(|a: &(Exchange, Ticker, TickerStats, bool), b| {
                        b.2.daily_volume
                            .partial_cmp(&a.2.daily_volume)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
            }
            SortOptions::VolumeAsc => {
                self.combined_tickers
                    .sort_by(|a: &(Exchange, Ticker, TickerStats, bool), b| {
                        a.2.daily_volume
                            .partial_cmp(&b.2.daily_volume)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
            }
            SortOptions::ChangeDesc => {
                self.combined_tickers
                    .sort_by(|a: &(Exchange, Ticker, TickerStats, bool), b| {
                        b.2.daily_price_chg
                            .partial_cmp(&a.2.daily_price_chg)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
            }
            SortOptions::ChangeAsc => {
                self.combined_tickers
                    .sort_by(|a: &(Exchange, Ticker, TickerStats, bool), b| {
                        a.2.daily_price_chg
                            .partial_cmp(&b.2.daily_price_chg)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
            }
        }
    }

    fn change_sort_option(&mut self, option: SortOptions) {
        if self.selected_sort_option == option {
            self.selected_sort_option = match self.selected_sort_option {
                SortOptions::VolumeDesc => SortOptions::VolumeAsc,
                SortOptions::VolumeAsc => SortOptions::VolumeDesc,
                SortOptions::ChangeDesc => SortOptions::ChangeAsc,
                SortOptions::ChangeAsc => SortOptions::ChangeDesc,
            };
        } else {
            self.selected_sort_option = option;
        }

        self.update_combined_tickers();
    }

    fn favorite_ticker(&mut self, exchange: Exchange, ticker: Ticker) {
        for (ex, tick, _, is_fav) in &mut self.combined_tickers {
            if ex == &exchange && tick == &ticker {
                *is_fav = !*is_fav;
            }
        }

        self.favorited_tickers = self
            .combined_tickers
            .iter()
            .filter(|(_, _, _, is_fav)| *is_fav)
            .map(|(exchange, ticker, _, _)| (*exchange, *ticker))
            .collect();
    }

    pub fn get_favorited_tickers(&self) -> Vec<(Exchange, Ticker)> {
        self.combined_tickers
            .iter()
            .filter(|(_, _, _, is_fav)| *is_fav)
            .map(|(exchange, ticker, _, _)| (*exchange, *ticker))
            .collect()
    }

    fn compute_display_data(ticker: &Ticker, stats: &TickerStats) -> TickerDisplayData {
        let (ticker_str, market) = ticker.get_string();
        let display_ticker = if ticker_str.len() >= 11 {
            ticker_str[..9].to_string() + "..."
        } else {
            ticker_str + {
                match market {
                    MarketType::Spot => "",
                    MarketType::LinearPerps => "P",
                }
            }
        };

        TickerDisplayData {
            display_ticker,
            price_change_display: convert_to_pct_change(stats.daily_price_chg).to_string(),
            volume_display: convert_to_currency_abbr(stats.daily_volume).to_string(),
            mark_price_display: stats.mark_price.to_string(),
            card_color_alpha: { (stats.daily_price_chg / 8.0).clamp(-1.0, 1.0) },
        }
    }

    fn matches_exchange(ex: &Exchange, tab: &TickerTab) -> bool {
        match tab {
            TickerTab::Bybit => matches!(ex, Exchange::BybitLinear | Exchange::BybitSpot),
            TickerTab::Binance => matches!(ex, Exchange::BinanceFutures | Exchange::BinanceSpot),
            TickerTab::Rithmic => matches!(ex, Exchange::Rithmic),
            _ => false,
        }
    }

    fn create_ticker_container<'a>(
        &'a self,
        is_visible: bool,
        exchange: Exchange,
        ticker: &'a Ticker,
        is_fav: bool,
    ) -> Container<'a, Message> {
        if !is_visible {
            return container(column![].width(Length::Fill).height(Length::Fixed(60.0)));
        }

        let display_data = &self.display_cache[&(exchange, *ticker)];

        container(
            if let Some((selected_ticker, selected_exchange)) = &self.expand_ticker_card {
                if ticker == selected_ticker && exchange == *selected_exchange {
                    create_expanded_ticker_card(&exchange, ticker, display_data, is_fav)
                } else {
                    create_ticker_card(&exchange, ticker, display_data)
                }
            } else {
                create_ticker_card(&exchange, ticker, display_data)
            },
        )
        .style(style::ticker_card)
    }

    fn is_container_visible(&self, index: usize, bounds: Size) -> bool {
        let ticker_container_height = 64.0;
        let base_search_bar_height = 120.0;

        let item_top = base_search_bar_height + (index as f32 * ticker_container_height);
        let item_bottom = item_top + ticker_container_height;

        (item_bottom >= (self.scroll_offset.y - (2.0 * ticker_container_height)))
            && (item_top
                <= (self.scroll_offset.y + bounds.height + (2.0 * ticker_container_height)))
    }

    pub fn is_open(&self) -> bool {
        self.is_show
    }

    pub fn toggle_table(&mut self) {
        self.is_show = !self.is_show;
    }

    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::ChangeTickersTableTab(tab) => {
                self.selected_tab = tab;
            }
            Message::UpdateSearchQuery(query) => {
                self.search_query = query.to_uppercase();
            }
            Message::ChangeSortOption(option) => {
                self.change_sort_option(option);
            }
            Message::ShowSortingOptions => {
                self.show_sort_options = !self.show_sort_options;
            }
            Message::ExpandTickerCard(is_ticker) => {
                self.expand_ticker_card = is_ticker;
            }
            Message::FavoriteTicker(exchange, ticker) => {
                self.favorite_ticker(exchange, ticker);
            }
            Message::Scrolled(viewport) => {
                self.scroll_offset = viewport.absolute_offset();
            }
            Message::SetMarketFilter(market) => {
                if self.selected_market == market {
                    self.selected_market = None;
                } else {
                    self.selected_market = market;
                }
            }
            _ => {}
        }
        Task::none()
    }

    pub fn view(&self, bounds: Size) -> Element<'_, Message> {
        let all_button = create_tab_button(text("ALL"), &self.selected_tab, TickerTab::All);
        let bybit_button = create_tab_button(text("Bybit"), &self.selected_tab, TickerTab::Bybit);
        let binance_button =
            create_tab_button(text("Binance"), &self.selected_tab, TickerTab::Binance);
        let rithmic_button =
            create_tab_button(text("Rithmic"), &self.selected_tab, TickerTab::Rithmic);
        let favorites_button = create_tab_button(
            text(char::from(Icon::StarFilled).to_string())
                .font(ICON_FONT)
                .width(11),
            &self.selected_tab,
            TickerTab::Favorites,
        );

        let spot_market_button = button(text("Spot"))
            .on_press(Message::SetMarketFilter(Some(MarketType::Spot)))
            .style(move |theme, status| style::button_transparent(theme, status, false));

        let perp_market_button = button(text("Linear Perps"))
            .on_press(Message::SetMarketFilter(Some(MarketType::LinearPerps)))
            .style(move |theme, status| style::button_transparent(theme, status, false));

        let show_sorting_button = button(get_icon_text(Icon::Sort, 14).align_x(Horizontal::Center))
            .on_press(Message::ShowSortingOptions);

        let volume_sort_button = button(
            row![
                text("Volume"),
                get_icon_text(
                    if self.selected_sort_option == SortOptions::VolumeDesc {
                        Icon::SortDesc
                    } else {
                        Icon::SortAsc
                    },
                    14
                )
            ]
            .spacing(4)
            .align_y(Vertical::Center),
        )
        .on_press(Message::ChangeSortOption(SortOptions::VolumeAsc));

        let change_sort_button = button(
            row![
                text("Change"),
                get_icon_text(
                    if self.selected_sort_option == SortOptions::ChangeDesc {
                        Icon::SortDesc
                    } else {
                        Icon::SortAsc
                    },
                    14
                )
            ]
            .spacing(4)
            .align_y(Vertical::Center),
        )
        .on_press(Message::ChangeSortOption(SortOptions::ChangeAsc));

        let mut content = column![
            row![
                text_input("Search for a ticker...", &self.search_query)
                    .style(style::search_input)
                    .on_input(Message::UpdateSearchQuery)
                    .align_x(Horizontal::Left),
                if self.show_sort_options {
                    show_sorting_button
                        .style(move |theme, status| style::button_transparent(theme, status, true))
                } else {
                    show_sorting_button
                        .style(move |theme, status| style::button_transparent(theme, status, false))
                }
            ]
            .align_y(Vertical::Center)
            .spacing(4),
            if self.show_sort_options {
                container(
                    column![
                        row![
                            Space::new(Length::FillPortion(2), Length::Shrink),
                            match self.selected_sort_option {
                                SortOptions::VolumeAsc | SortOptions::VolumeDesc =>
                                    volume_sort_button.style(move |theme, status| {
                                        style::button_transparent(theme, status, true)
                                    }),
                                _ => volume_sort_button.style(move |theme, status| {
                                    style::button_transparent(theme, status, false)
                                }),
                            },
                            Space::new(Length::FillPortion(1), Length::Shrink),
                            match self.selected_sort_option {
                                SortOptions::ChangeAsc | SortOptions::ChangeDesc =>
                                    change_sort_button.style(move |theme, status| {
                                        style::button_transparent(theme, status, true)
                                    }),
                                _ => change_sort_button.style(move |theme, status| {
                                    style::button_transparent(theme, status, false)
                                }),
                            },
                            Space::new(Length::FillPortion(2), Length::Shrink),
                        ],
                        row![
                            Space::new(Length::FillPortion(1), Length::Shrink),
                            match self.selected_market {
                                Some(MarketType::Spot) =>
                                    spot_market_button.style(move |theme, status| {
                                        style::button_transparent(theme, status, true)
                                    }),
                                _ => spot_market_button.style(move |theme, status| {
                                    style::button_transparent(theme, status, false)
                                }),
                            },
                            Space::new(Length::FillPortion(1), Length::Shrink),
                            match self.selected_market {
                                Some(MarketType::LinearPerps) =>
                                    perp_market_button.style(move |theme, status| {
                                        style::button_transparent(theme, status, true)
                                    }),
                                _ => perp_market_button.style(move |theme, status| {
                                    style::button_transparent(theme, status, false)
                                }),
                            },
                            Space::new(Length::FillPortion(1), Length::Shrink),
                        ],
                    ]
                    .spacing(4),
                )
                .padding(4)
                .style(style::sorter_container)
            } else {
                container(column![])
            },
            row![
                favorites_button,
                Space::new(Length::FillPortion(1), Length::Shrink),
                all_button,
                Space::new(Length::FillPortion(1), Length::Shrink),
                bybit_button,
                Space::new(Length::FillPortion(1), Length::Shrink),
                binance_button,
                Space::new(Length::FillPortion(1), Length::Shrink),
                rithmic_button,
            ]
            .padding(padding::bottom(4)),
        ]
        .spacing(4)
        .padding(padding::right(8))
        .width(Length::Fill);

        match self.selected_tab {
            TickerTab::All => {
                content =
                    self.combined_tickers
                        .iter()
                        .filter(|(_, ticker, _, _)| {
                            let (ticker, market) = ticker.get_string();
                            ticker.contains(&self.search_query)
                                && match self.selected_market {
                                    Some(market_type) => market == market_type,
                                    None => true,
                                }
                        })
                        .enumerate()
                        .fold(
                            content,
                            |content, (index, (exchange, ticker, _, is_fav))| {
                                let is_visible = self.is_container_visible(index, bounds);
                                content.push(self.create_ticker_container(
                                    is_visible, *exchange, ticker, *is_fav,
                                ))
                            },
                        );
            }
            TickerTab::Favorites => {
                content =
                    self.combined_tickers
                        .iter()
                        .filter(|(_, ticker, _, is_fav)| {
                            let (ticker, market) = ticker.get_string();
                            *is_fav
                                && ticker.contains(&self.search_query)
                                && match self.selected_market {
                                    Some(market_type) => market == market_type,
                                    None => true,
                                }
                        })
                        .enumerate()
                        .fold(
                            content,
                            |content, (index, (exchange, ticker, _, is_fav))| {
                                let is_visible = self.is_container_visible(index, bounds);
                                content.push(self.create_ticker_container(
                                    is_visible, *exchange, ticker, *is_fav,
                                ))
                            },
                        );
            }
            _ => {
                content = self
                    .combined_tickers
                    .iter()
                    .filter(|(ex, ticker, _, _)| {
                        let (ticker, market) = ticker.get_string();
                        Self::matches_exchange(ex, &self.selected_tab)
                            && ticker.contains(&self.search_query)
                            && match self.selected_market {
                                Some(market_type) => market == market_type,
                                None => true,
                            }
                    })
                    .enumerate()
                    .fold(content, |content, (index, (ex, ticker, _, is_fav))| {
                        let is_visible = self.is_container_visible(index, bounds);
                        content.push(self.create_ticker_container(is_visible, *ex, ticker, *is_fav))
                    });
            }
        }

        scrollable::Scrollable::with_direction(
            content,
            scrollable::Direction::Vertical(
                scrollable::Scrollbar::new().width(8).scroller_width(6),
            ),
        )
        .on_scroll(Message::Scrolled)
        .style(style::scroll_bar)
        .into()
    }
}

fn create_ticker_card<'a>(
    exchange: &Exchange,
    ticker: &Ticker,
    display_data: &'a TickerDisplayData,
) -> Column<'a, Message> {
    let color_column = container(column![])
        .height(Length::Fill)
        .width(Length::Fixed(2.0))
        .style(move |theme| style::ticker_card_bar(theme, display_data.card_color_alpha));

    column![
        button(row![
            color_column,
            column![
                row![
                    row![
                        match exchange {
                            Exchange::BybitLinear | Exchange::BybitSpot =>
                                get_icon_text(Icon::BybitLogo, 12),
                            Exchange::BinanceFutures | Exchange::BinanceSpot =>
                                get_icon_text(Icon::BinanceLogo, 12),
                            Exchange::Rithmic =>
                                get_icon_text(Icon::RithmicLogo, 12),
                        },
                        text(&display_data.display_ticker),
                    ]
                    .spacing(2)
                    .align_y(alignment::Vertical::Center),
                    Space::new(Length::Fill, Length::Shrink),
                    text(&display_data.price_change_display),
                ]
                .spacing(4)
                .align_y(alignment::Vertical::Center),
                row![
                    text(&display_data.mark_price_display),
                    Space::new(Length::Fill, Length::Shrink),
                    text(&display_data.volume_display),
                ]
                .spacing(4),
            ]
            .padding(8)
            .spacing(4),
        ])
        .style(style::ticker_card_button)
        .on_press(Message::ExpandTickerCard(Some((*ticker, *exchange))))
    ]
    .height(Length::Fixed(60.0))
}

fn create_expanded_ticker_card<'a>(
    exchange: &Exchange,
    ticker: &Ticker,
    display_data: &'a TickerDisplayData,
    is_fav: bool,
) -> Column<'a, Message> {
    let (ticker_str, market) = ticker.get_string();

    column![
        row![
            button(get_icon_text(Icon::Return, 11))
                .on_press(Message::ExpandTickerCard(None))
                .style(move |theme, status| style::button_transparent(theme, status, false)),
            button(if is_fav {
                get_icon_text(Icon::StarFilled, 11)
            } else {
                get_icon_text(Icon::Star, 11)
            })
            .on_press(Message::FavoriteTicker(*exchange, *ticker))
            .style(move |theme, status| style::button_transparent(theme, status, false)),
        ]
        .spacing(2),
        row![
            match exchange {
                Exchange::BybitLinear | Exchange::BybitSpot => get_icon_text(Icon::BybitLogo, 12),
                Exchange::BinanceFutures | Exchange::BinanceSpot =>
                    get_icon_text(Icon::BinanceLogo, 12),
                Exchange::Rithmic => get_icon_text(Icon::RithmicLogo, 12),
            },
            text(
                ticker_str + {
                    match market {
                        MarketType::Spot => "",
                        MarketType::LinearPerps => " Perp",
                    }
                }
            ),
        ]
        .spacing(2),
        column![
            row![
                text("Last Updated Price: ").size(11),
                Space::new(Length::Fill, Length::Shrink),
                text(&display_data.mark_price_display)
            ],
            row![
                text("Daily Change: ").size(11),
                Space::new(Length::Fill, Length::Shrink),
                text(&display_data.price_change_display),
            ],
            row![
                text("Daily Volume: ").size(11),
                Space::new(Length::Fill, Length::Shrink),
                text(&display_data.volume_display),
            ],
        ]
        .spacing(4),
        column![
            button(text("Heatmap Chart").align_x(Horizontal::Center))
                .on_press(Message::TickerSelected(
                    *ticker,
                    *exchange,
                    "heatmap".to_string()
                ))
                .width(Length::Fixed(180.0)),
            button(text("Footprint Chart").align_x(Horizontal::Center))
                .on_press(Message::TickerSelected(
                    *ticker,
                    *exchange,
                    "footprint".to_string()
                ))
                .width(Length::Fixed(180.0)),
            button(text("Candlestick Chart").align_x(Horizontal::Center))
                .on_press(Message::TickerSelected(
                    *ticker,
                    *exchange,
                    "candlestick".to_string()
                ))
                .width(Length::Fixed(180.0)),
            button(text("Time&Sales").align_x(Horizontal::Center))
                .on_press(Message::TickerSelected(
                    *ticker,
                    *exchange,
                    "time&sales".to_string()
                ))
                .width(Length::Fixed(160.0)),
        ]
        .width(Length::Fill)
        .spacing(2),
    ]
    .padding(padding::top(8).right(16).left(16).bottom(16))
    .spacing(12)
}

fn create_tab_button<'a>(
    text: Text<'a, Theme, Renderer>,
    current_tab: &TickerTab,
    target_tab: TickerTab,
) -> Button<'a, Message, Theme, Renderer> {
    let mut btn =
        button(text).style(move |theme, status| style::button_transparent(theme, status, false));
    if *current_tab != target_tab {
        btn = btn.on_press(Message::ChangeTickersTableTab(target_tab));
    }
    btn
}

fn convert_to_currency_abbr(price: f32) -> String {
    if price > 1_000_000_000.0 {
        format!("${:.2}b", price / 1_000_000_000.0)
    } else if price > 1_000_000.0 {
        format!("${:.1}m", price / 1_000_000.0)
    } else if price > 1000.0 {
        format!("${:.2}k", price / 1000.0)
    } else {
        format!("${price:.2}")
    }
}

fn convert_to_pct_change(change: f32) -> String {
    if change > 0.0 {
        format!("+{change:.2}%")
    } else {
        format!("{change:.2}%")
    }
}
