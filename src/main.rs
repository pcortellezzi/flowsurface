#![windows_subsystem = "windows"]

mod charts;
mod layout;
mod logger;
mod screen;
mod style;
mod widget;
mod window;

use layout::{Layout, LayoutManager};
use screen::{
    create_button,
    dashboard::{
        self, Dashboard, pane,
        tickers_table::{self, TickersTable},
    },
};
use style::{Icon, TITLE_PADDING_TOP, get_icon_text};
use widget::{
    confirm_dialog_container, dashboard_modal, main_dialog_modal,
    notification::{self, Toast},
    tooltip,
};
use window::{Window, window_events};

use data::{InternalError, config::theme::custom_theme, layout::WindowSpec, sidebar};
use exchange::adapter::{Exchange, StreamType, fetch_ticker_info};
use iced::{
    Alignment, Element, Length, Subscription, Task, padding,
    widget::{
        Space, button, center, column, container, pane_grid, pick_list, responsive, row, text,
        tooltip::Position as TooltipPosition,
    },
};
use std::{collections::HashMap, vec};

fn main() {
    logger::setup(false).expect("Failed to initialize logger");

    let saved_state = layout::load_saved_state();

    std::thread::spawn(data::cleanup_old_market_data);

    let main_window_cfg = window::Settings {
        size: saved_state
            .main_window
            .map_or_else(window::default_size, |w| w.get_size()),
        position: saved_state.main_window.map(|w| w.get_position()).map_or(
            iced::window::Position::Centered,
            iced::window::Position::Specific,
        ),
        exit_on_close_request: false,
        ..window::settings()
    };

    let _ = iced::daemon("Flowsurface", State::update, State::view)
        .settings(iced::Settings {
            default_text_size: iced::Pixels(12.0),
            antialiasing: true,
            ..Default::default()
        })
        .font(style::ICON_BYTES)
        .theme(State::theme)
        .scale_factor(State::scale_factor)
        .subscription(State::subscription)
        .run_with(move || State::new(saved_state, main_window_cfg));
}

#[derive(Debug, Clone)]
enum Message {
    ErrorOccurred(InternalError),

    MarketWsEvent(exchange::Event),
    ToggleTradeFetch(bool),

    WindowEvent(window::Event),
    SaveAndExit(HashMap<window::Id, WindowSpec>),

    LayoutSelected(Layout),
    LoadLayout(Layout),
    ManageLayouts(layout::Message),

    Dashboard(Option<uuid::Uuid>, dashboard::Message),

    SetTimezone(data::UserTimezone),
    ToggleSidebarMenu(sidebar::Menu),
    SetSidebarPosition(sidebar::Position),
    ScaleFactorChanged(f64),
    ThemeSelected(data::Theme),

    TickersTable(tickers_table::Message),
    FetchForTickersInfo,

    ToggleDialogModal(Option<(String, Box<Message>)>),

    AddNotification(Toast),
    DeleteNotification(usize),
}

struct State {
    main_window: Window,
    layout_manager: LayoutManager,
    tickers_table: TickersTable,
    confirm_dialog: Option<(String, Box<Message>)>,
    scale_factor: data::ScaleFactor,
    timezone: data::UserTimezone,
    sidebar: data::Sidebar,
    theme: data::Theme,
    notifications: Vec<Toast>,
}

impl State {
    fn new(
        saved_state: layout::SavedState,
        main_window_cfg: window::Settings,
    ) -> (Self, Task<Message>) {
        let (main_window_id, open_main_window) = window::open(main_window_cfg);

        let active_layout = saved_state.layout_manager.active_layout.clone();

        (
            Self {
                main_window: Window::new(main_window_id),
                layout_manager: saved_state.layout_manager,
                tickers_table: TickersTable::new(saved_state.favorited_tickers),
                confirm_dialog: None,
                timezone: saved_state.timezone,
                scale_factor: saved_state.scale_factor,
                sidebar: saved_state.sidebar,
                theme: saved_state.theme,
                notifications: vec![],
            },
            open_main_window
                .then(|_| Task::none())
                .chain(Task::batch(vec![
                    Task::done(Message::LoadLayout(active_layout)),
                    Task::done(Message::SetTimezone(saved_state.timezone)),
                    Task::done(Message::FetchForTickersInfo),
                ])),
        )
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::FetchForTickersInfo => {
                let fetch_tasks = Exchange::ALL
                    .iter()
                    .map(|exchange| {
                        Task::perform(fetch_ticker_info(*exchange), move |result| match result {
                            Ok(ticker_info) => Message::TickersTable(
                                tickers_table::Message::UpdateTickersInfo(*exchange, ticker_info),
                            ),
                            Err(err) => {
                                Message::ErrorOccurred(InternalError::Fetch(err.to_string()))
                            }
                        })
                    })
                    .collect::<Vec<Task<Message>>>();

                return Task::batch(fetch_tasks);
            }
            Message::MarketWsEvent(event) => {
                let main_window_id = self.main_window.id;

                if let Some(dashboard) = self.get_active_dashboard_mut() {
                    match event {
                        exchange::Event::Connected(exchange, _) => {
                            log::info!("a stream connected to {exchange} WS");
                        }
                        exchange::Event::Disconnected(exchange, reason) => {
                            log::info!("a stream disconnected from {exchange} WS: {reason:?}");
                        }
                        exchange::Event::DepthReceived(
                            stream,
                            depth_update_t,
                            depth,
                            trades_buffer,
                        ) => {
                            return dashboard
                                .update_depth_and_trades(
                                    &stream,
                                    depth_update_t,
                                    &depth,
                                    &trades_buffer,
                                    main_window_id,
                                )
                                .map(move |msg| Message::Dashboard(None, msg));
                        }
                        exchange::Event::KlineReceived(stream, kline) => {
                            return dashboard
                                .update_latest_klines(&stream, &kline, main_window_id)
                                .map(move |msg| Message::Dashboard(None, msg));
                        }
                    }
                }
            }
            Message::WindowEvent(event) => match event {
                window::Event::CloseRequested(window) => {
                    let main_window = self.main_window.id;

                    let Some(dashboard) = self.get_active_dashboard_mut() else {
                        return iced::exit();
                    };

                    if window != main_window {
                        dashboard.popout.remove(&window);
                        return window::close(window);
                    }

                    let mut opened_windows: Vec<window::Id> =
                        dashboard.popout.keys().copied().collect::<Vec<_>>();

                    opened_windows.push(self.main_window.id);

                    return window::collect_window_specs(opened_windows, Message::SaveAndExit);
                }
            },
            Message::SaveAndExit(windows) => {
                let dashboard = self
                    .get_active_dashboard_mut()
                    .expect("No active dashboard");

                dashboard
                    .popout
                    .iter_mut()
                    .for_each(|(id, (_, window_spec))| {
                        if let Some(new_window_spec) = windows.get(id) {
                            *window_spec = *new_window_spec;
                        }
                    });

                let mut ser_layouts = Vec::new();

                for id in &self.layout_manager.layout_order {
                    if let Some((layout, dashboard)) = self.layout_manager.layouts.get(id) {
                        let serialized_dashboard = data::Dashboard::from(dashboard);

                        ser_layouts.push(data::Layout {
                            name: layout.name.clone(),
                            dashboard: serialized_dashboard,
                        });
                    }
                }

                let layouts = data::Layouts {
                    layouts: ser_layouts,
                    active_layout: self.layout_manager.active_layout.name.clone(),
                };

                let main_window = windows
                    .iter()
                    .find(|(id, _)| **id == self.main_window.id)
                    .map(|(_, spec)| *spec);

                let layout = data::State::from_parts(
                    layouts,
                    self.theme.clone(),
                    self.tickers_table.get_favorited_tickers(),
                    main_window,
                    self.timezone,
                    self.sidebar,
                    self.scale_factor,
                );

                match serde_json::to_string(&layout) {
                    Ok(layout_str) => {
                        let file_name = data::SAVED_STATE_PATH;

                        if let Err(e) = data::write_json_to_file(&layout_str, file_name) {
                            log::error!("Failed to write layout state to file: {}", e);
                        } else {
                            log::info!("Successfully wrote layout state to {file_name}");
                        }
                    }
                    Err(e) => log::error!("Failed to serialize layout: {}", e),
                }

                return iced::exit();
            }
            Message::ErrorOccurred(err) => {
                return match err {
                    InternalError::Fetch(err) | InternalError::Layout(err) => {
                        Task::done(Message::AddNotification(Toast::error(err)))
                    }
                };
            }
            Message::ThemeSelected(theme) => {
                self.theme = theme;
            }
            Message::LayoutSelected(new_layout_id) => {
                if let Some(dashboard) = self.get_active_dashboard() {
                    let active_popout_keys = dashboard.popout.keys().copied().collect::<Vec<_>>();

                    let window_tasks = Task::batch(
                        active_popout_keys
                            .iter()
                            .map(|&popout_id| window::close(popout_id))
                            .collect::<Vec<_>>(),
                    )
                    .then(|_: Task<window::Id>| Task::none());

                    return window::collect_window_specs(
                        active_popout_keys,
                        dashboard::Message::SavePopoutSpecs,
                    )
                    .map(move |msg| Message::Dashboard(None, msg))
                    .chain(window_tasks)
                    .chain(Task::done(Message::LoadLayout(new_layout_id)));
                }
            }
            Message::LoadLayout(layout) => {
                self.layout_manager.active_layout = layout.clone();
                if let Some(dashboard) = self.get_active_dashboard_mut() {
                    dashboard.focus = None;
                    return dashboard
                        .load_layout()
                        .map(move |msg| Message::Dashboard(None, msg));
                }
            }
            Message::Dashboard(id, message) => match message {
                dashboard::Message::Notification(toast) => {
                    return Task::done(Message::AddNotification(toast));
                }
                dashboard::Message::DistributeFetchedData(layout_id, pane_uid, data, stream) => {
                    let main_window = self.main_window;

                    if let Some(dashboard) = self.layout_manager.get_mut_dashboard(&layout_id) {
                        return dashboard
                            .distribute_fetched_data(main_window.id, pane_uid, data, stream)
                            .map(move |msg| Message::Dashboard(Some(layout_id), msg));
                    } else {
                        return Task::done(Message::ErrorOccurred(InternalError::Layout(
                            "Couldn't find dashboard".to_string(),
                        )));
                    }
                }
                _ => {
                    let main_window = self.main_window;
                    let layout_id = id.unwrap_or(self.layout_manager.active_layout.id);

                    if let Some(dashboard) = self.layout_manager.get_mut_dashboard(&layout_id) {
                        return dashboard
                            .update(message, &main_window, &layout_id)
                            .map(move |msg| Message::Dashboard(Some(layout_id), msg));
                    }
                }
            },
            Message::TickersTable(message) => {
                let action = self.tickers_table.update(message);

                match action {
                    tickers_table::Action::TickerSelected(ticker_info, exchange, content) => {
                        let main_window_id = self.main_window.id;

                        if let Some(dashboard) = self.get_active_dashboard_mut() {
                            let task = dashboard.init_pane_task(
                                main_window_id,
                                ticker_info,
                                exchange,
                                &content,
                            );

                            return task.map(move |msg| Message::Dashboard(None, msg));
                        }
                    }
                    tickers_table::Action::Fetch(task) => {
                        return task.map(Message::TickersTable);
                    }
                    tickers_table::Action::ErrorOccurred(err) => {
                        return Task::done(Message::ErrorOccurred(err));
                    }
                    tickers_table::Action::None => {}
                }
            }
            Message::SetTimezone(tz) => {
                self.timezone = tz;
            }
            Message::ToggleSidebarMenu(menu) => {
                let new_menu = if self.sidebar.is_menu_active(menu) {
                    sidebar::Menu::None
                } else {
                    menu
                };
                self.sidebar.set_menu(new_menu);
            }
            Message::SetSidebarPosition(position) => {
                self.sidebar.set_position(position);
            }
            Message::ToggleTradeFetch(checked) => {
                self.layout_manager
                    .iter_dashboards_mut()
                    .for_each(|dashboard| {
                        dashboard.toggle_trade_fetch(checked, &self.main_window);
                    });

                if checked {
                    self.confirm_dialog = None;
                }
            }
            Message::ToggleDialogModal(dialog) => {
                self.confirm_dialog = dialog;
            }
            Message::ScaleFactorChanged(value) => {
                self.scale_factor = data::ScaleFactor::from(value);
            }
            Message::ManageLayouts(msg) => {
                if let layout::Message::SelectActive(layout) = msg {
                    return Task::done(Message::LayoutSelected(layout));
                } else {
                    return self.layout_manager.update(msg).map(Message::ManageLayouts);
                }
            }
            Message::AddNotification(toast) => {
                self.notifications.push(toast);
            }
            Message::DeleteNotification(index) => {
                self.notifications.remove(index);
            }
        }
        Task::none()
    }

    fn view(&self, id: window::Id) -> Element<'_, Message> {
        let Some(dashboard) = self.get_active_dashboard() else {
            return center(
                column![
                    text("No dashboard available").size(20),
                    button("Add new dashboard")
                        .on_press(Message::ManageLayouts(layout::Message::AddLayout))
                ]
                .align_x(Alignment::Center)
                .spacing(8),
            )
            .into();
        };

        let sidebar_pos = self.sidebar.position;

        let content = if id == self.main_window.id {
            let tooltip_position = if sidebar_pos == sidebar::Position::Left {
                TooltipPosition::Right
            } else {
                TooltipPosition::Left
            };

            let sidebar = {
                let is_table_open = self.tickers_table.is_open();

                let nav_buttons = {
                    let settings_modal_button = {
                        let is_active = self.sidebar.is_menu_active(sidebar::Menu::Settings);

                        create_button(
                            get_icon_text(Icon::Cog, 14)
                                .width(24)
                                .align_x(Alignment::Center),
                            Message::ToggleSidebarMenu(sidebar::Menu::Settings),
                            Some("Settings"),
                            tooltip_position,
                            move |theme, status| {
                                style::button::transparent(theme, status, is_active)
                            },
                        )
                    };
                    let layout_modal_button = {
                        let is_active = self.sidebar.is_menu_active(sidebar::Menu::Layout);

                        create_button(
                            get_icon_text(Icon::Layout, 14)
                                .width(24)
                                .align_x(Alignment::Center),
                            Message::ToggleSidebarMenu(sidebar::Menu::Layout),
                            None,
                            tooltip_position,
                            move |theme, status| {
                                style::button::transparent(theme, status, is_active)
                            },
                        )
                    };
                    let ticker_search_button = {
                        create_button(
                            get_icon_text(Icon::Search, 14)
                                .width(24)
                                .align_x(Alignment::Center),
                            Message::TickersTable(tickers_table::Message::ToggleTable),
                            Some("Search Tickers"),
                            tooltip_position,
                            move |theme, status| {
                                style::button::transparent(theme, status, is_table_open)
                            },
                        )
                    };

                    column![
                        ticker_search_button,
                        layout_modal_button,
                        Space::with_height(Length::Fill),
                        settings_modal_button,
                    ]
                    .width(32)
                    .spacing(8)
                };

                let tickers_table = {
                    if is_table_open {
                        column![responsive(move |size| {
                            self.tickers_table.view(size).map(Message::TickersTable)
                        })]
                        .width(260)
                    } else {
                        column![]
                    }
                };

                match sidebar_pos {
                    sidebar::Position::Left => {
                        row![nav_buttons, tickers_table,]
                    }
                    sidebar::Position::Right => {
                        row![tickers_table, nav_buttons,]
                    }
                }
                .spacing(if is_table_open { 8 } else { 4 })
            };

            let dashboard_view = dashboard
                .view(&self.main_window, self.timezone)
                .map(move |msg| Message::Dashboard(None, msg));

            let base = column![
                {
                    #[cfg(target_os = "macos")]
                    {
                        iced::widget::center(
                            text("FLOWSURFACE")
                                .font(iced::Font {
                                    weight: iced::font::Weight::Bold,
                                    ..Default::default()
                                })
                                .size(16)
                                .style(style::branding_text)
                                .align_x(Alignment::Center),
                        )
                        .height(20)
                        .align_y(Alignment::Center)
                        .padding(padding::right(8).top(4))
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        column![]
                    }
                },
                match sidebar_pos {
                    sidebar::Position::Left => row![sidebar, dashboard_view,],
                    sidebar::Position::Right => row![dashboard_view, sidebar],
                }
                .spacing(4)
                .padding(8),
            ];

            match self.sidebar.active_menu {
                sidebar::Menu::Settings => {
                    let settings_modal = {
                        let mut all_themes = iced_core::Theme::ALL.to_vec();
                        all_themes.push(iced_core::Theme::Custom(custom_theme().into()));

                        let trade_fetch_checkbox = {
                            let is_active = dashboard.trade_fetch_enabled;

                            let checkbox =
                                iced::widget::checkbox("Fetch trades (Binance)", is_active)
                                    .on_toggle(|checked| {
                                        if checked {
                                            Message::ToggleDialogModal(Some((
                                        "This might be unreliable and take some time to complete"
                                            .to_string(),
                                        Box::new(Message::ToggleTradeFetch(true)),
                                    )))
                                        } else {
                                            Message::ToggleTradeFetch(false)
                                        }
                                    });

                            tooltip(
                                checkbox,
                                Some("Try to fetch trades for footprint charts"),
                                TooltipPosition::Top,
                            )
                        };

                        let theme_picklist =
                            pick_list(all_themes, Some(self.theme.clone().0), |theme| {
                                Message::ThemeSelected(data::Theme(theme))
                            });

                        let timezone_picklist = pick_list(
                            [data::UserTimezone::Utc, data::UserTimezone::Local],
                            Some(self.timezone),
                            Message::SetTimezone,
                        );

                        let sidebar_pos = pick_list(
                            [sidebar::Position::Left, sidebar::Position::Right],
                            Some(sidebar_pos),
                            Message::SetSidebarPosition,
                        );

                        let scale_factor = {
                            let current_value: f64 = self.scale_factor.into();

                            let decrease_btn = if current_value > 0.8 {
                                button(text("-"))
                                    .on_press(Message::ScaleFactorChanged(current_value - 0.1))
                            } else {
                                button(text("-"))
                            };

                            let increase_btn = if current_value < 1.8 {
                                button(text("+"))
                                    .on_press(Message::ScaleFactorChanged(current_value + 0.1))
                            } else {
                                button(text("+"))
                            };

                            container(
                                row![
                                    decrease_btn,
                                    text(format!("{:.0}%", current_value * 100.0)).size(14),
                                    increase_btn,
                                ]
                                .align_y(Alignment::Center)
                                .spacing(8)
                                .padding(4),
                            )
                            .style(style::modal_container)
                        };

                        container(
                            column![
                                column![text("Sidebar position").size(14), sidebar_pos,].spacing(8),
                                column![text("Time zone").size(14), timezone_picklist,].spacing(8),
                                column![text("Theme").size(14), theme_picklist,].spacing(8),
                                column![text("Interface scale").size(14), scale_factor,].spacing(8),
                                column![text("Experimental").size(14), trade_fetch_checkbox,]
                                    .spacing(8),
                            ]
                            .spacing(20),
                        )
                        .align_x(Alignment::Start)
                        .max_width(400)
                        .padding(24)
                        .style(style::dashboard_modal)
                    };

                    let (align_x, padding) = match sidebar_pos {
                        sidebar::Position::Left => (Alignment::Start, padding::left(48).top(8)),
                        sidebar::Position::Right => (Alignment::End, padding::right(48).top(8)),
                    };

                    let base_content = dashboard_modal(
                        base,
                        settings_modal,
                        Message::ToggleSidebarMenu(sidebar::Menu::None),
                        padding,
                        Alignment::End,
                        align_x,
                    );

                    if let Some((dialog, on_confirm)) = &self.confirm_dialog {
                        let dialog_content = confirm_dialog_container(
                            dialog,
                            *on_confirm.to_owned(),
                            Message::ToggleDialogModal(None),
                        );

                        main_dialog_modal(
                            base_content,
                            dialog_content,
                            Message::ToggleDialogModal(None),
                        )
                    } else {
                        base_content
                    }
                }
                sidebar::Menu::Layout => {
                    let reset_pane_button = tooltip(
                        button(text("Reset").align_x(Alignment::Center))
                            .width(iced::Length::Fill)
                            .on_press(Message::Dashboard(
                                None,
                                dashboard::Message::Pane(
                                    id,
                                    pane::Message::ReplacePane(
                                        if let Some(focus) = dashboard.focus {
                                            focus.1
                                        } else {
                                            *dashboard.panes.iter().next().unwrap().0
                                        },
                                    ),
                                ),
                            )),
                        Some("Reset selected pane"),
                        TooltipPosition::Top,
                    );
                    let split_pane_button = tooltip(
                        button(text("Split").align_x(Alignment::Center))
                            .width(iced::Length::Fill)
                            .on_press(Message::Dashboard(
                                None,
                                dashboard::Message::Pane(
                                    id,
                                    pane::Message::SplitPane(
                                        pane_grid::Axis::Horizontal,
                                        if let Some(focus) = dashboard.focus {
                                            focus.1
                                        } else {
                                            *dashboard.panes.iter().next().unwrap().0
                                        },
                                    ),
                                ),
                            )),
                        Some("Split selected pane horizontally"),
                        TooltipPosition::Top,
                    );

                    let manage_layout_modal = {
                        container(
                            column![
                                column![
                                    text("Panes").size(14),
                                    if dashboard.focus.is_some() {
                                        row![reset_pane_button, split_pane_button,]
                                            .padding(padding::left(8).right(8))
                                            .spacing(8)
                                    } else {
                                        row![text("No pane selected"),]
                                    },
                                ]
                                .align_x(Alignment::Center)
                                .spacing(8),
                                column![
                                    text("Layouts").size(14),
                                    self.layout_manager.view().map(Message::ManageLayouts),
                                ]
                                .align_x(Alignment::Center)
                                .spacing(8),
                            ]
                            .align_x(Alignment::Center)
                            .spacing(32),
                        )
                        .width(280)
                        .padding(24)
                        .style(style::dashboard_modal)
                    };

                    let (align_x, padding) = match sidebar_pos {
                        sidebar::Position::Left => (Alignment::Start, padding::left(48).top(40)),
                        sidebar::Position::Right => (Alignment::End, padding::right(48).top(40)),
                    };

                    dashboard_modal(
                        base,
                        manage_layout_modal,
                        Message::ToggleSidebarMenu(sidebar::Menu::None),
                        padding,
                        Alignment::Start,
                        align_x,
                    )
                }
                sidebar::Menu::None => base.into(),
            }
        } else {
            container(
                dashboard
                    .view_window(id, &self.main_window, self.timezone)
                    .map(move |msg| Message::Dashboard(None, msg)),
            )
            .padding(padding::top(TITLE_PADDING_TOP))
            .into()
        };

        notification::Manager::new(
            content,
            &self.notifications,
            match sidebar_pos {
                sidebar::Position::Left => Alignment::End,
                sidebar::Position::Right => Alignment::Start,
            },
            Message::DeleteNotification,
        )
        .into()
    }

    fn theme(&self, _window: window::Id) -> iced_core::Theme {
        self.theme.clone().into()
    }

    fn scale_factor(&self, _window: window::Id) -> f64 {
        self.scale_factor.into()
    }

    fn subscription(&self) -> Subscription<Message> {
        let window_events = window_events().map(Message::WindowEvent);

        let Some(dashboard) = self.get_active_dashboard() else {
            return window_events;
        };

        let exchange_streams = dashboard.get_market_subscriptions(Message::MarketWsEvent);

        let tickers_table_fetch = self.tickers_table.subscription().map(Message::TickersTable);

        Subscription::batch(vec![exchange_streams, tickers_table_fetch, window_events])
    }

    fn get_active_dashboard(&self) -> Option<&Dashboard> {
        self.layout_manager.get_active_dashboard()
    }

    fn get_active_dashboard_mut(&mut self) -> Option<&mut Dashboard> {
        self.layout_manager.get_active_dashboard_mut()
    }
}
