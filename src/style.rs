use iced::widget::container::{self, Style};
use iced::widget::pane_grid::{Highlight, Line};
use iced::widget::scrollable::{Rail, Scroller};
use iced::widget::{Text, text};
use iced::{Border, Color, Font, Renderer, Shadow, Theme, widget};

pub const ICON_BYTES: &[u8] = include_bytes!(".././assets/fonts/icons.ttf");
pub const ICON_FONT: Font = Font::with_name("icons");

pub enum Icon {
    Locked,
    Unlocked,
    ResizeFull,
    ResizeSmall,
    Close,
    Layout,
    Cog,
    Link,
    BinanceLogo,
    BybitLogo,
    RithmicLogo,
    Search,
    Sort,
    SortDesc,
    SortAsc,
    Star,
    StarFilled,
    Return,
    Popout,
    ChartOutline,
    TrashBin,
    Edit,
    Checkmark,
    Clone,
}

impl From<Icon> for char {
    fn from(icon: Icon) -> Self {
        match icon {
            Icon::Locked => '\u{E800}',
            Icon::Unlocked => '\u{E801}',
            Icon::Search => '\u{E802}',
            Icon::ResizeFull => '\u{E803}',
            Icon::ResizeSmall => '\u{E804}',
            Icon::Close => '\u{E805}',
            Icon::Layout => '\u{E806}',
            Icon::Link => '\u{E807}',
            Icon::BybitLogo => '\u{E808}',
            Icon::BinanceLogo => '\u{E809}',
            Icon::RithmicLogo => '\u{E7FF}',
            Icon::Cog => '\u{E810}',
            Icon::Sort => '\u{F0DC}',
            Icon::SortDesc => '\u{F0DD}',
            Icon::SortAsc => '\u{F0DE}',
            Icon::Star => '\u{E80A}',
            Icon::StarFilled => '\u{E80B}',
            Icon::Return => '\u{E80C}',
            Icon::Popout => '\u{E80D}',
            Icon::ChartOutline => '\u{E80E}',
            Icon::TrashBin => '\u{E80F}',
            Icon::Edit => '\u{E811}',
            Icon::Checkmark => '\u{E812}',
            Icon::Clone => '\u{F0C5}',
        }
    }
}

pub fn get_icon_text<'a>(icon: Icon, size: u16) -> Text<'a, Theme, Renderer> {
    text(char::from(icon).to_string())
        .font(ICON_FONT)
        .size(iced::Pixels(size.into()))
}

#[cfg(target_os = "macos")]
pub fn branding_text(theme: &Theme) -> iced::widget::text::Style {
    let palette = theme.extended_palette();

    iced::widget::text::Style {
        color: Some(
            palette
                .secondary
                .weak
                .color
                .scale_alpha(if palette.is_dark { 0.1 } else { 0.6 }),
        ),
    }
}

// Tooltips
pub fn tooltip(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: Some(palette.background.weakest.color.into()),
        border: Border {
            width: 1.0,
            color: palette.background.weak.color,
            radius: 4.0.into(),
        },
        ..Default::default()
    }
}

pub mod button {
    use iced::{
        Border, Theme,
        widget::button::{Status, Style},
    };

    pub fn confirm(theme: &Theme, status: Status, is_active: bool) -> Style {
        let palette = theme.extended_palette();

        let color_alpha = if palette.is_dark { 0.2 } else { 0.6 };

        Style {
            text_color: match status {
                Status::Active => palette.success.base.color,
                Status::Pressed => palette.success.weak.color,
                Status::Hovered => palette.success.strong.color,
                Status::Disabled => palette.background.base.text,
            },
            background: match (status, is_active) {
                (Status::Disabled, false) => {
                    Some(palette.success.weak.color.scale_alpha(color_alpha).into())
                }
                _ => None,
            },
            border: Border {
                radius: 3.0.into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn cancel(theme: &Theme, status: Status, is_active: bool) -> Style {
        let palette = theme.extended_palette();

        let color_alpha = if palette.is_dark { 0.2 } else { 0.6 };

        Style {
            text_color: match status {
                Status::Active => palette.danger.base.color,
                Status::Pressed => palette.danger.weak.color,
                Status::Hovered => palette.danger.strong.color,
                Status::Disabled => palette.background.base.text,
            },
            background: match (status, is_active) {
                (Status::Disabled, false) => {
                    Some(palette.danger.weak.color.scale_alpha(color_alpha).into())
                }
                _ => None,
            },
            border: Border {
                radius: 3.0.into(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn layout_name(theme: &Theme, status: Status) -> Style {
        let palette = theme.extended_palette();

        Style {
            text_color: match status {
                Status::Active => palette.background.base.text,
                Status::Pressed => palette.background.weak.text,
                Status::Hovered => palette.background.strong.text,
                Status::Disabled => palette.background.weakest.text,
            },
            background: None,
            ..Default::default()
        }
    }

    pub fn transparent(theme: &Theme, status: Status, is_clicked: bool) -> Style {
        let palette = theme.extended_palette();

        Style {
            text_color: palette.background.base.text,
            border: Border {
                radius: 3.0.into(),
                ..Default::default()
            },
            background: match status {
                Status::Active => {
                    if is_clicked {
                        Some(palette.background.weak.color.into())
                    } else {
                        None
                    }
                }
                Status::Pressed => Some(palette.background.strongest.color.into()),
                Status::Hovered => Some(palette.background.strong.color.into()),
                Status::Disabled => {
                    if is_clicked {
                        Some(palette.background.strongest.color.into())
                    } else {
                        Some(palette.background.strong.color.into())
                    }
                }
            },
            ..Default::default()
        }
    }

    pub fn modifier(theme: &Theme, status: Status, is_clicked: bool) -> Style {
        let palette = theme.extended_palette();

        Style {
            text_color: palette.background.base.text,
            border: Border {
                radius: 3.0.into(),
                ..Default::default()
            },
            background: match status {
                Status::Active => {
                    if is_clicked {
                        Some(palette.background.weak.color.into())
                    } else {
                        Some(palette.background.base.color.into())
                    }
                }
                Status::Pressed => Some(palette.background.strongest.color.into()),
                Status::Hovered => Some(palette.background.strong.color.into()),
                Status::Disabled => {
                    if is_clicked {
                        None
                    } else {
                        Some(palette.secondary.weak.color.into())
                    }
                }
            },
            ..Default::default()
        }
    }

    pub fn ticker_card(theme: &Theme, status: Status) -> Style {
        let palette = theme.extended_palette();

        match status {
            Status::Hovered => Style {
                text_color: palette.background.base.text,
                background: Some(palette.background.weak.color.into()),
                border: Border {
                    radius: 4.0.into(),
                    width: 1.0,
                    color: palette.background.weak.color,
                },
                ..Default::default()
            },
            Status::Pressed => Style {
                text_color: palette.background.base.text,
                background: Some(palette.background.weakest.color.into()),
                border: Border {
                    radius: 2.0.into(),
                    width: 1.0,
                    color: palette.background.weak.color,
                },
                ..Default::default()
            },
            _ => Style {
                text_color: palette.background.base.text,
                background: Some(palette.background.weakest.color.into()),
                border: Border {
                    radius: 2.0.into(),
                    width: 1.0,
                    color: palette.background.weak.color,
                },
                ..Default::default()
            },
        }
    }
}

// Panes
pub fn pane_grid(theme: &Theme) -> widget::pane_grid::Style {
    let palette = theme.extended_palette();

    widget::pane_grid::Style {
        hovered_region: Highlight {
            background: palette.background.strong.color.into(),
            border: Border {
                width: 1.0,
                color: palette.primary.base.color,
                radius: 4.0.into(),
            },
        },
        picked_split: Line {
            color: palette.primary.strong.color,
            width: 4.0,
        },
        hovered_split: Line {
            color: palette.primary.weak.color,
            width: 4.0,
        },
    }
}

pub fn pane_title_bar(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: {
            if palette.is_dark {
                Some(palette.background.weak.color.scale_alpha(0.2).into())
            } else {
                Some(palette.background.strong.color.scale_alpha(0.2).into())
            }
        },
        ..Default::default()
    }
}

pub fn pane_background(theme: &Theme, is_focused: bool) -> Style {
    let palette = theme.extended_palette();

    Style {
        text_color: Some(palette.background.base.text),
        background: Some(palette.background.weakest.color.into()),
        border: {
            if is_focused {
                Border {
                    width: 1.0,
                    color: palette.background.strong.color,
                    radius: 4.0.into(),
                }
            } else {
                Border {
                    width: 1.0,
                    color: palette.background.base.color,
                    radius: 2.0.into(),
                }
            }
        },
        ..Default::default()
    }
}

// Modals
pub fn chart_modal(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        text_color: Some(palette.background.base.text),
        background: Some(
            Color {
                a: 0.99,
                ..palette.background.base.color
            }
            .into(),
        ),
        border: Border {
            width: 1.0,
            color: palette.background.weak.color,
            radius: 4.0.into(),
        },
        shadow: Shadow {
            offset: iced::Vector { x: 0.0, y: 0.0 },
            blur_radius: 12.0,
            color: Color::BLACK.scale_alpha(if palette.is_dark { 0.4 } else { 0.2 }),
        },
    }
}

pub fn dashboard_modal(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: Some(
            Color {
                a: 0.99,
                ..palette.background.base.color
            }
            .into(),
        ),
        border: Border {
            width: 1.0,
            color: palette.secondary.weak.color,
            radius: 6.0.into(),
        },
        shadow: Shadow {
            offset: iced::Vector { x: 0.0, y: 0.0 },
            blur_radius: 20.0,
            color: Color::BLACK.scale_alpha(if palette.is_dark { 0.4 } else { 0.2 }),
        },
        ..Default::default()
    }
}

pub fn modal_container(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        text_color: Some(palette.background.base.text),
        background: Some(palette.background.weakest.color.into()),
        border: Border {
            width: 1.0,
            color: palette.background.weak.color,
            radius: 4.0.into(),
        },
        shadow: Shadow {
            offset: iced::Vector { x: 0.0, y: 0.0 },
            blur_radius: 2.0,
            color: Color::BLACK.scale_alpha(if palette.is_dark { 0.8 } else { 0.2 }),
        },
    }
}

pub fn layout_row_container(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        text_color: Some(palette.background.base.text),
        background: Some(palette.background.weakest.color.into()),
        border: Border {
            width: 1.0,
            color: palette.background.weak.color,
            radius: 4.0.into(),
        },
        shadow: Shadow {
            offset: iced::Vector { x: 0.0, y: 0.0 },
            blur_radius: 2.0,
            color: Color::BLACK.scale_alpha(if palette.is_dark { 0.8 } else { 0.2 }),
        },
    }
}

pub fn layout_card_bar(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: Some(palette.secondary.weak.color.into()),
        ..Default::default()
    }
}

// Time&Sales Table
pub fn ts_table_container(theme: &Theme, is_sell: bool, color_alpha: f32) -> Style {
    let palette = theme.extended_palette();

    let color = if is_sell {
        palette.danger.base.color
    } else {
        palette.success.base.color
    };

    Style {
        text_color: color.into(),
        border: Border {
            width: 2.0,
            color: color.scale_alpha(color_alpha),
            radius: 2.0.into(),
        },
        ..Default::default()
    }
}

// Tickers Table
pub fn search_input(
    theme: &Theme,
    status: widget::text_input::Status,
) -> widget::text_input::Style {
    let palette = theme.extended_palette();

    let (background, border_color, placeholder) = match status {
        widget::text_input::Status::Active => (
            palette.background.weakest.color,
            palette.background.weak.color,
            palette.background.strongest.color,
        ),
        widget::text_input::Status::Hovered => (
            palette.background.weak.color,
            palette.background.strong.color,
            palette.background.base.text,
        ),
        widget::text_input::Status::Focused { .. }
        | widget::text_input::Status::Disabled { .. } => (
            palette.background.base.color,
            palette.background.strong.color,
            palette.background.base.text,
        ),
    };

    widget::text_input::Style {
        background: background.into(),
        border: Border {
            radius: 3.0.into(),
            width: 1.0,
            color: border_color,
        },
        icon: palette.background.strong.text,
        placeholder,
        value: palette.background.weak.text,
        selection: palette.background.strongest.color,
    }
}

pub fn ticker_card(theme: &Theme) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: Some(palette.background.weakest.color.into()),
        border: Border {
            radius: 4.0.into(),
            width: 1.0,
            color: palette.background.strong.color,
        },
        ..Default::default()
    }
}

// the bar that lights up depending on the price change
pub fn ticker_card_bar(theme: &Theme, color_alpha: f32) -> Style {
    let palette = theme.extended_palette();

    Style {
        background: {
            if color_alpha > 0.0 {
                Some(palette.success.strong.color.scale_alpha(color_alpha).into())
            } else {
                Some(palette.danger.strong.color.scale_alpha(-color_alpha).into())
            }
        },
        border: Border {
            radius: 4.0.into(),
            width: 1.0,
            color: if color_alpha > 0.0 {
                palette.success.strong.color.scale_alpha(color_alpha)
            } else {
                palette.danger.strong.color.scale_alpha(-color_alpha)
            },
        },
        ..Default::default()
    }
}

// Scrollable
pub fn scroll_bar(theme: &Theme, status: widget::scrollable::Status) -> widget::scrollable::Style {
    let palette = theme.extended_palette();

    let (rail_bg, scroller_bg) = match status {
        widget::scrollable::Status::Hovered { .. } | widget::scrollable::Status::Dragged { .. } => {
            (
                palette.background.weakest.color,
                palette.background.weak.color,
            )
        }
        _ => (
            palette.background.base.color,
            palette.background.weakest.color,
        ),
    };

    let rail = Rail {
        background: Some(iced::Background::Color(rail_bg)),
        border: Border {
            radius: 2.0.into(),
            width: 1.0,
            color: Color::TRANSPARENT,
        },
        scroller: Scroller {
            color: scroller_bg,
            border: Border {
                radius: 2.0.into(),
                width: 0.0,
                color: Color::TRANSPARENT,
            },
        },
    };

    widget::scrollable::Style {
        container: container::Style {
            ..Default::default()
        },
        vertical_rail: rail,
        horizontal_rail: rail,
        gap: None,
    }
}

// custom widgets
pub fn split_ruler(theme: &Theme) -> iced::widget::rule::Style {
    let palette = theme.extended_palette();

    iced::widget::rule::Style {
        color: palette.background.strong.color.scale_alpha(0.4),
        width: 1,
        radius: iced::border::Radius::default(),
        fill_mode: iced::widget::rule::FillMode::Full,
    }
}
