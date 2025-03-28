use super::Element;
use crate::style;
use iced::{
    Alignment, Color, Length, padding,
    widget::{
        button, center, column, container, mouse_area, opaque, row, scrollable, stack, text,
        tooltip::Position,
    },
};

pub mod column_drag;
pub mod hsplit;
pub mod notification;

pub fn tooltip<'a, Message: 'a>(
    content: impl Into<Element<'a, Message>>,
    tooltip: Option<&'a str>,
    position: Position,
) -> Element<'a, Message> {
    match tooltip {
        Some(tooltip) => iced::widget::tooltip(
            content,
            container(text(tooltip)).style(style::tooltip).padding(8),
            position,
        )
        .into(),
        None => content.into(),
    }
}

pub fn scrollable_content<'a, Message: 'a>(
    content: impl Into<Element<'a, Message>>,
) -> Element<'a, Message> {
    scrollable::Scrollable::with_direction(
        content,
        scrollable::Direction::Vertical(scrollable::Scrollbar::new().width(4).scroller_width(4)),
    )
    .into()
}

pub fn confirm_dialog_container<'a, Message: 'a + Clone>(
    dialog: &'a str,
    on_confirm: Message,
    on_cancel: Message,
) -> Element<'a, Message> {
    container(
        column![
            text(dialog).size(14),
            row![
                button(text("Cancel"))
                    .style(|theme, status| style::button::transparent(theme, status, false))
                    .on_press(on_cancel),
                button(text("Confirm")).on_press(on_confirm),
            ]
            .spacing(8),
        ]
        .align_x(Alignment::Center)
        .spacing(16),
    )
    .padding(24)
    .style(style::dashboard_modal)
    .into()
}

pub fn main_dialog_modal<'a, Message>(
    base: impl Into<Element<'a, Message>>,
    content: impl Into<Element<'a, Message>>,
    on_blur: Message,
) -> Element<'a, Message>
where
    Message: Clone + 'a,
{
    stack![
        base.into(),
        opaque(
            mouse_area(center(opaque(content)).style(|_theme| {
                container::Style {
                    background: Some(
                        Color {
                            a: 0.8,
                            ..Color::BLACK
                        }
                        .into(),
                    ),
                    ..container::Style::default()
                }
            }))
            .on_press(on_blur)
        )
    ]
    .into()
}

pub fn dashboard_modal<'a, Message>(
    base: impl Into<Element<'a, Message>>,
    content: impl Into<Element<'a, Message>>,
    on_blur: Message,
    padding: padding::Padding,
    align_y: Alignment,
    align_x: Alignment,
) -> Element<'a, Message>
where
    Message: Clone + 'a,
{
    stack![
        base.into(),
        mouse_area(
            container(opaque(content))
                .width(Length::Fill)
                .height(Length::Fill)
                .padding(padding)
                .align_y(align_y)
                .align_x(align_x)
        )
        .on_press(on_blur)
    ]
    .into()
}

pub fn pane_modal<'a, Message>(
    base: impl Into<Element<'a, Message>>,
    content: impl Into<Element<'a, Message>>,
    on_blur: Message,
    padding: padding::Padding,
    alignment: Alignment,
) -> Element<'a, Message>
where
    Message: Clone + 'a,
{
    stack![
        base.into(),
        mouse_area(
            container(opaque(content))
                .width(Length::Fill)
                .height(Length::Fill)
                .padding(padding)
                .align_x(alignment)
        )
        .on_press(on_blur)
    ]
    .into()
}
