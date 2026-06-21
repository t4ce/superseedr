// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::theme::ThemeContext;
use ratatui::prelude::{Color, Modifier, Style};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionTone {
    Add,
    Cancel,
    Clear,
    Confirm,
    Destructive,
    Edit,
    Info,
    Mode,
    Navigate,
    Open,
    Paste,
    Queue,
    Rate,
    Replay,
    Search,
    Select,
    Sort,
    Theme,
    Toggle,
}

pub fn action_color(ctx: &ThemeContext, tone: ActionTone) -> Color {
    match tone {
        ActionTone::Add | ActionTone::Confirm | ActionTone::Replay => ctx.state_success(),
        ActionTone::Cancel | ActionTone::Destructive => ctx.state_error(),
        ActionTone::Clear => ctx.accent_sapphire(),
        ActionTone::Edit | ActionTone::Queue | ActionTone::Sort => ctx.state_warning(),
        ActionTone::Info => ctx.theme.semantic.subtext1,
        ActionTone::Mode | ActionTone::Theme => ctx.state_selected(),
        ActionTone::Navigate | ActionTone::Select | ActionTone::Toggle => ctx.state_info(),
        ActionTone::Open => ctx.state_complete(),
        ActionTone::Paste => ctx.accent_teal(),
        ActionTone::Rate | ActionTone::Search => ctx.accent_sapphire(),
    }
}

pub fn footer_key_style(ctx: &ThemeContext, tone: ActionTone) -> Style {
    ctx.apply(
        Style::default()
            .fg(action_color(ctx, tone))
            .add_modifier(Modifier::BOLD),
    )
}

pub fn help_key_style(ctx: &ThemeContext, tone: ActionTone) -> Style {
    ctx.apply(Style::default().fg(action_color(ctx, tone)))
}
