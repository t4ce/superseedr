// SPDX-FileCopyrightText: 2026 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::theme::ThemeContext;
use ratatui::layout::Rect;
use ratatui::prelude::{Frame, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Padding, Paragraph};

pub(crate) fn draw_prompt_panel(
    f: &mut Frame,
    area: Rect,
    title: String,
    value: String,
    mut trailing_spans: Vec<Span<'static>>,
    ctx: &ThemeContext,
) {
    let mut line_spans = vec![
        Span::styled(
            "> ",
            ctx.apply(Style::default().fg(ctx.state_selected()).bold()),
        ),
        Span::raw(value),
        Span::styled("_", ctx.apply(Style::default().fg(ctx.state_warning()))),
    ];
    line_spans.append(&mut trailing_spans);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .padding(Padding::horizontal(1))
        .border_style(ctx.apply(Style::default().fg(ctx.state_selected())));
    f.render_widget(Paragraph::new(Line::from(line_spans)).block(block), area);
}
