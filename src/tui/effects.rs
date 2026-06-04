// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::app::AppState;
use crate::config::Settings;
use crate::theme::ThemeContext;
use ratatui::prelude::{Color, Frame};

pub(crate) fn compute_effects_activity_speed_multiplier(
    app_state: &AppState,
    settings: &Settings,
) -> f64 {
    let dl_bps = app_state.avg_download_history.last().copied().unwrap_or(0) as f64;
    let ul_bps = app_state.avg_upload_history.last().copied().unwrap_or(0) as f64;

    let dl_limit = app_state.effective_download_limit_bps;
    let dl_ref = if !crate::config::is_unlimited_rate_limit_bps(dl_limit) {
        dl_limit as f64
    } else {
        4_000_000.0
    };
    let ul_ref = if !crate::config::is_unlimited_rate_limit_bps(settings.global_upload_limit_bps) {
        settings.global_upload_limit_bps as f64
    } else {
        1_000_000.0
    };

    let dl_activity = (dl_bps / dl_ref).clamp(0.0, 1.0);
    let ul_activity = (ul_bps / ul_ref).clamp(0.0, 1.0);

    let activity_score = (dl_activity * 0.60) + (ul_activity * 0.40);
    1.0 + (activity_score * 2.0)
}

pub(crate) fn apply_theme_effects_to_frame(f: &mut Frame, ctx: &ThemeContext) {
    if !ctx.theme.effects.enabled() {
        return;
    }

    let area = f.area();
    let buf = f.buffer_mut();

    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            if let Some(cell) = buf.cell_mut((x, y)) {
                if cell.fg != Color::Reset {
                    cell.fg = ctx.apply_effects_to_color_at(cell.fg, x, y, area.width, area.height);
                }
            }
        }
    }
}
