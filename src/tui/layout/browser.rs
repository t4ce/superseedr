// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::app::BrowserPane;
use ratatui::layout::{Constraint, Layout, Rect};

#[derive(Default, Debug)]
pub struct FileBrowserLayout {
    pub area: Rect,
    pub content: Rect,
    pub footer: Rect,

    pub preview: Option<Rect>,
    pub browser: Rect,

    pub search: Option<Rect>,
    pub list: Rect,
}

pub fn calculate_file_browser_layout(
    area: Rect,
    show_preview: bool,
    show_search: bool,
    focused_pane: &BrowserPane,
    preview_only: bool,
) -> FileBrowserLayout {
    let mut plan = FileBrowserLayout::default();
    let main_chunks = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(area);

    plan.area = area;
    plan.content = main_chunks[0];
    plan.footer = main_chunks[1];

    let content_area = if show_search {
        let search_chunks =
            Layout::vertical([Constraint::Length(3), Constraint::Min(0)]).split(plan.content);
        plan.search = Some(search_chunks[0]);
        search_chunks[1]
    } else {
        plan.content
    };

    if show_preview && preview_only {
        plan.preview = Some(content_area);
        plan.browser = Rect::default();
        plan.list = Rect::default();
        return plan;
    }

    let is_narrow = area.width < 100 || (area.height as f32 > (area.width as f32 * 0.6));

    let content_chunks = if show_preview {
        if is_narrow {
            let constraints = match focused_pane {
                BrowserPane::FileSystem => [Constraint::Percentage(35), Constraint::Percentage(65)],
                BrowserPane::TorrentPreview => {
                    [Constraint::Percentage(60), Constraint::Percentage(40)]
                }
            };
            Layout::vertical(constraints).split(content_area)
        } else {
            let constraints = match focused_pane {
                BrowserPane::FileSystem => [Constraint::Percentage(35), Constraint::Percentage(65)],
                BrowserPane::TorrentPreview => {
                    [Constraint::Percentage(60), Constraint::Percentage(40)]
                }
            };
            Layout::horizontal(constraints).split(content_area)
        }
    } else {
        Layout::horizontal([Constraint::Percentage(0), Constraint::Percentage(100)])
            .split(content_area)
    };

    plan.preview = if show_preview {
        Some(content_chunks[0])
    } else {
        None
    };
    plan.browser = content_chunks[1];
    plan.list = plan.browser;

    plan
}
