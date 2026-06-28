// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::sync::Arc;

use crate::app::{AppCommand, AppMode, ConfigItem, FileBrowserMode};
use crate::config::Settings;
use crate::token_bucket::{rate_limit_bps_to_bucket_bytes_per_sec, TokenBucket};
use crate::tui::action_style::{footer_key_style, ActionTone};
use crate::tui::app_command::spawn_app_command_sender;
use crate::tui::formatters::{format_limit_bps, path_to_string};
use crate::tui::screen_context::ScreenContext;
use directories::UserDirs;
use ratatui::crossterm::event::{Event as CrosstermEvent, KeyCode, KeyEventKind};
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::prelude::{Frame, Line, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use tokio::sync::{broadcast, mpsc};

const RATE_LIMIT_STEP_BPS: u64 = 10_000 * 8;
const UNLIMITED_RATE_LIMIT_BPS: u64 = crate::config::UNLIMITED_RATE_LIMIT_BPS;

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigAction {
    SaveAndExit,
    StartEditOrBrowse,
    ToggleSelectedBool,
    SetSelectedBool(bool),
    MoveUp,
    MoveDown,
    ResetSelected,
    IncreaseSelected,
    DecreaseSelected,
    EditInsert(char),
    EditBackspace,
    EditCancel,
    EditCommit,
}

pub enum ConfigEffect {
    AppCommand(Box<AppCommand>),
    SetDownloadRate(u64),
    SetUploadRate(u64),
    ToNormal,
}

pub struct ConfigHandleContext<'a> {
    pub mode: &'a mut AppMode,
    pub settings_edit: &'a mut Box<Settings>,
    pub selected_index: &'a mut usize,
    pub items: &'a mut [ConfigItem],
    pub editing: &'a mut Option<(ConfigItem, String)>,
    pub app_command_tx: &'a mpsc::Sender<AppCommand>,
    pub shutdown_tx: &'a broadcast::Sender<()>,
    pub file_browser_generation: &'a mut u64,
    pub global_dl_bucket: &'a Arc<TokenBucket>,
    pub global_ul_bucket: &'a Arc<TokenBucket>,
}

#[derive(Default)]
pub struct ConfigReduceResult {
    pub consumed: bool,
    pub effects: Vec<ConfigEffect>,
}

fn shared_path_is_manual(item: ConfigItem) -> bool {
    crate::config::is_shared_config_mode() && item == ConfigItem::DefaultDownloadFolder
}

fn increase_rate_limit_bps(current: u64) -> u64 {
    match current {
        0 => UNLIMITED_RATE_LIMIT_BPS,
        UNLIMITED_RATE_LIMIT_BPS => RATE_LIMIT_STEP_BPS,
        _ => current.saturating_add(RATE_LIMIT_STEP_BPS),
    }
}

fn decrease_rate_limit_bps(current: u64) -> u64 {
    match current {
        0 => 0,
        UNLIMITED_RATE_LIMIT_BPS => 0,
        _ => current
            .checked_sub(RATE_LIMIT_STEP_BPS)
            .filter(|new_rate| *new_rate > 0)
            .unwrap_or(UNLIMITED_RATE_LIMIT_BPS),
    }
}

fn map_key_to_config_action(
    key_code: KeyCode,
    editing: &Option<(ConfigItem, String)>,
) -> Option<ConfigAction> {
    if editing.is_some() {
        return match key_code {
            KeyCode::Char(c) if c.is_ascii_digit() => Some(ConfigAction::EditInsert(c)),
            KeyCode::Backspace => Some(ConfigAction::EditBackspace),
            KeyCode::Esc => Some(ConfigAction::EditCancel),
            KeyCode::Enter => Some(ConfigAction::EditCommit),
            _ => None,
        };
    }

    match key_code {
        KeyCode::Esc | KeyCode::Char('Q') => Some(ConfigAction::SaveAndExit),
        KeyCode::Enter => Some(ConfigAction::StartEditOrBrowse),
        KeyCode::Char(' ') => Some(ConfigAction::ToggleSelectedBool),
        KeyCode::Char('t') => Some(ConfigAction::SetSelectedBool(true)),
        KeyCode::Char('f') => Some(ConfigAction::SetSelectedBool(false)),
        KeyCode::Up | KeyCode::Char('k') => Some(ConfigAction::MoveUp),
        KeyCode::Down | KeyCode::Char('j') => Some(ConfigAction::MoveDown),
        KeyCode::Char('r') => Some(ConfigAction::ResetSelected),
        KeyCode::Right | KeyCode::Char('l') => Some(ConfigAction::IncreaseSelected),
        KeyCode::Left | KeyCode::Char('h') => Some(ConfigAction::DecreaseSelected),
        _ => None,
    }
}

pub fn reduce_config_action(
    action: ConfigAction,
    settings_edit: &mut Box<Settings>,
    selected_index: &mut usize,
    items: &mut [ConfigItem],
    editing: &mut Option<(ConfigItem, String)>,
) -> ConfigReduceResult {
    let mut result = ConfigReduceResult::default();
    match action {
        ConfigAction::SaveAndExit => {
            result.consumed = true;
            result.effects.push(ConfigEffect::AppCommand(Box::new(
                AppCommand::UpdateConfig(*settings_edit.clone()),
            )));
            result.effects.push(ConfigEffect::ToNormal);
        }
        ConfigAction::StartEditOrBrowse => {
            result.consumed = true;
            let selected_item = items[*selected_index];
            match selected_item {
                ConfigItem::GlobalDownloadLimit
                | ConfigItem::GlobalUploadLimit
                | ConfigItem::ClientPort => {
                    *editing = Some((selected_item, String::new()));
                }
                ConfigItem::AlwaysShowAddLocationPrompt => {
                    settings_edit.always_show_add_location_prompt =
                        !settings_edit.always_show_add_location_prompt;
                }
                ConfigItem::UiLayoutMode => {
                    settings_edit.ui_layout_mode = settings_edit.ui_layout_mode.next();
                }
                ConfigItem::DefaultDownloadFolder | ConfigItem::WatchFolder => {
                    if shared_path_is_manual(selected_item) {
                        return result;
                    }
                    let initial_path = if selected_item == ConfigItem::WatchFolder {
                        settings_edit.watch_folder.clone()
                    } else {
                        settings_edit.default_download_folder.clone()
                    }
                    .unwrap_or_else(|| {
                        UserDirs::new()
                            .and_then(|ud| ud.download_dir().map(|p| p.to_path_buf()))
                            .unwrap_or_else(|| std::path::PathBuf::from("."))
                    });

                    result.effects.push(ConfigEffect::AppCommand(Box::new(
                        AppCommand::FetchFileTree {
                            browser_generation: 0,
                            path: initial_path,
                            browser_mode: FileBrowserMode::ConfigPathSelection {
                                target_item: selected_item,
                                current_settings: settings_edit.clone(),
                                selected_index: *selected_index,
                                items: items.to_vec(),
                            },
                            preserve_browser_mode: false,
                            highlight_path: None,
                        },
                    )));
                }
            }
        }
        ConfigAction::ToggleSelectedBool => {
            result.consumed = true;
            if items[*selected_index] == ConfigItem::AlwaysShowAddLocationPrompt {
                settings_edit.always_show_add_location_prompt =
                    !settings_edit.always_show_add_location_prompt;
            }
        }
        ConfigAction::SetSelectedBool(value) => {
            result.consumed = true;
            if items[*selected_index] == ConfigItem::AlwaysShowAddLocationPrompt {
                settings_edit.always_show_add_location_prompt = value;
            }
        }
        ConfigAction::MoveUp => {
            result.consumed = true;
            *selected_index = selected_index.saturating_sub(1);
        }
        ConfigAction::MoveDown => {
            result.consumed = true;
            if *selected_index < items.len().saturating_sub(1) {
                *selected_index += 1;
            }
        }
        ConfigAction::ResetSelected => {
            result.consumed = true;
            let default_settings = Settings::default();
            let selected_item = items[*selected_index];
            match selected_item {
                ConfigItem::ClientPort => {
                    settings_edit.client_port = default_settings.client_port;
                }
                ConfigItem::DefaultDownloadFolder => {
                    if !shared_path_is_manual(selected_item) {
                        settings_edit.default_download_folder =
                            default_settings.default_download_folder;
                    }
                }
                ConfigItem::WatchFolder => {
                    settings_edit.watch_folder = default_settings.watch_folder;
                }
                ConfigItem::AlwaysShowAddLocationPrompt => {
                    settings_edit.always_show_add_location_prompt =
                        default_settings.always_show_add_location_prompt;
                }
                ConfigItem::UiLayoutMode => {
                    settings_edit.ui_layout_mode = default_settings.ui_layout_mode;
                }
                ConfigItem::GlobalDownloadLimit => {
                    settings_edit.global_download_limit_bps =
                        default_settings.global_download_limit_bps;
                }
                ConfigItem::GlobalUploadLimit => {
                    settings_edit.global_upload_limit_bps =
                        default_settings.global_upload_limit_bps;
                }
            }
        }
        ConfigAction::IncreaseSelected => {
            result.consumed = true;
            let item = items[*selected_index];
            match item {
                ConfigItem::GlobalDownloadLimit => {
                    let new_rate = increase_rate_limit_bps(settings_edit.global_download_limit_bps);
                    settings_edit.global_download_limit_bps = new_rate;
                    result.effects.push(ConfigEffect::SetDownloadRate(new_rate));
                }
                ConfigItem::GlobalUploadLimit => {
                    let new_rate = increase_rate_limit_bps(settings_edit.global_upload_limit_bps);
                    settings_edit.global_upload_limit_bps = new_rate;
                    result.effects.push(ConfigEffect::SetUploadRate(new_rate));
                }
                ConfigItem::UiLayoutMode => {
                    settings_edit.ui_layout_mode = settings_edit.ui_layout_mode.next();
                }
                _ => {}
            }
        }
        ConfigAction::DecreaseSelected => {
            result.consumed = true;
            let item = items[*selected_index];
            match item {
                ConfigItem::GlobalDownloadLimit => {
                    let new_rate = decrease_rate_limit_bps(settings_edit.global_download_limit_bps);
                    settings_edit.global_download_limit_bps = new_rate;
                    result.effects.push(ConfigEffect::SetDownloadRate(new_rate));
                }
                ConfigItem::GlobalUploadLimit => {
                    let new_rate = decrease_rate_limit_bps(settings_edit.global_upload_limit_bps);
                    settings_edit.global_upload_limit_bps = new_rate;
                    result.effects.push(ConfigEffect::SetUploadRate(new_rate));
                }
                ConfigItem::UiLayoutMode => {
                    settings_edit.ui_layout_mode = settings_edit.ui_layout_mode.previous();
                }
                _ => {}
            }
        }
        ConfigAction::EditInsert(c) => {
            result.consumed = true;
            if let Some((_item, buffer)) = editing {
                buffer.push(c);
            }
        }
        ConfigAction::EditBackspace => {
            result.consumed = true;
            if let Some((_item, buffer)) = editing {
                buffer.pop();
            }
        }
        ConfigAction::EditCancel => {
            result.consumed = true;
            *editing = None;
        }
        ConfigAction::EditCommit => {
            result.consumed = true;
            if let Some((item, buffer)) = editing {
                match item {
                    ConfigItem::ClientPort => {
                        if let Ok(new_port) = buffer.parse::<u16>() {
                            if new_port > 0 {
                                settings_edit.client_port = new_port;
                            }
                        }
                    }
                    ConfigItem::GlobalDownloadLimit => {
                        if let Ok(new_rate) = buffer.parse::<u64>() {
                            settings_edit.global_download_limit_bps = new_rate;
                            result.effects.push(ConfigEffect::SetDownloadRate(new_rate));
                        }
                    }
                    ConfigItem::GlobalUploadLimit => {
                        if let Ok(new_rate) = buffer.parse::<u64>() {
                            settings_edit.global_upload_limit_bps = new_rate;
                            result.effects.push(ConfigEffect::SetUploadRate(new_rate));
                        }
                    }
                    _ => {}
                }
                *editing = None;
            }
        }
    }
    result
}

pub fn draw(
    f: &mut Frame,
    screen: &ScreenContext<'_>,
    settings: &Settings,
    selected_index: usize,
    items: &[ConfigItem],
    editing: &Option<(ConfigItem, String)>,
) {
    let ctx = screen.theme;

    let area = crate::tui::formatters::centered_rect(80, 60, f.area());
    f.render_widget(Clear, f.area());
    let block = Block::default()
        .title(Span::styled(
            "Config",
            ctx.apply(Style::default().fg(ctx.state_selected())),
        ))
        .borders(Borders::ALL)
        .border_style(ctx.apply(Style::default().fg(ctx.theme.semantic.border)));
    let inner_area = block.inner(area);
    f.render_widget(block, area);

    let settings_area = inner_area;
    let footer_y = area.y.saturating_add(area.height);
    let footer_area = if footer_y < f.area().y.saturating_add(f.area().height) {
        ratatui::layout::Rect::new(area.x, footer_y, area.width, 1)
    } else {
        // Fallback for very short terminals: keep commands visible at panel bottom.
        ratatui::layout::Rect::new(
            inner_area.x,
            inner_area.y + inner_area.height.saturating_sub(1),
            inner_area.width,
            1,
        )
    };
    let rows_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            items
                .iter()
                .map(|_| Constraint::Length(1))
                .collect::<Vec<_>>(),
        )
        .split(settings_area);

    for (i, item) in items.iter().enumerate() {
        let (name_str, value_str) = match item {
            ConfigItem::ClientPort => ("Listen Port", settings.client_port.to_string()),
            ConfigItem::DefaultDownloadFolder => (
                "Default Download Folder",
                path_to_string(settings.default_download_folder.as_deref()),
            ),
            ConfigItem::WatchFolder => (
                "Torrent Watch Folder",
                path_to_string(settings.watch_folder.as_deref()),
            ),
            ConfigItem::AlwaysShowAddLocationPrompt => (
                "Always Confirm Add Priority And Location",
                if settings.always_show_add_location_prompt {
                    "[x] true".to_string()
                } else {
                    "[ ] false".to_string()
                },
            ),
            ConfigItem::UiLayoutMode => ("Layout", settings.ui_layout_mode.label().to_string()),
            ConfigItem::GlobalDownloadLimit => (
                "Global DL Limit",
                format_limit_bps(settings.global_download_limit_bps),
            ),
            ConfigItem::GlobalUploadLimit => (
                "Global UL Limit",
                format_limit_bps(settings.global_upload_limit_bps),
            ),
        };

        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(rows_layout[i]);
        let is_highlighted = if let Some((edited_item, _)) = editing {
            *edited_item == *item
        } else {
            i == selected_index
        };
        let row_style = if is_highlighted {
            ctx.apply(Style::default().fg(ctx.state_warning()))
        } else {
            ctx.apply(Style::default().fg(ctx.theme.semantic.text))
        };
        let name_with_selector = if is_highlighted {
            format!("▶ {}", name_str)
        } else {
            format!("  {}", name_str)
        };

        let name_p = Paragraph::new(name_with_selector).style(row_style);
        f.render_widget(name_p, columns[0]);

        if let Some((_edited_item, buffer)) = editing {
            if is_highlighted {
                let edit_p =
                    Paragraph::new(buffer.as_str()).style(row_style.fg(ctx.state_warning()));
                f.set_cursor_position((columns[1].x + buffer.len() as u16, columns[1].y));
                f.render_widget(edit_p, columns[1]);
            } else {
                let value_p = Paragraph::new(value_str).style(row_style);
                f.render_widget(value_p, columns[1]);
            }
        } else {
            let value_p = Paragraph::new(value_str).style(row_style);
            f.render_widget(value_p, columns[1]);
        }
    }

    let shared_path_notice = crate::config::is_shared_config_mode()
        && items.get(selected_index) == Some(&ConfigItem::DefaultDownloadFolder);
    let help_text = if editing.is_some() {
        Line::from(vec![
            Span::styled("[Enter]", footer_key_style(ctx, ActionTone::Confirm)),
            Span::raw(" to confirm, "),
            Span::styled("[Esc]", footer_key_style(ctx, ActionTone::Cancel)),
            Span::raw(" to cancel."),
        ])
    } else if shared_path_notice {
        let settings_label = crate::config::shared_settings_path()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| "settings.toml".to_string());
        Line::from(vec![
            Span::raw("Shared mode: edit Default Download Folder in "),
            Span::styled(
                settings_label,
                ctx.apply(Style::default().fg(ctx.state_warning())),
            ),
            Span::raw(". Host-local fields still save here."),
        ])
    } else if items.get(selected_index) == Some(&ConfigItem::AlwaysShowAddLocationPrompt) {
        Line::from(vec![
            Span::styled("[t]", footer_key_style(ctx, ActionTone::Toggle)),
            Span::raw(" true, "),
            Span::styled("[f]", footer_key_style(ctx, ActionTone::Toggle)),
            Span::raw(" false, "),
            Span::styled("[Enter]|[Space]", footer_key_style(ctx, ActionTone::Toggle)),
            Span::raw(" toggle. "),
            Span::styled("[Esc]|[Q]", footer_key_style(ctx, ActionTone::Confirm)),
            Span::raw(" to Save & Exit."),
        ])
    } else if items.get(selected_index) == Some(&ConfigItem::UiLayoutMode) {
        Line::from(vec![
            Span::styled("←/→/h/l", footer_key_style(ctx, ActionTone::Toggle)),
            Span::raw(" cycle layout. "),
            Span::styled("[Enter]", footer_key_style(ctx, ActionTone::Toggle)),
            Span::raw(" next. "),
            Span::styled("[r]", footer_key_style(ctx, ActionTone::Clear)),
            Span::raw("eset to auto. "),
            Span::styled("[Esc]|[Q]", footer_key_style(ctx, ActionTone::Confirm)),
            Span::raw(" to Save & Exit."),
        ])
    } else {
        Line::from(vec![
            Span::raw("Use "),
            Span::styled("↑/↓/k/j", footer_key_style(ctx, ActionTone::Navigate)),
            Span::raw(" to navigate. "),
            Span::styled("[Enter]", footer_key_style(ctx, ActionTone::Edit)),
            Span::raw(" to edit. "),
            Span::styled("[r]", footer_key_style(ctx, ActionTone::Clear)),
            Span::raw("eset to default. "),
            Span::styled("[Esc]|[Q]", footer_key_style(ctx, ActionTone::Confirm)),
            Span::raw(" to Save & Exit, "),
        ])
    };

    let footer_paragraph = Paragraph::new(help_text)
        .alignment(Alignment::Center)
        .style(ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)));
    f.render_widget(footer_paragraph, footer_area);
}

pub fn handle_event(event: CrosstermEvent, ctx: ConfigHandleContext<'_>) -> bool {
    if let CrosstermEvent::Key(key) = event {
        if key.kind != KeyEventKind::Press {
            return false;
        }
        if let Some(action) = map_key_to_config_action(key.code, ctx.editing) {
            let reduced = reduce_config_action(
                action,
                ctx.settings_edit,
                ctx.selected_index,
                ctx.items,
                ctx.editing,
            );
            for effect in reduced.effects {
                match effect {
                    ConfigEffect::AppCommand(command) => {
                        let mut command = *command;
                        if let AppCommand::FetchFileTree {
                            browser_generation, ..
                        } = &mut command
                        {
                            *ctx.file_browser_generation =
                                ctx.file_browser_generation.wrapping_add(1);
                            *browser_generation = *ctx.file_browser_generation;
                        }
                        spawn_app_command_sender(
                            ctx.app_command_tx.clone(),
                            ctx.shutdown_tx.subscribe(),
                            command,
                        );
                    }
                    ConfigEffect::SetDownloadRate(new_rate) => {
                        let bucket = ctx.global_dl_bucket.clone();
                        tokio::spawn(async move {
                            bucket.set_rate(rate_limit_bps_to_bucket_bytes_per_sec(new_rate));
                        });
                    }
                    ConfigEffect::SetUploadRate(new_rate) => {
                        let bucket = ctx.global_ul_bucket.clone();
                        tokio::spawn(async move {
                            bucket.set_rate(rate_limit_bps_to_bucket_bytes_per_sec(new_rate));
                        });
                    }
                    ConfigEffect::ToNormal => {
                        *ctx.file_browser_generation = ctx.file_browser_generation.wrapping_add(1);
                        *ctx.mode = AppMode::Normal;
                    }
                }
            }
            return reduced.consumed;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_items() -> Vec<ConfigItem> {
        vec![
            ConfigItem::ClientPort,
            ConfigItem::DefaultDownloadFolder,
            ConfigItem::WatchFolder,
            ConfigItem::AlwaysShowAddLocationPrompt,
            ConfigItem::GlobalDownloadLimit,
            ConfigItem::GlobalUploadLimit,
        ]
    }

    #[test]
    fn reducer_move_down_is_clamped() {
        let mut settings = Box::new(Settings::default());
        let mut idx = 0usize;
        let mut items = config_items();
        let mut editing = None;

        for _ in 0..10 {
            let _ = reduce_config_action(
                ConfigAction::MoveDown,
                &mut settings,
                &mut idx,
                items.as_mut_slice(),
                &mut editing,
            );
        }

        assert_eq!(idx, items.len() - 1);
    }

    #[test]
    fn reducer_edit_commit_updates_download_limit_and_emits_effect() {
        let mut settings = Box::new(Settings::default());
        let mut idx = 4usize;
        let mut items = config_items();
        let mut editing = Some((ConfigItem::GlobalDownloadLimit, "123".to_string()));

        let out = reduce_config_action(
            ConfigAction::EditCommit,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );

        assert_eq!(settings.global_download_limit_bps, 123);
        assert_eq!(editing, None);
        assert_eq!(out.effects.len(), 1);
        assert!(matches!(out.effects[0], ConfigEffect::SetDownloadRate(123)));
    }

    #[test]
    fn reducer_rate_limit_arrows_keep_unlimited_as_sentinel() {
        let mut settings = Box::new(Settings::default());
        let mut idx = 4usize;
        let mut items = config_items();
        let mut editing = None;

        let out = reduce_config_action(
            ConfigAction::IncreaseSelected,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert_eq!(settings.global_download_limit_bps, RATE_LIMIT_STEP_BPS);
        assert!(matches!(
            out.effects.as_slice(),
            [ConfigEffect::SetDownloadRate(RATE_LIMIT_STEP_BPS)]
        ));

        let out = reduce_config_action(
            ConfigAction::DecreaseSelected,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert_eq!(settings.global_download_limit_bps, UNLIMITED_RATE_LIMIT_BPS);
        assert!(matches!(
            out.effects.as_slice(),
            [ConfigEffect::SetDownloadRate(UNLIMITED_RATE_LIMIT_BPS)]
        ));

        let out = reduce_config_action(
            ConfigAction::DecreaseSelected,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert_eq!(settings.global_download_limit_bps, 0);
        assert!(matches!(
            out.effects.as_slice(),
            [ConfigEffect::SetDownloadRate(0)]
        ));
    }

    #[test]
    fn reducer_upload_rate_decrease_from_small_cap_returns_to_unlimited() {
        let mut settings = Box::new(Settings::default());
        settings.global_upload_limit_bps = RATE_LIMIT_STEP_BPS / 2;
        let mut idx = 5usize;
        let mut items = config_items();
        let mut editing = None;

        let out = reduce_config_action(
            ConfigAction::DecreaseSelected,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );

        assert_eq!(settings.global_upload_limit_bps, UNLIMITED_RATE_LIMIT_BPS);
        assert!(matches!(
            out.effects.as_slice(),
            [ConfigEffect::SetUploadRate(UNLIMITED_RATE_LIMIT_BPS)]
        ));
    }

    #[test]
    fn reducer_boolean_row_accepts_toggle_true_and_false() {
        let mut settings = Box::new(Settings::default());
        let mut idx = 3usize;
        let mut items = config_items();
        let mut editing = None;

        let out = reduce_config_action(
            ConfigAction::ToggleSelectedBool,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert!(out.consumed);
        assert!(settings.always_show_add_location_prompt);

        let out = reduce_config_action(
            ConfigAction::SetSelectedBool(false),
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert!(out.consumed);
        assert!(!settings.always_show_add_location_prompt);

        let out = reduce_config_action(
            ConfigAction::SetSelectedBool(true),
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );
        assert!(out.consumed);
        assert!(settings.always_show_add_location_prompt);
    }

    #[test]
    fn reducer_save_and_exit_emits_update_config_command() {
        let mut settings = Box::new(Settings::default());
        let mut idx = 0usize;
        let mut items = config_items();
        let mut editing = None;

        let out = reduce_config_action(
            ConfigAction::SaveAndExit,
            &mut settings,
            &mut idx,
            items.as_mut_slice(),
            &mut editing,
        );

        assert_eq!(out.effects.len(), 2);
        assert!(matches!(out.effects[0], ConfigEffect::AppCommand(_)));
        assert!(matches!(out.effects[1], ConfigEffect::ToNormal));
    }
}
