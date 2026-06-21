// SPDX-FileCopyrightText: 2026 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::app::{
    torrent_completion_percent, App, AppCommand, AppMode, AppState, SearchMode,
    TorrentControlState, TorrentDisplayState, TorrentManagementPendingCommand,
};
use crate::config::SortDirection;
use crate::integrations::control::ControlRequest;
use crate::theme::ThemeContext;
use crate::tui::action_style::{footer_key_style, ActionTone};
use crate::tui::app_command::spawn_app_command_batch_sender;
use crate::tui::formatters::{
    anonymize_preserving_shape, centered_rect, format_bytes, format_duration, format_speed,
    sanitize_text, speed_to_style,
};
use crate::tui::layout::common::{compute_smart_table_layout, SmartCol};
use crate::tui::screen_context::ScreenContext;
use crate::tui::screens::input_panel::draw_prompt_panel;
use chrono::{DateTime, Local};
use fuzzy_matcher::skim::SkimMatcherV2;
use fuzzy_matcher::FuzzyMatcher;
use ratatui::crossterm::event::{Event as CrosstermEvent, KeyCode, KeyEventKind};
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::prelude::{Color, Frame, Line, Modifier, Span, Style};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Padding, Paragraph, Row, Table, TableState, Wrap,
};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TorrentManagementAction {
    ToNormal,
    MoveUp,
    MoveDown,
    MoveColumnLeft,
    MoveColumnRight,
    SortBySelectedColumn,
    StartSearch,
    SearchInsert(char),
    SearchBackspace,
    SearchCommit,
    SearchCancel,
    ToggleSearchMode,
    ToggleAnonymizeNames,
    ToggleCurrentSelection,
    SelectAllVisible,
    ClearPendingForTargets,
    OpenHighlightedTorrentFiles,
    TogglePauseTargets,
    StartDelete { delete_files: bool },
    ShowSubmitConfirmation,
    CancelSubmitConfirmation,
    SubmitPendingCommands,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TorrentManagementEffect {
    ToNormal,
    SubmitControlRequest(ControlRequest),
    MarkControlState {
        info_hash: Vec<u8>,
        state: TorrentControlState,
        delete_files: bool,
    },
    OpenExistingTorrentFileBrowser(Vec<u8>),
}

#[derive(Default)]
pub struct TorrentManagementReduceResult {
    pub consumed: bool,
    pub redraw: bool,
    pub effects: Vec<TorrentManagementEffect>,
}

#[derive(Clone, Debug, PartialEq)]
struct ManagementRow {
    kind: ManagementRowKind,
    label: String,
    info_hashes: Vec<Vec<u8>>,
    depth: usize,
    metrics: RowMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ManagementRowKind {
    Torrent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagementColumnId {
    Selection,
    Name,
    Completed,
    State,
    Peers,
    DownSpeed,
    UpSpeed,
    Eta,
    Size,
    DateAdded,
}

#[derive(Clone, Debug)]
struct ManagementColumnDefinition {
    id: ManagementColumnId,
    header: &'static str,
    min_width: u16,
    priority: u8,
    constraint: Constraint,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct RowMetrics {
    count: usize,
    completed: f64,
    state_label: String,
    peer_count: usize,
    download_bps: u64,
    upload_bps: u64,
    eta: Option<Duration>,
    total_size: u64,
    added_at_unix_secs: Option<u64>,
}

#[derive(Default)]
#[cfg(test)]
struct PendingManagementSummary {
    pause_count: usize,
    resume_count: usize,
    remove_count: usize,
    purge_count: usize,
}

#[derive(Default)]
struct PendingManagementReviewGroups {
    pause: Vec<String>,
    resume: Vec<String>,
    delete: Vec<String>,
    purge: Vec<String>,
    purge_total_bytes: u64,
}

pub fn handle_event(event: CrosstermEvent, app: &mut App) -> bool {
    if !matches!(app.app_state.mode, AppMode::TorrentManagement) {
        return false;
    }

    let CrosstermEvent::Key(key) = event else {
        return false;
    };
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return false;
    }

    let Some(action) = map_key_to_management_action(key.code, &app.app_state) else {
        return false;
    };
    let result = reduce_torrent_management_action(&mut app.app_state, action);
    if result.redraw {
        app.app_state.ui.needs_redraw = true;
    }
    execute_management_effects(app, result.effects);
    result.consumed
}

fn map_key_to_management_action(
    key_code: KeyCode,
    app_state: &AppState,
) -> Option<TorrentManagementAction> {
    if app_state.ui.torrent_management.confirm_submit {
        return match key_code {
            KeyCode::Char('Y') => Some(TorrentManagementAction::SubmitPendingCommands),
            KeyCode::Char('u') => Some(TorrentManagementAction::ClearPendingForTargets),
            KeyCode::Esc | KeyCode::Char('q') => {
                Some(TorrentManagementAction::CancelSubmitConfirmation)
            }
            _ => None,
        };
    }

    if app_state.ui.torrent_management.is_searching {
        return match key_code {
            KeyCode::Esc => Some(TorrentManagementAction::SearchCancel),
            KeyCode::Enter => Some(TorrentManagementAction::SearchCommit),
            KeyCode::Tab => Some(TorrentManagementAction::ToggleSearchMode),
            KeyCode::Backspace => Some(TorrentManagementAction::SearchBackspace),
            KeyCode::Char(c) => Some(TorrentManagementAction::SearchInsert(c)),
            _ => None,
        };
    }

    if management_search_panel_active(app_state) && matches!(key_code, KeyCode::Tab) {
        return Some(TorrentManagementAction::ToggleSearchMode);
    }

    match key_code {
        KeyCode::Esc | KeyCode::Char('q') => Some(TorrentManagementAction::ToNormal),
        KeyCode::Up | KeyCode::Char('k') => Some(TorrentManagementAction::MoveUp),
        KeyCode::Down | KeyCode::Char('j') => Some(TorrentManagementAction::MoveDown),
        KeyCode::Left | KeyCode::Char('h') => Some(TorrentManagementAction::MoveColumnLeft),
        KeyCode::Right | KeyCode::Char('l') => Some(TorrentManagementAction::MoveColumnRight),
        KeyCode::Char('s') => Some(TorrentManagementAction::SortBySelectedColumn),
        KeyCode::Char('/') => Some(TorrentManagementAction::StartSearch),
        KeyCode::Char('x') => Some(TorrentManagementAction::ToggleAnonymizeNames),
        KeyCode::Char('Y') if !app_state.ui.torrent_management.pending_commands.is_empty() => {
            Some(TorrentManagementAction::ShowSubmitConfirmation)
        }
        KeyCode::Char(' ') => Some(TorrentManagementAction::ToggleCurrentSelection),
        KeyCode::Char('A') => Some(TorrentManagementAction::SelectAllVisible),
        KeyCode::Char('u') => Some(TorrentManagementAction::ClearPendingForTargets),
        KeyCode::Char('f') => Some(TorrentManagementAction::OpenHighlightedTorrentFiles),
        KeyCode::Char('p') => Some(TorrentManagementAction::TogglePauseTargets),
        KeyCode::Char('d') => Some(TorrentManagementAction::StartDelete {
            delete_files: false,
        }),
        KeyCode::Char('D') => Some(TorrentManagementAction::StartDelete { delete_files: true }),
        _ => None,
    }
}

pub fn reduce_torrent_management_action(
    app_state: &mut AppState,
    action: TorrentManagementAction,
) -> TorrentManagementReduceResult {
    let mut result = TorrentManagementReduceResult {
        consumed: true,
        redraw: true,
        effects: Vec::new(),
    };
    app_state.ui.torrent_management.status_message = None;
    prune_selected_hashes(app_state);

    match action {
        TorrentManagementAction::ToNormal => {
            app_state.ui.torrent_management.is_searching = false;
            app_state.ui.torrent_management.search_query.clear();
            app_state.ui.torrent_management.pending_commands.clear();
            app_state.ui.torrent_management.selected_hashes.clear();
            app_state.ui.torrent_management.confirm_submit = false;
            result.effects.push(TorrentManagementEffect::ToNormal);
        }
        TorrentManagementAction::MoveUp => {
            app_state.ui.torrent_management.selected_index = app_state
                .ui
                .torrent_management
                .selected_index
                .saturating_sub(1);
        }
        TorrentManagementAction::MoveDown => {
            let row_count = build_management_rows(app_state).len();
            if row_count > 0 {
                app_state.ui.torrent_management.selected_index =
                    (app_state.ui.torrent_management.selected_index + 1).min(row_count - 1);
            }
        }
        TorrentManagementAction::MoveColumnLeft => {
            move_management_column(app_state, -1);
        }
        TorrentManagementAction::MoveColumnRight => {
            move_management_column(app_state, 1);
        }
        TorrentManagementAction::SortBySelectedColumn => {
            let selected_column_index = normalized_selected_management_column_index(app_state);
            app_state.ui.torrent_management.selected_column_index = selected_column_index;
            if app_state.ui.torrent_management.sort_column_index == Some(selected_column_index) {
                app_state.ui.torrent_management.sort_direction =
                    reverse_sort_direction(app_state.ui.torrent_management.sort_direction);
            } else {
                app_state.ui.torrent_management.sort_column_index = Some(selected_column_index);
                app_state.ui.torrent_management.sort_direction =
                    management_column_default_direction(
                        management_columns()[selected_column_index].id,
                    );
            }
        }
        TorrentManagementAction::StartSearch => {
            app_state.ui.torrent_management.is_searching = true;
            app_state.ui.torrent_management.selected_index = 0;
        }
        TorrentManagementAction::SearchInsert(c) => {
            app_state.ui.torrent_management.search_query.push(c);
            app_state.ui.torrent_management.selected_index = 0;
        }
        TorrentManagementAction::SearchBackspace => {
            app_state.ui.torrent_management.search_query.pop();
            app_state.ui.torrent_management.selected_index = 0;
        }
        TorrentManagementAction::SearchCommit => {
            app_state.ui.torrent_management.is_searching = false;
        }
        TorrentManagementAction::SearchCancel => {
            app_state.ui.torrent_management.is_searching = false;
            app_state.ui.torrent_management.search_query.clear();
            app_state.ui.torrent_management.selected_index = 0;
        }
        TorrentManagementAction::ToggleSearchMode => {
            app_state.ui.torrent_management.search_mode =
                match app_state.ui.torrent_management.search_mode {
                    SearchMode::Fuzzy => SearchMode::Regex,
                    SearchMode::Regex => SearchMode::Fuzzy,
                };
            app_state.ui.torrent_management.selected_index = 0;
        }
        TorrentManagementAction::ToggleAnonymizeNames => {
            app_state.anonymize_torrent_names = !app_state.anonymize_torrent_names;
        }
        TorrentManagementAction::ToggleCurrentSelection => {
            let targets = current_row_targets(app_state);
            toggle_hash_selection(app_state, &targets);
        }
        TorrentManagementAction::SelectAllVisible => {
            app_state.ui.torrent_management.selected_hashes.clear();
            for hash in visible_torrent_hashes(app_state) {
                app_state.ui.torrent_management.selected_hashes.insert(hash);
            }
            let selected_count = app_state.ui.torrent_management.selected_hashes.len();
            app_state.ui.torrent_management.status_message =
                Some(format!("Selected {selected_count} visible torrents"));
        }
        TorrentManagementAction::ClearPendingForTargets => {
            let targets = management_targets(app_state);
            let target_set = targets.into_iter().collect::<HashSet<_>>();
            let cleared = clear_pending_management_commands_for_targets(app_state, &target_set);
            app_state.ui.torrent_management.status_message = if cleared == 0 {
                Some("No draft commands to clear".to_string())
            } else {
                Some(format!("Cleared {cleared} draft commands"))
            };
        }
        TorrentManagementAction::OpenHighlightedTorrentFiles => {
            if let Some(info_hash) = current_row_targets(app_state).into_iter().next() {
                result
                    .effects
                    .push(TorrentManagementEffect::OpenExistingTorrentFileBrowser(
                        info_hash,
                    ));
            } else {
                app_state.ui.torrent_management.status_message =
                    Some("No torrent highlighted".to_string());
            }
        }
        TorrentManagementAction::TogglePauseTargets => {
            let targets = management_targets(app_state);
            if targets.is_empty() {
                app_state.ui.torrent_management.status_message =
                    Some("No torrents selected".to_string());
            } else {
                for info_hash in targets {
                    let should_resume = app_state.torrents.get(&info_hash).is_some_and(|torrent| {
                        torrent.latest_state.torrent_control_state == TorrentControlState::Paused
                    });
                    let state = if should_resume {
                        TorrentControlState::Running
                    } else {
                        TorrentControlState::Paused
                    };
                    let request = if should_resume {
                        ControlRequest::Resume {
                            info_hash_hex: hex::encode(&info_hash),
                        }
                    } else {
                        ControlRequest::Pause {
                            info_hash_hex: hex::encode(&info_hash),
                        }
                    };
                    toggle_pending_management_command(
                        app_state,
                        TorrentManagementPendingCommand {
                            info_hash,
                            request,
                            state,
                            delete_files: false,
                        },
                    );
                }
                app_state.ui.torrent_management.selected_hashes.clear();
                app_state.ui.torrent_management.status_message =
                    Some(pending_management_status(app_state));
            }
        }
        TorrentManagementAction::StartDelete { delete_files } => {
            let targets = management_targets(app_state);
            if targets.is_empty() {
                app_state.ui.torrent_management.status_message =
                    Some("No torrents selected".to_string());
            } else {
                for info_hash in targets {
                    toggle_pending_management_command(
                        app_state,
                        TorrentManagementPendingCommand {
                            request: ControlRequest::Delete {
                                info_hash_hex: hex::encode(&info_hash),
                                delete_files,
                            },
                            info_hash: info_hash.clone(),
                            state: TorrentControlState::Deleting,
                            delete_files,
                        },
                    );
                }
                app_state.ui.torrent_management.selected_hashes.clear();
                app_state.ui.torrent_management.status_message =
                    Some(pending_management_status(app_state));
            }
        }
        TorrentManagementAction::ShowSubmitConfirmation => {
            if app_state.ui.torrent_management.pending_commands.is_empty() {
                app_state.ui.torrent_management.status_message =
                    Some("No draft commands to submit".to_string());
            } else {
                app_state.ui.torrent_management.confirm_submit = true;
            }
        }
        TorrentManagementAction::CancelSubmitConfirmation => {
            app_state.ui.torrent_management.confirm_submit = false;
        }
        TorrentManagementAction::SubmitPendingCommands => {
            app_state.ui.torrent_management.confirm_submit = false;
            let pending_commands =
                std::mem::take(&mut app_state.ui.torrent_management.pending_commands);
            if pending_commands.is_empty() {
                app_state.ui.torrent_management.status_message =
                    Some("No draft commands to submit".to_string());
            } else {
                for command in pending_commands {
                    result
                        .effects
                        .push(TorrentManagementEffect::SubmitControlRequest(
                            command.request,
                        ));
                    result
                        .effects
                        .push(TorrentManagementEffect::MarkControlState {
                            info_hash: command.info_hash.clone(),
                            state: command.state,
                            delete_files: command.delete_files,
                        });
                }
                app_state.ui.torrent_management.selected_hashes.clear();
                app_state.ui.torrent_management.status_message =
                    Some("Draft commands submitted".to_string());
            }
        }
    }

    clamp_management_selection(app_state);
    clamp_management_column_state(app_state);
    result
}
fn execute_management_effects(app: &mut App, effects: Vec<TorrentManagementEffect>) {
    let mut control_requests = Vec::new();
    for effect in effects {
        match effect {
            TorrentManagementEffect::ToNormal => {
                app.app_state.mode = AppMode::Normal;
            }
            TorrentManagementEffect::SubmitControlRequest(request) => {
                control_requests.push(request);
            }
            TorrentManagementEffect::MarkControlState {
                info_hash,
                state,
                delete_files,
            } => {
                if !app.is_current_shared_follower() {
                    if let Some(torrent) = app.app_state.torrents.get_mut(&info_hash) {
                        torrent.latest_state.torrent_control_state = state;
                        torrent.latest_state.delete_files = delete_files;
                    }
                }
            }
            TorrentManagementEffect::OpenExistingTorrentFileBrowser(info_hash) => {
                app.open_existing_torrent_file_browser(info_hash);
            }
        }
    }
    if !control_requests.is_empty() {
        spawn_app_command_batch_sender(
            app.app_command_tx.clone(),
            app.shutdown_tx.subscribe(),
            control_requests
                .into_iter()
                .map(AppCommand::SubmitControlRequest)
                .collect(),
        );
    }
}

pub fn draw(f: &mut Frame, screen: &ScreenContext<'_>) {
    let app_state = screen.app.state;
    let ctx = screen.theme;
    let area = f.area();
    f.render_widget(Clear, area);
    let content_area = management_content_area(area);

    let search_panel_active = management_search_panel_active(app_state);
    let chunks = if search_panel_active {
        Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(content_area)
    } else {
        Layout::vertical([Constraint::Min(5), Constraint::Length(1)]).split(content_area)
    };

    let (table_area, footer_area) = if search_panel_active {
        draw_management_search_panel(f, app_state, chunks[0], ctx);
        (chunks[1], chunks[2])
    } else {
        (chunks[0], chunks[1])
    };

    draw_management_table(f, app_state, table_area, ctx);
    if !app_state.ui.torrent_management.confirm_submit {
        draw_management_footer(f, app_state, footer_area, ctx);
    }

    if app_state.ui.torrent_management.confirm_submit {
        draw_management_review_panel(f, app_state, ctx);
    }
}

fn management_content_area(area: Rect) -> Rect {
    if area.width < 90 || area.height < 18 {
        return area;
    }

    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

fn management_search_panel_active(app_state: &AppState) -> bool {
    app_state.ui.torrent_management.is_searching
        || !app_state.ui.torrent_management.search_query.is_empty()
}

fn draw_management_search_panel(
    f: &mut Frame,
    app_state: &AppState,
    area: Rect,
    ctx: &ThemeContext,
) {
    draw_prompt_panel(
        f,
        area,
        " Torrent Search ".to_string(),
        sanitize_text(&app_state.ui.torrent_management.search_query),
        management_search_mode_spans(app_state, ctx),
        ctx,
    );
}

fn management_search_mode_spans(app_state: &AppState, ctx: &ThemeContext) -> Vec<Span<'static>> {
    let (fuzzy_style, regex_style) = match app_state.ui.torrent_management.search_mode {
        SearchMode::Fuzzy => (
            ctx.apply(Style::default().fg(ctx.state_selected()).bold()),
            ctx.apply(Style::default().fg(ctx.theme.semantic.overlay0)),
        ),
        SearchMode::Regex => (
            ctx.apply(Style::default().fg(ctx.theme.semantic.overlay0)),
            ctx.apply(Style::default().fg(ctx.state_selected()).bold()),
        ),
    };
    vec![
        Span::raw("  "),
        Span::styled("Fuzzy", fuzzy_style),
        Span::raw(" / "),
        Span::styled("Regex", regex_style),
    ]
}

fn draw_management_table(f: &mut Frame, app_state: &AppState, area: Rect, ctx: &ThemeContext) {
    let rows = build_management_rows(app_state);
    let all_columns = management_columns();
    let (constraints, visible_columns) = compute_visible_management_columns(area.width);
    let mut table_state = TableState::default();
    if !rows.is_empty() {
        table_state.select(Some(
            app_state
                .ui
                .torrent_management
                .selected_index
                .min(rows.len().saturating_sub(1)),
        ));
    }

    let table_rows = rows
        .iter()
        .enumerate()
        .map(|(idx, row)| management_table_row(app_state, row, idx, ctx, &visible_columns))
        .collect::<Vec<_>>();

    let header = Row::new(
        visible_columns
            .iter()
            .map(|&idx| {
                let column = &all_columns[idx];
                let is_selected = idx
                    == normalized_selected_column_from_visible(
                        app_state.ui.torrent_management.selected_column_index,
                        &visible_columns,
                    );
                let is_sorting = app_state.ui.torrent_management.sort_column_index == Some(idx);
                let mut style =
                    ctx.apply(Style::default().fg(management_column_header_color(column.id, ctx)));
                if is_sorting {
                    style = ctx.apply(style.bold());
                }

                let mut spans = vec![Span::styled(column.header, style)];
                if is_sorting {
                    spans.push(Span::styled(
                        management_sort_arrow(
                            column.id,
                            app_state.ui.torrent_management.sort_direction,
                        ),
                        style,
                    ));
                }
                if is_selected {
                    spans[0] = spans[0].clone().style(
                        ctx.apply(
                            Style::default()
                                .fg(ctx.theme.scale.categorical.lavender)
                                .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
                        ),
                    );
                }
                Cell::from(Line::from(spans))
            })
            .collect::<Vec<_>>(),
    )
    .style(ctx.apply(Style::default().fg(ctx.state_warning()).bold()));

    let table = Table::new(table_rows, constraints).header(header).block(
        Block::default()
            .title(Span::styled(
                " Torrents ",
                ctx.apply(Style::default().fg(ctx.state_selected())),
            ))
            .borders(Borders::ALL)
            .border_style(ctx.apply(Style::default().fg(ctx.theme.semantic.border)))
            .padding(Padding::new(1, 1, 0, 0)),
    );
    f.render_stateful_widget(table, area, &mut table_state);

    if rows.is_empty() {
        let inner = Rect::new(
            area.x.saturating_add(1),
            area.y.saturating_add(1),
            area.width.saturating_sub(2),
            area.height.saturating_sub(2),
        );
        let message = if app_state.ui.torrent_management.search_query.is_empty() {
            "No torrents"
        } else {
            "No torrents match the search"
        };
        f.render_widget(
            Paragraph::new(message)
                .alignment(Alignment::Center)
                .style(ctx.apply(Style::default().fg(ctx.theme.semantic.surface2))),
            centered_line_rect(inner),
        );
    }
}

fn management_table_row<'a>(
    app_state: &AppState,
    row: &ManagementRow,
    row_index: usize,
    ctx: &ThemeContext,
    visible_columns: &[usize],
) -> Row<'a> {
    let selected_state = row_selection_state(app_state, row);
    let pending_label = pending_management_label_for_row(app_state, row);
    let reviewing_changes = app_state.ui.torrent_management.confirm_submit;
    let has_pending_action = matches!(row.kind, ManagementRowKind::Torrent)
        && row
            .info_hashes
            .iter()
            .any(|hash| pending_management_command_for_hash(app_state, hash).is_some());
    let pending_action_style = pending_management_review_style_for_row(app_state, row, ctx)
        .unwrap_or_else(|| ctx.apply(Style::default().fg(ctx.theme.semantic.surface2)));
    let affected_by_review = reviewing_changes && has_pending_action;
    let selection_marker = management_selection_marker(selected_state, has_pending_action);

    let row_is_cursor = app_state.ui.torrent_management.selected_index == row_index;
    let row_style = if row_is_cursor && !reviewing_changes {
        ctx.apply(Style::default().fg(ctx.state_warning()).bold())
    } else if !matches!(selected_state, SelectionState::None) {
        ctx.apply(
            Style::default()
                .fg(ctx.theme.scale.categorical.lavender)
                .bold(),
        )
    } else if affected_by_review || has_pending_action {
        pending_action_style
    } else if row.metrics.state_label == "Paused" {
        ctx.apply(Style::default().fg(ctx.theme.semantic.surface1))
    } else if row.metrics.state_label == "Deleting" {
        ctx.apply(Style::default().fg(ctx.state_error()))
    } else {
        ctx.apply(Style::default().fg(ctx.theme.semantic.text))
    };
    let pending_cell_style = pending_label.is_some().then_some(row_style);

    let name_prefix = if row.depth > 0 { "  " } else { "" };
    let name = match &row.kind {
        ManagementRowKind::Torrent => format!("{name_prefix}{}", row.label),
    };

    let all_columns = management_columns();
    let cells = visible_columns
        .iter()
        .map(|&idx| match all_columns[idx].id {
            ManagementColumnId::Selection => {
                let cell = Cell::from(selection_marker);
                if let Some(style) = pending_cell_style {
                    cell.style(style)
                } else {
                    cell
                }
            }
            ManagementColumnId::Name => Cell::from(name.clone()),
            ManagementColumnId::DateAdded => {
                Cell::from(format_added_date(row.metrics.added_at_unix_secs))
            }
            ManagementColumnId::Completed => Cell::from(format!("{:.0}%", row.metrics.completed)),
            ManagementColumnId::State => {
                let cell = Cell::from(
                    pending_label
                        .clone()
                        .unwrap_or_else(|| row.metrics.state_label.clone()),
                );
                if let Some(style) = pending_cell_style {
                    cell.style(style)
                } else {
                    cell
                }
            }
            ManagementColumnId::Peers => Cell::from(row.metrics.peer_count.to_string()),
            ManagementColumnId::DownSpeed => management_speed_cell(ctx, row.metrics.download_bps),
            ManagementColumnId::UpSpeed => management_speed_cell(ctx, row.metrics.upload_bps),
            ManagementColumnId::Eta => Cell::from(
                row.metrics
                    .eta
                    .map(format_duration)
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ManagementColumnId::Size => Cell::from(format_bytes(row.metrics.total_size)),
        })
        .collect::<Vec<_>>();

    Row::new(cells).style(row_style)
}

fn management_column_header_color(column: ManagementColumnId, ctx: &ThemeContext) -> Color {
    match column {
        ManagementColumnId::Selection => ctx.theme.semantic.subtext1,
        ManagementColumnId::Name => ctx.accent_sky(),
        ManagementColumnId::Eta => ctx.accent_teal(),
        ManagementColumnId::Completed => ctx.state_success(),
        ManagementColumnId::State => ctx.metric_upload(),
        ManagementColumnId::Peers => ctx.state_info(),
        ManagementColumnId::DownSpeed => ctx.metric_download(),
        ManagementColumnId::UpSpeed => ctx.accent_sapphire(),
        ManagementColumnId::Size => ctx.theme.semantic.text,
        ManagementColumnId::DateAdded => ctx.state_error(),
    }
}

fn management_selection_marker(
    selected_state: SelectionState,
    has_pending_action: bool,
) -> &'static str {
    if has_pending_action {
        return "!";
    }

    match selected_state {
        SelectionState::None => "-",
        SelectionState::Partial => "~",
        SelectionState::Full => "x",
    }
}

fn draw_management_footer(f: &mut Frame, app_state: &AppState, area: Rect, ctx: &ThemeContext) {
    if area.height == 0 {
        return;
    }

    let mut footer_spans = Vec::new();
    let mut push_action = |key: &str, action: &str, tone: ActionTone| {
        footer_spans.push(Span::styled(
            format!("[{key}]"),
            footer_key_style(ctx, tone),
        ));
        footer_spans.push(Span::styled(
            action.to_string(),
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext0)),
        ));
        footer_spans.push(Span::styled(
            " | ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.overlay0)),
        ));
    };

    if app_state.ui.torrent_management.confirm_submit {
        push_action("shift+y", "finalize changes", ActionTone::Confirm);
        push_action("Esc", "cancel", ActionTone::Cancel);
    } else if app_state.ui.torrent_management.is_searching {
        push_action("Enter", "apply", ActionTone::Confirm);
        push_action("Tab", "mode", ActionTone::Mode);
        push_action("Esc", "clear", ActionTone::Cancel);
    } else {
        let pending_count = app_state.ui.torrent_management.pending_commands.len();
        if pending_count > 0 {
            push_action("shift+y", "review", ActionTone::Confirm);
        }
        push_action("arrows", "nav", ActionTone::Navigate);
        push_action("s", "ort", ActionTone::Sort);
        push_action("Space", "select", ActionTone::Select);
        push_action("A", "select-all", ActionTone::Select);
        push_action("u", "clear", ActionTone::Clear);
        push_action("f", "files", ActionTone::Navigate);
        push_action("/", "search", ActionTone::Search);
        if management_search_panel_active(app_state) {
            push_action("Tab", "mode", ActionTone::Mode);
        }
        push_action("x", "names", ActionTone::Toggle);
        push_action("p", "ause", ActionTone::Queue);
        push_action("d/D", "remove/purge", ActionTone::Destructive);
        push_action("Esc", "back", ActionTone::Cancel);
    }

    if !footer_spans.is_empty() {
        footer_spans.pop();
    }

    let footer = Paragraph::new(Line::from(footer_spans))
        .alignment(Alignment::Center)
        .style(ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)));
    f.render_widget(footer, area);
}

fn draw_management_review_panel(f: &mut Frame, app_state: &AppState, ctx: &ThemeContext) {
    let groups = pending_management_review_groups(app_state);
    let max_area = centered_rect(72, 44, f.area());
    let width = pending_management_review_popup_width(&groups, max_area.width);
    let area = Rect::new(
        f.area().x + f.area().width.saturating_sub(width) / 2,
        max_area.y,
        width,
        max_area.height,
    );
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(Span::styled(
            " Review Changes ",
            ctx.apply(Style::default().fg(ctx.state_selected()).bold()),
        ))
        .borders(Borders::ALL)
        .border_style(ctx.apply(Style::default().fg(ctx.theme.semantic.border)))
        .padding(Padding::new(2, 2, 1, 1));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut body = Vec::new();
    push_pending_review_section(
        &mut body,
        "Pause",
        &groups.pause,
        None,
        ctx.theme.semantic.surface2,
        ctx,
    );
    if !groups.resume.is_empty() {
        push_pending_review_section(
            &mut body,
            "Resume",
            &groups.resume,
            None,
            ctx.state_success(),
            ctx,
        );
    }
    push_pending_review_section(
        &mut body,
        "Remove from client",
        &groups.delete,
        None,
        ctx.state_warning(),
        ctx,
    );
    push_pending_review_section(
        &mut body,
        "Purge torrent and files",
        &groups.purge,
        Some(format_gb(groups.purge_total_bytes)),
        ctx.state_error(),
        ctx,
    );

    f.render_widget(
        Paragraph::new(body)
            .alignment(Alignment::Left)
            .wrap(Wrap { trim: false }),
        inner,
    );
    draw_management_review_footer(f, area, ctx);
}

fn pending_management_review_popup_width(
    groups: &PendingManagementReviewGroups,
    max_width: u16,
) -> u16 {
    let mut longest = " Review Changes ".chars().count();
    for (title, names, detail, include_empty) in [
        ("Pause", groups.pause.as_slice(), None, true),
        (
            "Resume",
            groups.resume.as_slice(),
            None,
            !groups.resume.is_empty(),
        ),
        ("Remove from client", groups.delete.as_slice(), None, true),
        (
            "Purge torrent and files",
            groups.purge.as_slice(),
            Some(format_gb(groups.purge_total_bytes)),
            true,
        ),
    ] {
        if !include_empty {
            continue;
        }
        longest = longest.max(
            section_header_text(title, names.len(), detail.as_deref())
                .chars()
                .count(),
        );
        if names.is_empty() {
            longest = longest.max("  None".chars().count());
        } else {
            for name in names {
                longest = longest.max(format!("  {name}").chars().count());
            }
        }
    }

    let max_width = max_width as usize;
    let desired = longest.saturating_add(6).min(max_width);
    desired.max(1).max(32.min(max_width)) as u16
}

fn draw_management_review_footer(f: &mut Frame, popup_area: Rect, ctx: &ThemeContext) {
    let y = popup_area.y.saturating_add(popup_area.height);
    if y >= f.area().height {
        return;
    }

    let footer_area = Rect::new(popup_area.x, y, popup_area.width, 1);
    let footer = Paragraph::new(Line::from(vec![
        Span::styled("[shift+y]", footer_key_style(ctx, ActionTone::Confirm)),
        Span::styled(
            "finalize changes",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext0)),
        ),
        Span::styled(
            " | ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.overlay0)),
        ),
        Span::styled("[Esc]", footer_key_style(ctx, ActionTone::Cancel)),
        Span::styled(
            "cancel",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext0)),
        ),
    ]))
    .alignment(Alignment::Center)
    .style(ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)));
    f.render_widget(footer, footer_area);
}

fn push_pending_review_section(
    body: &mut Vec<Line<'static>>,
    title: &str,
    names: &[String],
    detail: Option<String>,
    color: Color,
    ctx: &ThemeContext,
) {
    body.push(Line::from(vec![
        Span::styled(
            title.to_string(),
            ctx.apply(Style::default().fg(color).bold()),
        ),
        Span::styled(
            section_header_suffix(names.len(), detail.as_deref()),
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
    ]));

    if names.is_empty() {
        body.push(Line::from(Span::styled(
            "  None",
            ctx.apply(Style::default().fg(ctx.theme.semantic.overlay0)),
        )));
    } else {
        for name in names {
            body.push(Line::from(format!("  {name}")));
        }
    }
    body.push(Line::from(""));
}

fn section_header_text(title: &str, count: usize, detail: Option<&str>) -> String {
    format!("{title}{}", section_header_suffix(count, detail))
}

fn section_header_suffix(count: usize, detail: Option<&str>) -> String {
    let noun = if count == 1 { "Torrent" } else { "Torrents" };
    match detail {
        Some(detail) => format!(": {count} {noun} ({detail})"),
        None => format!(": {count} {noun}"),
    }
}

fn centered_line_rect(area: Rect) -> Rect {
    Rect::new(
        area.x,
        area.y + area.height.saturating_sub(1) / 2,
        area.width,
        1,
    )
}

fn management_columns() -> Vec<ManagementColumnDefinition> {
    vec![
        ManagementColumnDefinition {
            id: ManagementColumnId::Selection,
            header: "=",
            min_width: 2,
            priority: 0,
            constraint: Constraint::Length(2),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::Name,
            header: "Name",
            min_width: 20,
            priority: 0,
            constraint: Constraint::Fill(3),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::Eta,
            header: "ETA",
            min_width: 9,
            priority: 4,
            constraint: Constraint::Length(9),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::Completed,
            header: "Done",
            min_width: 7,
            priority: 2,
            constraint: Constraint::Length(7),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::State,
            header: "Action",
            min_width: 8,
            priority: 2,
            constraint: Constraint::Length(8),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::Peers,
            header: "Peers",
            min_width: 7,
            priority: 3,
            constraint: Constraint::Length(7),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::DownSpeed,
            header: "DL",
            min_width: 10,
            priority: 1,
            constraint: Constraint::Length(10),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::UpSpeed,
            header: "UL",
            min_width: 10,
            priority: 1,
            constraint: Constraint::Length(10),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::Size,
            header: "Size",
            min_width: 10,
            priority: 5,
            constraint: Constraint::Length(10),
        },
        ManagementColumnDefinition {
            id: ManagementColumnId::DateAdded,
            header: "Added",
            min_width: 10,
            priority: 5,
            constraint: Constraint::Length(10),
        },
    ]
}

fn compute_visible_management_columns(available_width: u16) -> (Vec<Constraint>, Vec<usize>) {
    let columns = management_columns();
    let smart_columns = columns
        .iter()
        .map(|column| SmartCol {
            min_width: column.min_width,
            priority: column.priority,
            constraint: column.constraint,
        })
        .collect::<Vec<_>>();
    compute_smart_table_layout(&smart_columns, available_width.saturating_sub(4), 1)
}

#[cfg(test)]
fn visible_management_column_ids(available_width: u16) -> Vec<ManagementColumnId> {
    let columns = management_columns();
    let (_, visible_indices) = compute_visible_management_columns(available_width);
    visible_indices
        .into_iter()
        .map(|idx| columns[idx].id)
        .collect()
}

fn management_speed_cell<'a>(ctx: &ThemeContext, speed_bps: u64) -> Cell<'a> {
    Cell::from(format_speed(speed_bps)).style(ctx.apply(speed_to_style(ctx, speed_bps)))
}

fn management_table_width_for_state(app_state: &AppState) -> u16 {
    if app_state.screen_area.width > 0 {
        app_state.screen_area.width
    } else {
        140
    }
}

fn visible_management_column_indices_for_state(app_state: &AppState) -> Vec<usize> {
    compute_visible_management_columns(management_table_width_for_state(app_state)).1
}

fn normalized_selected_column_from_visible(
    selected_index: usize,
    visible_columns: &[usize],
) -> usize {
    if visible_columns.is_empty() {
        return management_column_index(ManagementColumnId::Name).unwrap_or(0);
    }
    if visible_columns.contains(&selected_index) {
        return selected_index;
    }
    visible_columns
        .iter()
        .copied()
        .rfind(|idx| *idx <= selected_index)
        .or_else(|| visible_columns.first().copied())
        .unwrap_or(0)
}

fn normalized_selected_management_column_index(app_state: &AppState) -> usize {
    normalized_selected_column_from_visible(
        app_state.ui.torrent_management.selected_column_index,
        &visible_management_column_indices_for_state(app_state),
    )
}

fn move_management_column(app_state: &mut AppState, direction: isize) {
    let visible_columns = visible_management_column_indices_for_state(app_state);
    if visible_columns.is_empty() {
        return;
    }

    let current = normalized_selected_column_from_visible(
        app_state.ui.torrent_management.selected_column_index,
        &visible_columns,
    );
    let current_pos = visible_columns
        .iter()
        .position(|idx| *idx == current)
        .unwrap_or(0);
    let next_pos = if direction < 0 {
        current_pos.saturating_sub(1)
    } else {
        (current_pos + 1).min(visible_columns.len().saturating_sub(1))
    };
    app_state.ui.torrent_management.selected_column_index = visible_columns[next_pos];
}

fn reverse_sort_direction(direction: SortDirection) -> SortDirection {
    match direction {
        SortDirection::Ascending => SortDirection::Descending,
        SortDirection::Descending => SortDirection::Ascending,
    }
}

fn management_column_default_direction(column: ManagementColumnId) -> SortDirection {
    if management_column_is_numeric(column) {
        SortDirection::Descending
    } else {
        SortDirection::Ascending
    }
}

fn management_column_is_numeric(column: ManagementColumnId) -> bool {
    matches!(
        column,
        ManagementColumnId::Completed
            | ManagementColumnId::Peers
            | ManagementColumnId::DownSpeed
            | ManagementColumnId::UpSpeed
            | ManagementColumnId::Eta
            | ManagementColumnId::Size
            | ManagementColumnId::DateAdded
    )
}

fn management_sort_arrow(column: ManagementColumnId, direction: SortDirection) -> &'static str {
    match (management_column_is_numeric(column), direction) {
        (true, SortDirection::Descending) | (false, SortDirection::Ascending) => " ▼",
        (true, SortDirection::Ascending) | (false, SortDirection::Descending) => " ▲",
    }
}

fn management_sort_column(app_state: &AppState) -> Option<ManagementColumnId> {
    let columns = management_columns();
    app_state
        .ui
        .torrent_management
        .sort_column_index
        .and_then(|idx| columns.get(idx))
        .map(|column| column.id)
}

fn management_column_index(column_id: ManagementColumnId) -> Option<usize> {
    management_columns()
        .iter()
        .position(|column| column.id == column_id)
}

fn sort_management_rows(app_state: &AppState, rows: &mut [ManagementRow]) {
    if management_sort_column(app_state).is_some() {
        rows.sort_by(|left, right| compare_management_rows(app_state, left, right));
    }
}

fn compare_management_rows(
    app_state: &AppState,
    left: &ManagementRow,
    right: &ManagementRow,
) -> Ordering {
    let Some(column) = management_sort_column(app_state) else {
        return Ordering::Equal;
    };
    let ordering = match column {
        ManagementColumnId::Selection => {
            selection_sort_rank(app_state, left).cmp(&selection_sort_rank(app_state, right))
        }
        ManagementColumnId::Name => left.label.cmp(&right.label),
        ManagementColumnId::Completed => left.metrics.completed.total_cmp(&right.metrics.completed),
        ManagementColumnId::State => left.metrics.state_label.cmp(&right.metrics.state_label),
        ManagementColumnId::DateAdded => left
            .metrics
            .added_at_unix_secs
            .unwrap_or(0)
            .cmp(&right.metrics.added_at_unix_secs.unwrap_or(0)),
        ManagementColumnId::Peers => left.metrics.peer_count.cmp(&right.metrics.peer_count),
        ManagementColumnId::DownSpeed => left.metrics.download_bps.cmp(&right.metrics.download_bps),
        ManagementColumnId::UpSpeed => left.metrics.upload_bps.cmp(&right.metrics.upload_bps),
        ManagementColumnId::Eta => left.metrics.eta.cmp(&right.metrics.eta),
        ManagementColumnId::Size => left.metrics.total_size.cmp(&right.metrics.total_size),
    };

    apply_sort_direction(ordering, app_state.ui.torrent_management.sort_direction)
        .then_with(|| left.label.cmp(&right.label))
        .then_with(|| left.info_hashes.len().cmp(&right.info_hashes.len()))
}

fn apply_sort_direction(ordering: Ordering, direction: SortDirection) -> Ordering {
    match direction {
        SortDirection::Ascending => ordering,
        SortDirection::Descending => ordering.reverse(),
    }
}

fn selection_sort_rank(app_state: &AppState, row: &ManagementRow) -> usize {
    match row_selection_state(app_state, row) {
        SelectionState::None => 0,
        SelectionState::Partial => 1,
        SelectionState::Full => 2,
    }
}

fn build_management_rows(app_state: &AppState) -> Vec<ManagementRow> {
    let visible = visible_torrent_hashes(app_state);
    let mut rows = visible
        .into_iter()
        .filter_map(|info_hash| torrent_row(app_state, info_hash, 0))
        .collect::<Vec<_>>();
    sort_management_rows(app_state, &mut rows);
    rows
}

fn torrent_row(app_state: &AppState, info_hash: Vec<u8>, depth: usize) -> Option<ManagementRow> {
    let torrent = app_state.torrents.get(&info_hash)?;
    let label = if app_state.anonymize_torrent_names {
        anonymize_preserving_shape(&torrent.latest_state.torrent_name)
    } else {
        sanitize_text(&torrent.latest_state.torrent_name)
    };
    Some(ManagementRow {
        kind: ManagementRowKind::Torrent,
        label,
        info_hashes: vec![info_hash.clone()],
        depth,
        metrics: aggregate_metrics_for_hashes(app_state, vec![info_hash]),
    })
}

fn visible_torrent_hashes(app_state: &AppState) -> Vec<Vec<u8>> {
    let query = app_state.ui.torrent_management.search_query.trim();
    let mode = app_state.ui.torrent_management.search_mode;
    let matcher = SkimMatcherV2::default();
    ordered_torrent_hashes(app_state)
        .into_iter()
        .filter(|info_hash| {
            app_state
                .torrents
                .get(info_hash)
                .is_some_and(|torrent| torrent_matches_query(torrent, query, mode, &matcher))
        })
        .collect()
}

fn ordered_torrent_hashes(app_state: &AppState) -> Vec<Vec<u8>> {
    if !app_state.torrent_list_order.is_empty() {
        let mut hashes = app_state.torrents.keys().cloned().collect::<Vec<_>>();
        hashes.sort_by(|a, b| {
            let a_rank = app_state
                .torrent_list_order
                .iter()
                .position(|hash| hash == a);
            let b_rank = app_state
                .torrent_list_order
                .iter()
                .position(|hash| hash == b);
            match (a_rank, b_rank) {
                (Some(a_rank), Some(b_rank)) => a_rank.cmp(&b_rank),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => torrent_name(app_state, a).cmp(torrent_name(app_state, b)),
            }
        });
        return hashes;
    }

    let mut hashes = app_state.torrents.keys().cloned().collect::<Vec<_>>();
    hashes.sort_by(|a, b| torrent_name(app_state, a).cmp(torrent_name(app_state, b)));
    hashes
}

fn torrent_name<'a>(app_state: &'a AppState, info_hash: &[u8]) -> &'a str {
    app_state
        .torrents
        .get(info_hash)
        .map(|torrent| torrent.latest_state.torrent_name.as_str())
        .unwrap_or_default()
}

fn torrent_matches_query(
    torrent: &TorrentDisplayState,
    query: &str,
    mode: SearchMode,
    matcher: &SkimMatcherV2,
) -> bool {
    if query.is_empty() {
        return true;
    }

    let mut haystack = torrent.latest_state.torrent_name.clone();
    if let Some(path) = &torrent.latest_state.download_path {
        haystack.push(' ');
        haystack.push_str(&path.to_string_lossy());
    }
    if let Some(container) = &torrent.latest_state.container_name {
        haystack.push(' ');
        haystack.push_str(container);
    }
    match mode {
        SearchMode::Fuzzy => matcher
            .fuzzy_match(&haystack.to_lowercase(), &query.to_lowercase())
            .is_some(),
        SearchMode::Regex => regex::RegexBuilder::new(query)
            .case_insensitive(true)
            .build()
            .ok()
            .is_some_and(|re| re.is_match(&haystack)),
    }
}

fn aggregate_metrics_for_hashes<I>(app_state: &AppState, hashes: I) -> RowMetrics
where
    I: IntoIterator<Item = Vec<u8>>,
{
    let mut count = 0usize;
    let mut peer_count = 0usize;
    let mut download_bps = 0u64;
    let mut upload_bps = 0u64;
    let mut total_size = 0u64;
    let mut latest_added_at_unix_secs = None::<u64>;
    let mut weighted_done = 0f64;
    let mut unweighted_done = 0f64;
    let mut weighted_total = 0u64;
    let mut max_eta = Duration::ZERO;
    let mut any_incomplete = false;
    let mut states = HashSet::new();

    for info_hash in hashes {
        let Some(torrent) = app_state.torrents.get(&info_hash) else {
            continue;
        };
        let state = &torrent.latest_state;
        count += 1;
        peer_count += state
            .number_of_successfully_connected_peers
            .max(state.peers.len());
        download_bps = download_bps.saturating_add(torrent.smoothed_download_speed_bps);
        upload_bps = upload_bps.saturating_add(torrent.smoothed_upload_speed_bps);
        total_size = total_size.saturating_add(state.total_size);
        latest_added_at_unix_secs = latest_added_at_unix_secs.max(torrent.added_at_unix_secs);
        states.insert(state.torrent_control_state.clone());

        let pct = torrent_completion_percent(state).clamp(0.0, 100.0);
        unweighted_done += pct;
        if state.total_size > 0 {
            weighted_done += pct * state.total_size as f64;
            weighted_total = weighted_total.saturating_add(state.total_size);
        }
        if pct < 100.0 {
            any_incomplete = true;
            max_eta = max_eta.max(state.eta);
        }
    }

    let completed = if weighted_total > 0 {
        (weighted_done / weighted_total as f64).clamp(0.0, 100.0)
    } else if count > 0 {
        (unweighted_done / count as f64).clamp(0.0, 100.0)
    } else {
        0.0
    };

    RowMetrics {
        count,
        completed,
        state_label: aggregate_state_label(&states, count),
        peer_count,
        download_bps,
        upload_bps,
        eta: (any_incomplete && !max_eta.is_zero()).then_some(max_eta),
        total_size,
        added_at_unix_secs: latest_added_at_unix_secs,
    }
}

fn format_added_date(added_at_unix_secs: Option<u64>) -> String {
    let Some(added_at_unix_secs) = added_at_unix_secs else {
        return "-".to_string();
    };
    let system_time = UNIX_EPOCH + Duration::from_secs(added_at_unix_secs);
    let datetime: DateTime<Local> = system_time.into();
    datetime.format("%Y-%m-%d").to_string()
}

fn aggregate_state_label(states: &HashSet<TorrentControlState>, count: usize) -> String {
    if count == 0 {
        return "-".to_string();
    }
    if states.contains(&TorrentControlState::Deleting) {
        return "Deleting".to_string();
    }
    if states.len() > 1 {
        return "Mixed".to_string();
    }
    if states.contains(&TorrentControlState::Paused) {
        "Paused".to_string()
    } else {
        "Running".to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectionState {
    None,
    Partial,
    Full,
}

fn row_selection_state(app_state: &AppState, row: &ManagementRow) -> SelectionState {
    let selected = &app_state.ui.torrent_management.selected_hashes;
    let selected_count = row
        .info_hashes
        .iter()
        .filter(|hash| selected.contains(*hash))
        .count();
    match selected_count {
        0 => SelectionState::None,
        count if count == row.info_hashes.len() => SelectionState::Full,
        _ => SelectionState::Partial,
    }
}

fn toggle_hash_selection(app_state: &mut AppState, targets: &[Vec<u8>]) {
    if targets.is_empty() {
        return;
    }
    let selected = &mut app_state.ui.torrent_management.selected_hashes;
    if targets.iter().all(|hash| selected.contains(hash)) {
        for hash in targets {
            selected.remove(hash);
        }
    } else {
        for hash in targets {
            selected.insert(hash.clone());
        }
    }
}

fn current_row_targets(app_state: &AppState) -> Vec<Vec<u8>> {
    build_management_rows(app_state)
        .get(app_state.ui.torrent_management.selected_index)
        .map(|row| row.info_hashes.clone())
        .unwrap_or_default()
}

fn management_targets(app_state: &AppState) -> Vec<Vec<u8>> {
    if !app_state.ui.torrent_management.selected_hashes.is_empty() {
        let visible: HashSet<Vec<u8>> = visible_torrent_hashes(app_state).into_iter().collect();
        return app_state
            .ui
            .torrent_management
            .selected_hashes
            .iter()
            .filter(|hash| visible.contains(*hash))
            .cloned()
            .collect();
    }

    current_row_targets(app_state)
}

fn toggle_pending_management_command(
    app_state: &mut AppState,
    command: TorrentManagementPendingCommand,
) {
    if app_state
        .ui
        .torrent_management
        .pending_commands
        .iter()
        .any(|pending| pending.info_hash == command.info_hash && pending.request == command.request)
    {
        app_state
            .ui
            .torrent_management
            .pending_commands
            .retain(|pending| pending.info_hash != command.info_hash);
        return;
    }

    app_state
        .ui
        .torrent_management
        .pending_commands
        .retain(|pending| pending.info_hash != command.info_hash);
    app_state
        .ui
        .torrent_management
        .pending_commands
        .push(command);
}

fn clear_pending_management_commands_for_targets(
    app_state: &mut AppState,
    targets: &HashSet<Vec<u8>>,
) -> usize {
    let before = app_state.ui.torrent_management.pending_commands.len();
    app_state
        .ui
        .torrent_management
        .pending_commands
        .retain(|pending| !targets.contains(&pending.info_hash));
    before.saturating_sub(app_state.ui.torrent_management.pending_commands.len())
}

fn pending_management_status(app_state: &AppState) -> String {
    let pending_count = app_state.ui.torrent_management.pending_commands.len();
    format!("{pending_count} draft commands pending")
}

#[cfg(test)]
fn pending_management_summary(app_state: &AppState) -> PendingManagementSummary {
    let mut summary = PendingManagementSummary::default();
    for command in &app_state.ui.torrent_management.pending_commands {
        match &command.request {
            ControlRequest::Pause { .. } => summary.pause_count += 1,
            ControlRequest::Resume { .. } => summary.resume_count += 1,
            ControlRequest::Delete {
                delete_files: true, ..
            } => summary.purge_count += 1,
            ControlRequest::Delete {
                delete_files: false,
                ..
            } => summary.remove_count += 1,
            _ => {}
        }
    }
    summary
}

fn pending_management_review_groups(app_state: &AppState) -> PendingManagementReviewGroups {
    let mut groups = PendingManagementReviewGroups::default();
    for command in &app_state.ui.torrent_management.pending_commands {
        let name = pending_management_command_display_name(app_state, command);
        match &command.request {
            ControlRequest::Pause { .. } => groups.pause.push(name),
            ControlRequest::Resume { .. } => groups.resume.push(name),
            ControlRequest::Delete {
                delete_files: true, ..
            } => {
                groups.purge_total_bytes = groups
                    .purge_total_bytes
                    .saturating_add(pending_management_command_total_size(app_state, command));
                groups.purge.push(name);
            }
            ControlRequest::Delete {
                delete_files: false,
                ..
            } => groups.delete.push(name),
            _ => {}
        }
    }
    groups.pause.sort();
    groups.resume.sort();
    groups.delete.sort();
    groups.purge.sort();
    groups
}

fn pending_management_command_total_size(
    app_state: &AppState,
    command: &TorrentManagementPendingCommand,
) -> u64 {
    app_state
        .torrents
        .get(&command.info_hash)
        .map(|torrent| torrent.latest_state.total_size)
        .unwrap_or(0)
}

fn format_gb(bytes: u64) -> String {
    format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
}

fn pending_management_command_display_name(
    app_state: &AppState,
    command: &TorrentManagementPendingCommand,
) -> String {
    if let Some(torrent) = app_state.torrents.get(&command.info_hash) {
        if app_state.anonymize_torrent_names {
            anonymize_preserving_shape(&torrent.latest_state.torrent_name)
        } else {
            sanitize_text(&torrent.latest_state.torrent_name)
        }
    } else {
        let hash = hex::encode(&command.info_hash);
        format!(
            "unknown torrent {}",
            hash.chars().take(8).collect::<String>()
        )
    }
}

fn pending_management_command_for_hash<'a>(
    app_state: &'a AppState,
    info_hash: &[u8],
) -> Option<&'a TorrentManagementPendingCommand> {
    app_state
        .ui
        .torrent_management
        .pending_commands
        .iter()
        .find(|command| command.info_hash == info_hash)
}

fn pending_management_review_style_for_row(
    app_state: &AppState,
    row: &ManagementRow,
    ctx: &ThemeContext,
) -> Option<Style> {
    let mut style = None;
    for hash in &row.info_hashes {
        let Some(command) = pending_management_command_for_hash(app_state, hash) else {
            continue;
        };
        let next = match &command.request {
            ControlRequest::Pause { .. } => {
                ctx.apply(Style::default().fg(ctx.theme.semantic.surface2))
            }
            ControlRequest::Resume { .. } => ctx.apply(Style::default().fg(ctx.state_success())),
            ControlRequest::Delete {
                delete_files: false,
                ..
            } => ctx.apply(Style::default().fg(ctx.state_warning())),
            ControlRequest::Delete {
                delete_files: true, ..
            } => ctx.apply(Style::default().fg(ctx.state_error())),
            _ => continue,
        };

        style = Some(next);
        if matches!(
            command.request,
            ControlRequest::Delete {
                delete_files: true,
                ..
            }
        ) {
            break;
        }
    }
    style
}

fn pending_management_label_for_row(app_state: &AppState, row: &ManagementRow) -> Option<String> {
    let mut matching_commands = row
        .info_hashes
        .iter()
        .filter_map(|hash| pending_management_command_for_hash(app_state, hash));
    matching_commands.next().map(|_| "Review".to_string())
}

fn prune_selected_hashes(app_state: &mut AppState) {
    let live_hashes: HashSet<Vec<u8>> = app_state.torrents.keys().cloned().collect();
    app_state
        .ui
        .torrent_management
        .selected_hashes
        .retain(|hash| live_hashes.contains(hash));
    app_state
        .ui
        .torrent_management
        .pending_commands
        .retain(|command| live_hashes.contains(&command.info_hash));
}

fn clamp_management_selection(app_state: &mut AppState) {
    let row_count = build_management_rows(app_state).len();
    if row_count == 0 {
        app_state.ui.torrent_management.selected_index = 0;
    } else if app_state.ui.torrent_management.selected_index >= row_count {
        app_state.ui.torrent_management.selected_index = row_count - 1;
    }
}

fn clamp_management_column_state(app_state: &mut AppState) {
    let columns_len = management_columns().len();
    if columns_len == 0 {
        app_state.ui.torrent_management.selected_column_index = 0;
        app_state.ui.torrent_management.sort_column_index = None;
        return;
    }

    if app_state.ui.torrent_management.selected_column_index >= columns_len {
        app_state.ui.torrent_management.selected_column_index = columns_len - 1;
    }
    if app_state
        .ui
        .torrent_management
        .sort_column_index
        .is_some_and(|idx| idx >= columns_len)
    {
        app_state.ui.torrent_management.sort_column_index = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{TorrentMetrics, UiState};

    fn hash(byte: u8) -> Vec<u8> {
        vec![byte; 20]
    }

    fn app_state_with_torrents(torrents: Vec<(Vec<u8>, &str, u64, u64, usize)>) -> AppState {
        let mut app_state = AppState {
            mode: AppMode::TorrentManagement,
            ui: UiState::default(),
            ..Default::default()
        };

        for (idx, (info_hash, name, download_bps, upload_bps, peers)) in
            torrents.into_iter().enumerate()
        {
            let mut metrics = TorrentMetrics {
                info_hash: info_hash.clone(),
                torrent_name: name.to_string(),
                number_of_pieces_total: 100,
                number_of_pieces_completed: 50,
                number_of_successfully_connected_peers: peers,
                total_size: 1_000 + idx as u64,
                eta: Duration::from_secs(30 + idx as u64),
                ..Default::default()
            };
            metrics.peers = Vec::new();
            app_state.torrents.insert(
                info_hash.clone(),
                TorrentDisplayState {
                    latest_state: metrics,
                    added_at_unix_secs: Some(1_700_000_000 + idx as u64 * 86_400),
                    smoothed_download_speed_bps: download_bps,
                    smoothed_upload_speed_bps: upload_bps,
                    ..Default::default()
                },
            );
            app_state.torrent_list_order.push(info_hash);
        }

        app_state
    }

    #[test]
    fn management_columns_keep_core_identity_on_tiny_widths() {
        let visible = visible_management_column_ids(45);

        assert_eq!(
            visible,
            vec![ManagementColumnId::Selection, ManagementColumnId::Name]
        );
    }

    #[test]
    fn management_columns_prioritize_speeds_on_medium_widths() {
        let visible = visible_management_column_ids(80);

        assert!(visible.contains(&ManagementColumnId::Selection));
        assert!(visible.contains(&ManagementColumnId::Name));
        assert!(visible.contains(&ManagementColumnId::DownSpeed));
        assert!(visible.contains(&ManagementColumnId::UpSpeed));
        assert!(!visible.contains(&ManagementColumnId::Eta));
        assert!(!visible.contains(&ManagementColumnId::Size));
    }

    #[test]
    fn management_columns_restore_all_metrics_on_wide_widths() {
        let visible = visible_management_column_ids(150);

        assert_eq!(
            visible,
            vec![
                ManagementColumnId::Selection,
                ManagementColumnId::Name,
                ManagementColumnId::Eta,
                ManagementColumnId::Completed,
                ManagementColumnId::State,
                ManagementColumnId::Peers,
                ManagementColumnId::DownSpeed,
                ManagementColumnId::UpSpeed,
                ManagementColumnId::Size,
                ManagementColumnId::DateAdded,
            ]
        );
    }

    #[test]
    fn management_content_area_insets_roomy_viewports() {
        let area = Rect::new(0, 0, 120, 32);

        assert_eq!(management_content_area(area), Rect::new(1, 1, 118, 30));
    }

    #[test]
    fn management_content_area_keeps_compact_viewports_full_width() {
        let area = Rect::new(0, 0, 78, 16);

        assert_eq!(management_content_area(area), area);
    }

    #[test]
    fn management_keymap_moves_columns_and_sorts_selected_column() {
        let app_state = app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);

        assert_eq!(
            map_key_to_management_action(KeyCode::Left, &app_state),
            Some(TorrentManagementAction::MoveColumnLeft)
        );
        assert_eq!(
            map_key_to_management_action(KeyCode::Right, &app_state),
            Some(TorrentManagementAction::MoveColumnRight)
        );
        assert_eq!(
            map_key_to_management_action(KeyCode::Char('s'), &app_state),
            Some(TorrentManagementAction::SortBySelectedColumn)
        );
    }

    #[test]
    fn management_keymap_opens_highlighted_torrent_files() {
        let app_state = app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);

        assert_eq!(
            map_key_to_management_action(KeyCode::Char('f'), &app_state),
            Some(TorrentManagementAction::OpenHighlightedTorrentFiles)
        );
    }

    #[test]
    fn open_highlighted_torrent_files_ignores_multi_select_targets() {
        let first_hash = hash(1);
        let second_hash = hash(2);
        let mut app_state = app_state_with_torrents(vec![
            (first_hash.clone(), "Harbor Lights S01E01", 50, 5, 1),
            (second_hash.clone(), "Harbor Lights S01E02", 60, 6, 2),
        ]);
        app_state.ui.torrent_management.selected_index = 1;
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(first_hash);

        let result = reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::OpenHighlightedTorrentFiles,
        );

        assert_eq!(
            result.effects,
            vec![TorrentManagementEffect::OpenExistingTorrentFileBrowser(
                second_hash
            )]
        );
    }

    #[test]
    fn management_column_movement_stays_on_visible_columns() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.screen_area = Rect::new(0, 0, 80, 24);
        app_state.ui.torrent_management.selected_column_index =
            management_column_index(ManagementColumnId::Name).expect("name column");

        reduce_torrent_management_action(&mut app_state, TorrentManagementAction::MoveColumnRight);

        let visible = visible_management_column_ids(app_state.screen_area.width);
        assert!(visible.contains(
            &management_columns()[app_state.ui.torrent_management.selected_column_index].id
        ));
    }

    #[test]
    fn default_management_sort_is_name_ascending() {
        let app_state = app_state_with_torrents(vec![
            (hash(1), "Zephyr Archive", 100, 10, 2),
            (hash(2), "Aurora Archive", 100, 10, 2),
        ]);
        let rows = build_management_rows(&app_state);

        assert_eq!(rows[0].label, "Aurora Archive");
        assert_eq!(
            app_state.ui.torrent_management.sort_column_index,
            management_column_index(ManagementColumnId::Name)
        );
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn sorting_by_download_speed_orders_rows_descending_then_toggles() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Slower Seed", 100, 10, 2),
            (hash(2), "Faster Seed", 900, 20, 3),
        ]);
        app_state.ui.torrent_management.selected_column_index =
            management_column_index(ManagementColumnId::DownSpeed).expect("download column");

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SortBySelectedColumn,
        );
        let rows = build_management_rows(&app_state);
        assert_eq!(rows[0].label, "Faster Seed");
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Descending
        );

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SortBySelectedColumn,
        );
        let rows = build_management_rows(&app_state);
        assert_eq!(rows[0].label, "Slower Seed");
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn sorting_unsorted_numeric_column_starts_highest_first_with_down_arrow() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Fewer Peers", 100, 10, 2),
            (hash(2), "More Peers", 100, 10, 7),
        ]);
        app_state.ui.torrent_management.sort_column_index = None;
        app_state.ui.torrent_management.selected_column_index =
            management_column_index(ManagementColumnId::Peers).expect("peers column");

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SortBySelectedColumn,
        );

        let rows = build_management_rows(&app_state);
        assert_eq!(rows[0].label, "More Peers");
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Descending
        );
        assert_eq!(
            management_sort_arrow(ManagementColumnId::Peers, SortDirection::Descending),
            " ▼"
        );
    }

    #[test]
    fn sorting_by_date_added_orders_newest_first_then_toggles() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Older Seed", 100, 10, 2),
            (hash(2), "Newer Seed", 100, 10, 2),
        ]);
        app_state.ui.torrent_management.selected_column_index =
            management_column_index(ManagementColumnId::DateAdded).expect("date added column");

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SortBySelectedColumn,
        );
        let rows = build_management_rows(&app_state);
        assert_eq!(rows[0].label, "Newer Seed");
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Descending
        );

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SortBySelectedColumn,
        );
        let rows = build_management_rows(&app_state);
        assert_eq!(rows[0].label, "Older Seed");
        assert_eq!(
            app_state.ui.torrent_management.sort_direction,
            SortDirection::Ascending
        );
    }

    #[test]
    fn date_added_formats_as_local_calendar_date_or_dash() {
        assert_eq!(format_added_date(None), "-");

        let rendered = format_added_date(Some(1_700_000_000));

        assert_eq!(rendered.len(), 10);
        assert_eq!(rendered.chars().nth(4), Some('-'));
        assert_eq!(rendered.chars().nth(7), Some('-'));
    }

    #[test]
    fn pending_action_marker_overrides_selection_marker() {
        assert_eq!(management_selection_marker(SelectionState::None, true), "!");
        assert_eq!(
            management_selection_marker(SelectionState::Partial, true),
            "!"
        );
        assert_eq!(management_selection_marker(SelectionState::Full, true), "!");
        assert_eq!(
            management_selection_marker(SelectionState::Full, false),
            "x"
        );
    }

    #[test]
    fn selection_marker_column_uses_equals_header_and_compact_values() {
        let selection_column = management_columns()
            .into_iter()
            .find(|column| column.id == ManagementColumnId::Selection)
            .expect("selection column");

        assert_eq!(selection_column.header, "=");
        assert_eq!(selection_column.min_width, 2);
        assert_eq!(selection_column.constraint, Constraint::Length(2));
        assert_eq!(
            management_selection_marker(SelectionState::None, false),
            "-"
        );
        assert_eq!(
            management_selection_marker(SelectionState::Partial, false),
            "~"
        );
        assert_eq!(
            management_selection_marker(SelectionState::Full, false),
            "x"
        );
    }

    #[test]
    fn management_speed_cells_use_shared_speed_palette() {
        let ctx = ThemeContext::new(Default::default(), 0.0);
        let cell = management_speed_cell(&ctx, 2_100_000);

        assert_eq!(
            ratatui::style::Styled::style(&cell).fg,
            Some(ctx.theme.scale.speed[3])
        );
    }

    #[test]
    fn management_zero_speed_cells_inherit_row_style() {
        let ctx = ThemeContext::new(Default::default(), 0.0);
        let cell = management_speed_cell(&ctx, 0);

        assert_eq!(ratatui::style::Styled::style(&cell).fg, None);
    }

    #[test]
    fn search_filters_torrent_rows_without_mutating_dashboard_search() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Harbor Lights S01E01", 50, 5, 1),
        ]);
        app_state.ui.search_query = "normal".to_string();
        app_state.ui.torrent_management.search_query = "harbor".to_string();

        let rows = build_management_rows(&app_state);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].label, "Harbor Lights S01E01");
        assert_eq!(app_state.ui.search_query, "normal");
    }

    #[test]
    fn empty_management_search_ignores_cached_normal_search_subset() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Harbor Lights S01E01", 50, 5, 1),
            (hash(3), "Orchard Notes S01E01", 75, 8, 1),
        ]);
        app_state.ui.search_query = "harbor".to_string();
        app_state.torrent_list_order = vec![hash(2)];
        app_state.ui.torrent_management.search_query.clear();

        let rows = build_management_rows(&app_state);

        assert_eq!(rows.len(), 3);
        assert!(rows.iter().any(|row| row.info_hashes == vec![hash(1)]));
        assert!(rows.iter().any(|row| row.info_hashes == vec![hash(2)]));
        assert!(rows.iter().any(|row| row.info_hashes == vec![hash(3)]));
    }

    #[test]
    fn committed_management_search_keeps_search_panel_visible() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Harbor Lights S01E01", 50, 5, 1),
        ]);
        app_state.ui.torrent_management.is_searching = true;
        app_state.ui.torrent_management.search_query = "harbor".to_string();

        reduce_torrent_management_action(&mut app_state, TorrentManagementAction::SearchCommit);

        assert!(!app_state.ui.torrent_management.is_searching);
        assert_eq!(app_state.ui.torrent_management.search_query, "harbor");
        assert!(management_search_panel_active(&app_state));
    }

    #[test]
    fn empty_management_search_panel_stays_hidden_outside_search_mode() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.ui.torrent_management.is_searching = false;
        app_state.ui.torrent_management.search_query.clear();

        assert!(!management_search_panel_active(&app_state));
    }

    #[test]
    fn tab_toggles_management_search_mode_while_searching() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.ui.torrent_management.is_searching = true;
        assert!(matches!(
            app_state.ui.torrent_management.search_mode,
            SearchMode::Regex
        ));
        assert_eq!(
            map_key_to_management_action(KeyCode::Tab, &app_state),
            Some(TorrentManagementAction::ToggleSearchMode)
        );

        reduce_torrent_management_action(&mut app_state, TorrentManagementAction::ToggleSearchMode);

        assert!(matches!(
            app_state.ui.torrent_management.search_mode,
            SearchMode::Fuzzy
        ));
    }

    #[test]
    fn tab_toggles_management_search_mode_for_committed_search() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.ui.torrent_management.is_searching = false;
        app_state.ui.torrent_management.search_query = "Harbor".to_string();

        assert_eq!(
            map_key_to_management_action(KeyCode::Tab, &app_state),
            Some(TorrentManagementAction::ToggleSearchMode)
        );
    }

    #[test]
    fn regex_management_search_filters_torrent_rows() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Harbor Lights S01E02", 50, 5, 1),
        ]);
        app_state.ui.torrent_management.search_mode = SearchMode::Regex;
        app_state.ui.torrent_management.search_query = r"S01E0[12]".to_string();

        let rows = build_management_rows(&app_state);

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn invalid_regex_management_search_matches_no_rows() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.ui.torrent_management.search_mode = SearchMode::Regex;
        app_state.ui.torrent_management.search_query = "[".to_string();

        let rows = build_management_rows(&app_state);

        assert!(rows.is_empty());
    }

    #[test]
    fn anonymized_torrent_rows_hide_release_markers() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor.Lights S01E01", 50, 5, 1)]);
        app_state.anonymize_torrent_names = true;

        let rows = build_management_rows(&app_state);
        let anonymized = &rows[0].label;

        assert_ne!(anonymized, "Harbor.Lights S01E01");
        assert_ne!(anonymized, "Torrent 1");
        assert!(!anonymized.contains('.'));
        assert!(!anonymized.chars().any(|ch| ch.is_ascii_digit()));
        assert!(!anonymized.contains("  "));
        assert!(anonymized.matches(' ').count() >= 1);
    }

    #[test]
    fn anonymized_rows_hide_numbered_episode_markers() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state.anonymize_torrent_names = true;

        let rows = build_management_rows(&app_state);
        let anonymized = &rows[0].label;

        assert_ne!(anonymized, "Meadow Saga S01E01");
        assert!(!anonymized.chars().any(|ch| ch.is_ascii_digit()));
        assert!(!anonymized.contains("  "));
        assert!(anonymized.matches(' ').count() >= 2);
    }

    #[test]
    fn x_toggles_anonymized_names_in_management_screen() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        assert!(!app_state.anonymize_torrent_names);
        assert_eq!(
            map_key_to_management_action(KeyCode::Char('x'), &app_state),
            Some(TorrentManagementAction::ToggleAnonymizeNames)
        );

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ToggleAnonymizeNames,
        );
        assert!(app_state.anonymize_torrent_names);

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ToggleAnonymizeNames,
        );
        assert!(!app_state.anonymize_torrent_names);
    }

    #[test]
    fn x_still_types_into_management_search() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Harbor Lights S01E01", 50, 5, 1)]);
        app_state.ui.torrent_management.is_searching = true;

        assert_eq!(
            map_key_to_management_action(KeyCode::Char('x'), &app_state),
            Some(TorrentManagementAction::SearchInsert('x'))
        );
    }

    #[test]
    fn toggle_current_selection_selects_current_torrent_row() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);

        let result = reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ToggleCurrentSelection,
        );

        assert!(result.consumed);
        assert!(app_state
            .ui
            .torrent_management
            .selected_hashes
            .contains(&hash(1)));
        assert_eq!(app_state.ui.torrent_management.selected_hashes.len(), 1);
    }

    #[test]
    fn pause_action_stages_batch_pause_requests_for_selected_torrents() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(1));
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(2));

        let result = reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );

        assert!(result.effects.is_empty());
        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 2);
        assert!(matches!(
            app_state.ui.torrent_management.pending_commands[0].request,
            ControlRequest::Pause { .. }
        ));
        assert!(matches!(
            app_state.ui.torrent_management.pending_commands[1].request,
            ControlRequest::Pause { .. }
        ));
        assert!(app_state.ui.torrent_management.selected_hashes.is_empty());
        assert_eq!(
            map_key_to_management_action(KeyCode::Char('Y'), &app_state),
            Some(TorrentManagementAction::ShowSubmitConfirmation)
        );
        assert_eq!(
            map_key_to_management_action(KeyCode::Enter, &app_state),
            None
        );
    }

    #[test]
    fn pause_action_toggles_each_selected_torrent_independently() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state
            .torrents
            .get_mut(&hash(1))
            .expect("paused torrent")
            .latest_state
            .torrent_control_state = TorrentControlState::Paused;
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(1));
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(2));

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );

        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 2);
        assert!(app_state
            .ui
            .torrent_management
            .pending_commands
            .iter()
            .any(|command| command.info_hash == hash(1)
                && matches!(command.request, ControlRequest::Resume { .. })));
        assert!(app_state
            .ui
            .torrent_management
            .pending_commands
            .iter()
            .any(|command| command.info_hash == hash(2)
                && matches!(command.request, ControlRequest::Pause { .. })));
    }

    #[test]
    fn select_all_pause_select_all_clear_removes_all_pending_drafts() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);

        reduce_torrent_management_action(&mut app_state, TorrentManagementAction::SelectAllVisible);
        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );
        assert!(app_state.ui.torrent_management.selected_hashes.is_empty());
        reduce_torrent_management_action(&mut app_state, TorrentManagementAction::SelectAllVisible);
        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ClearPendingForTargets,
        );

        assert!(app_state.ui.torrent_management.pending_commands.is_empty());
        assert_eq!(app_state.ui.torrent_management.selected_hashes.len(), 2);
    }

    #[test]
    fn submit_confirmation_shift_y_emits_staged_requests_and_marks_state() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state
            .ui
            .torrent_management
            .pending_commands
            .push(TorrentManagementPendingCommand {
                info_hash: hash(1),
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(1)),
                },
                state: TorrentControlState::Paused,
                delete_files: false,
            });
        app_state
            .ui
            .torrent_management
            .pending_commands
            .push(TorrentManagementPendingCommand {
                info_hash: hash(2),
                request: ControlRequest::Delete {
                    info_hash_hex: hex::encode(hash(2)),
                    delete_files: true,
                },
                state: TorrentControlState::Deleting,
                delete_files: true,
            });
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(2));

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ShowSubmitConfirmation,
        );
        assert!(app_state.ui.torrent_management.confirm_submit);
        assert_eq!(
            map_key_to_management_action(KeyCode::Char('Y'), &app_state),
            Some(TorrentManagementAction::SubmitPendingCommands)
        );
        assert_eq!(
            map_key_to_management_action(KeyCode::Enter, &app_state),
            None
        );

        let result = reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::SubmitPendingCommands,
        );

        assert!(!app_state.ui.torrent_management.confirm_submit);
        assert!(app_state.ui.torrent_management.pending_commands.is_empty());
        assert_eq!(result.effects.len(), 4);
        assert!(matches!(
            result.effects[0],
            TorrentManagementEffect::SubmitControlRequest(ControlRequest::Pause { .. })
        ));
        assert!(matches!(
            result.effects[2],
            TorrentManagementEffect::SubmitControlRequest(ControlRequest::Delete { .. })
        ));
        assert!(app_state.ui.torrent_management.selected_hashes.is_empty());
    }

    #[test]
    fn exiting_management_clears_pending_draft_and_filter() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Meadow Saga S01E01", 100, 10, 2)]);
        app_state
            .ui
            .torrent_management
            .pending_commands
            .push(TorrentManagementPendingCommand {
                info_hash: hash(1),
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(1)),
                },
                state: TorrentControlState::Paused,
                delete_files: false,
            });
        app_state.ui.torrent_management.confirm_submit = true;
        app_state.ui.torrent_management.is_searching = true;
        app_state.ui.torrent_management.search_query = "meadow".to_string();
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(1));

        let result =
            reduce_torrent_management_action(&mut app_state, TorrentManagementAction::ToNormal);

        assert!(matches!(
            result.effects.as_slice(),
            [TorrentManagementEffect::ToNormal]
        ));
        assert!(app_state.ui.torrent_management.pending_commands.is_empty());
        assert!(!app_state.ui.torrent_management.confirm_submit);
        assert!(!app_state.ui.torrent_management.is_searching);
        assert!(app_state.ui.torrent_management.search_query.is_empty());
        assert!(app_state.ui.torrent_management.selected_hashes.is_empty());
    }

    #[test]
    fn u_clears_pending_drafts_for_selected_rows() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(2));
        app_state.ui.torrent_management.pending_commands = vec![
            TorrentManagementPendingCommand {
                info_hash: hash(1),
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(1)),
                },
                state: TorrentControlState::Paused,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                info_hash: hash(2),
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(2)),
                },
                state: TorrentControlState::Paused,
                delete_files: false,
            },
        ];
        app_state.ui.torrent_management.confirm_submit = true;

        assert_eq!(
            map_key_to_management_action(KeyCode::Char('u'), &app_state),
            Some(TorrentManagementAction::ClearPendingForTargets)
        );

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ClearPendingForTargets,
        );

        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 1);
        assert_eq!(
            app_state.ui.torrent_management.pending_commands[0].info_hash,
            hash(1)
        );
        assert!(app_state
            .ui
            .torrent_management
            .selected_hashes
            .contains(&hash(2)));
        assert!(app_state.ui.torrent_management.confirm_submit);
    }

    #[test]
    fn space_toggles_selection_without_clearing_pending_draft() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Meadow Saga S01E01", 100, 10, 2),
            (hash(2), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state.ui.torrent_management.pending_commands = vec![TorrentManagementPendingCommand {
            info_hash: hash(1),
            request: ControlRequest::Pause {
                info_hash_hex: hex::encode(hash(1)),
            },
            state: TorrentControlState::Paused,
            delete_files: false,
        }];

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::ToggleCurrentSelection,
        );

        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 1);
        assert!(app_state
            .ui
            .torrent_management
            .selected_hashes
            .contains(&hash(1)));
    }

    #[test]
    fn repeated_same_management_action_clears_pending_draft_for_target() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Meadow Saga S01E01", 100, 10, 2)]);
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(1));

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );
        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 1);

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );

        assert!(app_state.ui.torrent_management.pending_commands.is_empty());
    }

    #[test]
    fn different_management_action_replaces_pending_draft_for_target() {
        let mut app_state =
            app_state_with_torrents(vec![(hash(1), "Meadow Saga S01E01", 100, 10, 2)]);
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(1));

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::TogglePauseTargets,
        );
        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::StartDelete {
                delete_files: false,
            },
        );

        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 1);
        assert!(matches!(
            app_state.ui.torrent_management.pending_commands[0].request,
            ControlRequest::Delete {
                delete_files: false,
                ..
            }
        ));
    }

    #[test]
    fn pending_management_review_groups_split_commands_by_action() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Cinder Trails S01E01", 100, 10, 2),
            (hash(2), "Cinder Trails S01E02", 300, 20, 3),
            (hash(3), "Meadow Saga S01E01", 400, 30, 4),
        ]);
        app_state
            .torrents
            .get_mut(&hash(3))
            .expect("purge torrent")
            .latest_state
            .total_size = 2_500_000_000;
        app_state.ui.torrent_management.pending_commands = vec![
            TorrentManagementPendingCommand {
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(1)),
                },
                info_hash: hash(1),
                state: TorrentControlState::Paused,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                request: ControlRequest::Delete {
                    info_hash_hex: hex::encode(hash(2)),
                    delete_files: false,
                },
                info_hash: hash(2),
                state: TorrentControlState::Deleting,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                request: ControlRequest::Delete {
                    info_hash_hex: hex::encode(hash(3)),
                    delete_files: true,
                },
                info_hash: hash(3),
                state: TorrentControlState::Deleting,
                delete_files: true,
            },
        ];

        let groups = pending_management_review_groups(&app_state);

        assert_eq!(groups.pause, vec!["Cinder Trails S01E01"]);
        assert_eq!(groups.delete, vec!["Cinder Trails S01E02"]);
        assert_eq!(groups.purge, vec!["Meadow Saga S01E01"]);
        assert_eq!(groups.purge_total_bytes, 2_500_000_000);
        assert_eq!(format_gb(groups.purge_total_bytes), "2.50 GB");
        assert!(groups.resume.is_empty());
    }

    #[test]
    fn delete_action_stages_delete_request_without_confirmation() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Cinder Trails S01E01", 100, 10, 2),
            (hash(2), "Cinder Trails S01E02", 300, 20, 3),
        ]);
        app_state
            .ui
            .torrent_management
            .selected_hashes
            .insert(hash(2));

        reduce_torrent_management_action(
            &mut app_state,
            TorrentManagementAction::StartDelete { delete_files: true },
        );

        assert!(!app_state.ui.torrent_management.confirm_submit);
        assert_eq!(app_state.ui.torrent_management.pending_commands.len(), 1);
        assert!(matches!(
            app_state.ui.torrent_management.pending_commands[0].request,
            ControlRequest::Delete {
                delete_files: true,
                ..
            }
        ));
        assert!(app_state.ui.torrent_management.selected_hashes.is_empty());
    }

    #[test]
    fn pending_management_summary_counts_draft_actions() {
        let mut app_state = app_state_with_torrents(vec![
            (hash(1), "Cinder Trails S01E01", 100, 10, 2),
            (hash(2), "Cinder Trails S01E02", 300, 20, 3),
            (hash(3), "Meadow Saga S01E01", 100, 10, 2),
            (hash(4), "Meadow Saga S01E02", 300, 20, 3),
        ]);
        app_state.ui.torrent_management.pending_commands = vec![
            TorrentManagementPendingCommand {
                info_hash: hash(1),
                request: ControlRequest::Pause {
                    info_hash_hex: hex::encode(hash(1)),
                },
                state: TorrentControlState::Paused,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                info_hash: hash(2),
                request: ControlRequest::Resume {
                    info_hash_hex: hex::encode(hash(2)),
                },
                state: TorrentControlState::Running,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                info_hash: hash(3),
                request: ControlRequest::Delete {
                    info_hash_hex: hex::encode(hash(3)),
                    delete_files: false,
                },
                state: TorrentControlState::Deleting,
                delete_files: false,
            },
            TorrentManagementPendingCommand {
                info_hash: hash(4),
                request: ControlRequest::Delete {
                    info_hash_hex: hex::encode(hash(4)),
                    delete_files: true,
                },
                state: TorrentControlState::Deleting,
                delete_files: true,
            },
        ];

        let summary = pending_management_summary(&app_state);

        assert_eq!(summary.pause_count, 1);
        assert_eq!(summary.resume_count, 1);
        assert_eq!(summary.remove_count, 1);
        assert_eq!(summary.purge_count, 1);
    }
}
