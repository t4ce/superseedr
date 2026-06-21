// SPDX-FileCopyrightText: 2026 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::app::{AppCommand, AppMode, AppState, JournalFilter};
use crate::persistence::event_journal::{
    EventCategory, EventDetails, EventJournalEntry, EventType,
};
use crate::tui::action_style::{footer_key_style, ActionTone};
use crate::tui::app_command::spawn_app_command_sender;
use crate::tui::formatters::sanitize_text;
use crate::tui::screen_context::ScreenContext;
use chrono::{DateTime, Local};
use ratatui::crossterm::event::{Event as CrosstermEvent, KeyCode, KeyEventKind};
use ratatui::prelude::{Alignment, Constraint, Frame, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap};
use std::path::{Component, Path};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JournalAction {
    ToNormal,
    FilterNext,
    FilterPrev,
    MoveUp,
    MoveDown,
    ReplaySelected,
}

fn map_key_to_journal_action(key_code: KeyCode, key_kind: KeyEventKind) -> Option<JournalAction> {
    if !matches!(key_kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }

    match key_code {
        KeyCode::Esc | KeyCode::Char('q') => Some(JournalAction::ToNormal),
        KeyCode::Tab => Some(JournalAction::FilterNext),
        KeyCode::BackTab => Some(JournalAction::FilterPrev),
        KeyCode::Up | KeyCode::Char('k') => Some(JournalAction::MoveUp),
        KeyCode::Down | KeyCode::Char('j') => Some(JournalAction::MoveDown),
        KeyCode::Char('Y') => Some(JournalAction::ReplaySelected),
        _ => None,
    }
}

pub fn handle_event_with_shutdown(
    event: CrosstermEvent,
    app_state: &mut AppState,
    app_command_tx: &mpsc::Sender<AppCommand>,
    shutdown_tx: &broadcast::Sender<()>,
) {
    handle_event_inner(event, app_state, app_command_tx, Some(shutdown_tx));
}

fn handle_event_inner(
    event: CrosstermEvent,
    app_state: &mut AppState,
    app_command_tx: &mpsc::Sender<AppCommand>,
    shutdown_tx: Option<&broadcast::Sender<()>>,
) {
    if !matches!(app_state.mode, AppMode::Journal) {
        return;
    }

    let CrosstermEvent::Key(key) = event else {
        return;
    };

    let Some(action) = map_key_to_journal_action(key.code, key.kind) else {
        return;
    };

    app_state.ui.journal.status_message = None;

    match action {
        JournalAction::ToNormal => app_state.mode = AppMode::Normal,
        JournalAction::FilterNext => {
            app_state.ui.journal.filter = app_state.ui.journal.filter.next();
            app_state.ui.journal.selected_index = 0;
        }
        JournalAction::FilterPrev => {
            app_state.ui.journal.filter = app_state.ui.journal.filter.prev();
            app_state.ui.journal.selected_index = 0;
        }
        JournalAction::MoveUp => {
            app_state.ui.journal.selected_index =
                app_state.ui.journal.selected_index.saturating_sub(1);
        }
        JournalAction::MoveDown => {
            let len = filtered_entries(app_state).len();
            if len > 0 {
                app_state.ui.journal.selected_index =
                    (app_state.ui.journal.selected_index + 1).min(len - 1);
            }
        }
        JournalAction::ReplaySelected => {
            replay_selected_entry(app_state, app_command_tx, shutdown_tx)
        }
    }
}

fn entry_matches_filter(entry: &EventJournalEntry, filter: JournalFilter) -> bool {
    match filter {
        JournalFilter::All => true,
        JournalFilter::Queue => matches!(entry.category, EventCategory::Ingest),
        JournalFilter::Commands => matches!(entry.category, EventCategory::Control),
        JournalFilter::Health => matches!(entry.category, EventCategory::DataHealth),
    }
}

fn filtered_entries(app_state: &AppState) -> Vec<&EventJournalEntry> {
    app_state
        .event_journal_state
        .entries
        .iter()
        .rev()
        .filter(|entry| entry_matches_filter(entry, app_state.ui.journal.filter))
        .collect()
}

fn event_type_label(entry: &EventJournalEntry) -> &'static str {
    match entry.event_type {
        EventType::IngestQueued => "Queued",
        EventType::IngestAdded => "Added",
        EventType::IngestDuplicate => "Duplicate",
        EventType::IngestInvalid => "Invalid",
        EventType::IngestFailed => "Failed",
        EventType::TorrentCompleted => "Complete",
        EventType::DataUnavailable => "Missing",
        EventType::DataRecovered => "Found",
        EventType::ControlQueued => "Queued",
        EventType::ControlApplied => "Applied",
        EventType::ControlFailed => "Error",
    }
}

fn command_action_label(entry: &EventJournalEntry) -> String {
    match &entry.details {
        EventDetails::Control { action, .. } => sanitize_text(action),
        _ => event_type_label(entry).to_string(),
    }
}

fn source_label(entry: &EventJournalEntry, anonymize: bool) -> String {
    if anonymize {
        return "/path/to/source".to_string();
    }

    entry
        .source_watch_folder
        .as_ref()
        .map(|path| compact_path_label(path, 2))
        .or_else(|| {
            entry
                .source_path
                .as_ref()
                .map(|path| compact_path_label(path, 2))
        })
        .unwrap_or_else(|| "-".to_string())
}

fn torrent_label(entry: &EventJournalEntry, anonymize: bool) -> String {
    if anonymize {
        return "Torrent".to_string();
    }

    entry
        .torrent_name
        .as_ref()
        .map(|name| sanitize_text(name))
        .unwrap_or_else(|| "-".to_string())
}

fn live_completion_percent(entry: &EventJournalEntry, app_state: &AppState) -> Option<f64> {
    if let Some(info_hash_hex) = entry.info_hash_hex.as_deref() {
        if let Some(display) = app_state
            .torrents
            .iter()
            .find(|(info_hash, _)| hex::encode(info_hash.as_slice()) == info_hash_hex)
            .map(|(_, display)| display)
        {
            return Some(crate::app::torrent_completion_percent(
                &display.latest_state,
            ));
        }
    }

    entry.torrent_name.as_ref().and_then(|torrent_name| {
        app_state
            .torrents
            .values()
            .filter(|display| display.latest_state.torrent_name == *torrent_name)
            .map(|display| crate::app::torrent_completion_percent(&display.latest_state))
            .max_by(|left, right| left.total_cmp(right))
    })
}

fn progress_label(entry: &EventJournalEntry, app_state: &AppState) -> String {
    live_completion_percent(entry, app_state)
        .map(|pct| format!("{pct:.0}%"))
        .unwrap_or_else(|| "-".to_string())
}

fn preferred_source_text(entry: &EventJournalEntry) -> Option<String> {
    entry
        .source_path
        .as_ref()
        .map(|path| path.display().to_string())
        .or_else(|| {
            entry
                .source_watch_folder
                .as_ref()
                .map(|path| path.display().to_string())
        })
}

fn pretty_timestamp(ts_iso: &str) -> String {
    DateTime::parse_from_rfc3339(ts_iso)
        .map(|dt| {
            dt.with_timezone(&Local)
                .format("%b %d %I:%M %p")
                .to_string()
        })
        .unwrap_or_else(|_| ts_iso.to_string())
}

fn compact_path_label(path: &Path, depth: usize) -> String {
    let components = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(segment) => Some(segment.to_string_lossy().into_owned()),
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();

    if components.is_empty() {
        return sanitize_text(&path.display().to_string());
    }

    if components.len() <= depth {
        return sanitize_text(&components.join("/"));
    }

    sanitize_text(&format!(
        ".../{}",
        components[components.len() - depth..].join("/")
    ))
}

fn detail_text(entry: Option<&EventJournalEntry>, anonymize: bool) -> String {
    let Some(entry) = entry else {
        return "No journal entries yet.".to_string();
    };

    let mut text = entry
        .message
        .clone()
        .unwrap_or_else(|| "No journal entries yet.".to_string());

    if anonymize {
        if let Some(torrent_name) = &entry.torrent_name {
            text = text.replace(torrent_name, "Torrent");
        }
        if let Some(source_path) = &entry.source_path {
            text = text.replace(&source_path.display().to_string(), "/path/to/source");
        }
        if let Some(source_watch_folder) = &entry.source_watch_folder {
            text = text.replace(
                &source_watch_folder.display().to_string(),
                "/path/to/source",
            );
        }
    }

    sanitize_text(&text)
}

fn selected_detail_text(app_state: &AppState, entry: Option<&EventJournalEntry>) -> String {
    let Some(entry) = entry else {
        return "No journal entries yet.".to_string();
    };

    let source_text = preferred_source_text(entry);

    if let Some(source_text) = source_text {
        if app_state.anonymize_torrent_names {
            return "/path/to/source".to_string();
        }
        return sanitize_text(&source_text);
    }

    detail_text(Some(entry), app_state.anonymize_torrent_names)
}

fn replay_command_for_path(path: &Path) -> Option<AppCommand> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("torrent") => {
            Some(AppCommand::AddTorrentFromFile(path.to_path_buf()))
        }
        Some(ext) if ext.eq_ignore_ascii_case("magnet") => {
            Some(AppCommand::AddMagnetFromFile(path.to_path_buf()))
        }
        Some(ext) if ext.eq_ignore_ascii_case("path") => {
            Some(AppCommand::AddTorrentFromPathFile(path.to_path_buf()))
        }
        _ => None,
    }
}

fn replay_selected_entry(
    app_state: &mut AppState,
    app_command_tx: &mpsc::Sender<AppCommand>,
    shutdown_tx: Option<&broadcast::Sender<()>>,
) {
    let entries = filtered_entries(app_state);
    let Some(entry) = entries.get(app_state.ui.journal.selected_index).copied() else {
        app_state.ui.journal.status_message = Some("No journal entry selected".to_string());
        return;
    };

    let Some(source_path) = entry.source_path.as_ref() else {
        app_state.ui.journal.status_message =
            Some("Selected entry has no replayable source file".to_string());
        return;
    };

    let Some(command) = replay_command_for_path(source_path) else {
        app_state.ui.journal.status_message =
            Some("Selected entry does not point to a replayable source file".to_string());
        return;
    };

    if !source_path.exists() {
        app_state.ui.journal.status_message =
            Some("Replay source file is no longer available".to_string());
        return;
    }

    if let Some(shutdown_tx) = shutdown_tx {
        spawn_app_command_sender(app_command_tx.clone(), shutdown_tx.subscribe(), command);
        app_state.ui.journal.status_message =
            Some(format!("Replayed {}", compact_path_label(source_path, 2)));
    } else {
        match app_command_tx.try_send(command) {
            Ok(()) => {
                app_state.ui.journal.status_message =
                    Some(format!("Replayed {}", compact_path_label(source_path, 2)));
            }
            Err(_) => {
                app_state.ui.journal.status_message =
                    Some("Replay request queue is busy".to_string());
            }
        }
    }
}

#[derive(Clone, Copy)]
enum JournalColumn {
    Time,
    Event,
    Done,
    Torrent,
    Source,
}

fn columns_for_filter(filter: JournalFilter) -> Vec<JournalColumn> {
    match filter {
        JournalFilter::All => vec![
            JournalColumn::Time,
            JournalColumn::Event,
            JournalColumn::Done,
            JournalColumn::Torrent,
            JournalColumn::Source,
        ],
        JournalFilter::Queue => vec![
            JournalColumn::Time,
            JournalColumn::Event,
            JournalColumn::Done,
            JournalColumn::Torrent,
            JournalColumn::Source,
        ],
        JournalFilter::Commands => {
            vec![
                JournalColumn::Time,
                JournalColumn::Event,
                JournalColumn::Source,
            ]
        }
        JournalFilter::Health => vec![
            JournalColumn::Time,
            JournalColumn::Event,
            JournalColumn::Torrent,
        ],
    }
}

fn column_header(column: JournalColumn) -> &'static str {
    match column {
        JournalColumn::Time => "Time",
        JournalColumn::Event => "Event",
        JournalColumn::Done => "Done",
        JournalColumn::Torrent => "Torrent",
        JournalColumn::Source => "Source",
    }
}

fn column_constraint(column: JournalColumn, filter: JournalFilter) -> Constraint {
    match (filter, column) {
        (_, JournalColumn::Time) => Constraint::Length(17),
        (JournalFilter::Commands, JournalColumn::Event) => Constraint::Percentage(34),
        (JournalFilter::Health, JournalColumn::Event) => Constraint::Length(10),
        (_, JournalColumn::Event) => Constraint::Length(10),
        (_, JournalColumn::Done) => Constraint::Length(8),
        (JournalFilter::Health, JournalColumn::Torrent) => Constraint::Min(10),
        (_, JournalColumn::Torrent) => Constraint::Percentage(41),
        (JournalFilter::Commands, JournalColumn::Source) => Constraint::Percentage(46),
        (_, JournalColumn::Source) => Constraint::Percentage(24),
    }
}

fn column_cell(
    column: JournalColumn,
    entry: &EventJournalEntry,
    app_state: &AppState,
) -> Cell<'static> {
    match column {
        JournalColumn::Time => Cell::from(pretty_timestamp(&entry.ts_iso)),
        JournalColumn::Event => {
            let label = if matches!(app_state.ui.journal.filter, JournalFilter::Commands) {
                command_action_label(entry)
            } else {
                event_type_label(entry).to_string()
            };
            Cell::from(label)
        }
        JournalColumn::Done => Cell::from(progress_label(entry, app_state)),
        JournalColumn::Torrent => {
            Cell::from(torrent_label(entry, app_state.anonymize_torrent_names))
        }
        JournalColumn::Source => Cell::from(source_label(entry, app_state.anonymize_torrent_names)),
    }
}

pub fn draw(f: &mut Frame, screen: &ScreenContext<'_>) {
    let app_state = screen.app.state;
    let ctx = screen.theme;
    let area = f.area();
    let popup = crate::tui::formatters::centered_rect(92, 84, area);
    let popup_layout =
        ratatui::layout::Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(popup);
    let body_area = popup_layout[0];
    let footer_area = popup_layout[1];
    f.render_widget(Clear, popup);

    let outer = Block::default()
        .title(Span::styled(
            " Event Journal ",
            ctx.apply(Style::default().fg(ctx.accent_sapphire()).bold()),
        ))
        .borders(Borders::ALL)
        .border_style(ctx.apply(Style::default().fg(ctx.theme.semantic.border)));
    let inner = outer.inner(body_area);
    f.render_widget(outer, body_area);

    let rows = ratatui::layout::Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(8),
        Constraint::Length(4),
    ])
    .split(inner);

    let filter_spans = [
        JournalFilter::All,
        JournalFilter::Queue,
        JournalFilter::Commands,
        JournalFilter::Health,
    ]
    .iter()
    .enumerate()
    .flat_map(|(idx, filter)| {
        let style = if *filter == app_state.ui.journal.filter {
            ctx.apply(
                Style::default()
                    .fg(ctx.state_selected())
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1))
        };
        let mut spans = vec![Span::styled(filter.label().to_string(), style)];
        if idx < 3 {
            spans.push(Span::styled(
                "  ",
                ctx.apply(Style::default().fg(ctx.theme.semantic.surface2)),
            ));
        }
        spans
    })
    .collect::<Vec<_>>();
    f.render_widget(Paragraph::new(Line::from(filter_spans)), rows[0]);

    let entries = filtered_entries(app_state);
    let status_line = app_state
        .ui
        .journal
        .status_message
        .as_ref()
        .map(|message| format!("{} entries  |  {}", entries.len(), sanitize_text(message)))
        .unwrap_or_else(|| format!("{} entries", entries.len()));
    f.render_widget(
        Paragraph::new(status_line)
            .style(ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1))),
        rows[1],
    );

    let body_rows = entries
        .iter()
        .map(|entry| {
            Row::new(
                columns_for_filter(app_state.ui.journal.filter)
                    .into_iter()
                    .map(|column| column_cell(column, entry, app_state))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    let columns = columns_for_filter(app_state.ui.journal.filter);
    let constraints = columns
        .iter()
        .map(|column| column_constraint(*column, app_state.ui.journal.filter))
        .collect::<Vec<_>>();
    let header_cells = columns
        .iter()
        .map(|column| column_header(*column))
        .collect::<Vec<_>>();

    let table = Table::new(body_rows, constraints)
        .header(
            Row::new(header_cells).style(
                ctx.apply(
                    Style::default()
                        .fg(ctx.theme.semantic.subtext0)
                        .add_modifier(Modifier::BOLD),
                ),
            ),
        )
        .row_highlight_style(
            ctx.apply(
                Style::default()
                    .fg(ctx.theme.semantic.text)
                    .bg(ctx.theme.semantic.surface0),
            ),
        )
        .block(
            Block::default()
                .borders(Borders::TOP | Borders::BOTTOM)
                .border_style(ctx.apply(Style::default().fg(ctx.theme.semantic.surface2))),
        );

    let mut table_state = TableState::default();
    if !entries.is_empty() {
        table_state.select(Some(
            app_state.ui.journal.selected_index.min(entries.len() - 1),
        ));
    }
    f.render_stateful_widget(table, rows[2], &mut table_state);

    let details_text = selected_detail_text(
        app_state,
        entries.get(app_state.ui.journal.selected_index).copied(),
    );
    f.render_widget(
        Paragraph::new(details_text)
            .wrap(Wrap { trim: true })
            .alignment(Alignment::Left)
            .style(ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1))),
        rows[3],
    );

    let footer_hint = Paragraph::new(Line::from(vec![
        Span::styled("[Tab]", footer_key_style(ctx, ActionTone::Mode)),
        Span::styled(
            " Filter  ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
        Span::styled("[Shift+Tab]", footer_key_style(ctx, ActionTone::Mode)),
        Span::styled(
            " Back  ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
        Span::styled("[j/k]", footer_key_style(ctx, ActionTone::Navigate)),
        Span::styled(
            " Move  ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
        Span::styled("[Shift+Y]", footer_key_style(ctx, ActionTone::Replay)),
        Span::styled(
            " Replay  ",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
        Span::styled("[q]", footer_key_style(ctx, ActionTone::Cancel)),
        Span::styled(
            " Close",
            ctx.apply(Style::default().fg(ctx.theme.semantic.subtext1)),
        ),
    ]))
    .alignment(Alignment::Center);
    f.render_widget(footer_hint, footer_area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{TorrentDisplayState, TorrentMetrics};
    use crate::persistence::event_journal::{EventCategory, EventJournalState};
    use ratatui::crossterm::event::{KeyEvent, KeyModifiers};
    use std::fs;
    use std::path::Path;
    use tokio::sync::mpsc;

    fn base_state() -> AppState {
        let mut state = AppState {
            mode: AppMode::Journal,
            ..Default::default()
        };
        state.event_journal_state = EventJournalState {
            next_id: 4,
            entries: vec![
                EventJournalEntry {
                    id: 1,
                    category: EventCategory::Ingest,
                    event_type: EventType::IngestAdded,
                    torrent_name: Some("Sample Alpha".to_string()),
                    ..Default::default()
                },
                EventJournalEntry {
                    id: 2,
                    category: EventCategory::Control,
                    event_type: EventType::ControlApplied,
                    torrent_name: Some("Sample Beta".to_string()),
                    ..Default::default()
                },
                EventJournalEntry {
                    id: 3,
                    category: EventCategory::DataHealth,
                    event_type: EventType::DataUnavailable,
                    torrent_name: Some("Sample Gamma".to_string()),
                    ..Default::default()
                },
            ],
        };
        state
    }

    fn handle_event(
        event: CrosstermEvent,
        app_state: &mut AppState,
        app_command_tx: &mpsc::Sender<AppCommand>,
    ) {
        handle_event_inner(event, app_state, app_command_tx, None);
    }

    #[test]
    fn tab_cycles_filters() {
        let mut app_state = base_state();
        let (tx, _rx) = mpsc::channel(1);

        handle_event(
            CrosstermEvent::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE)),
            &mut app_state,
            &tx,
        );
        assert_eq!(app_state.ui.journal.filter, JournalFilter::Queue);

        handle_event(
            CrosstermEvent::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE)),
            &mut app_state,
            &tx,
        );
        assert_eq!(app_state.ui.journal.filter, JournalFilter::Commands);
    }

    #[test]
    fn filter_selection_matches_requested_groups() {
        let mut app_state = base_state();

        app_state.ui.journal.filter = JournalFilter::Queue;
        let added = filtered_entries(&app_state);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].event_type, EventType::IngestAdded);

        app_state.ui.journal.filter = JournalFilter::Commands;
        let commands = filtered_entries(&app_state);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].event_type, EventType::ControlApplied);

        app_state.ui.journal.filter = JournalFilter::Health;
        let health = filtered_entries(&app_state);
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].event_type, EventType::DataUnavailable);
    }

    #[test]
    fn compact_path_label_keeps_tail_components() {
        let label = compact_path_label(Path::new("/alpha/beta/watch_files"), 2);
        assert_eq!(label, ".../beta/watch_files");
    }

    #[test]
    fn pretty_timestamp_formats_rfc3339_values() {
        let label = pretty_timestamp("2026-03-15T14:26:28Z");
        assert!(label.contains("Mar"));
    }

    #[test]
    fn progress_label_uses_live_torrent_metrics_when_info_hash_matches() {
        let mut app_state = base_state();
        let info_hash = vec![0x11; 20];
        app_state.event_journal_state.entries[0].info_hash_hex = Some(hex::encode(&info_hash));
        app_state.torrents.insert(
            info_hash,
            TorrentDisplayState {
                latest_state: TorrentMetrics {
                    number_of_pieces_total: 10,
                    number_of_pieces_completed: 4,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        assert_eq!(
            progress_label(&app_state.event_journal_state.entries[0], &app_state),
            "40%"
        );
    }

    #[test]
    fn anonymized_journal_hides_torrent_names_and_paths() {
        let entry = EventJournalEntry {
            torrent_name: Some("Sample Alpha".to_string()),
            source_path: Some(Path::new("/alpha/beta/watch_files/sample.torrent").to_path_buf()),
            message: Some(
                "Added Sample Alpha from /alpha/beta/watch_files/sample.torrent".to_string(),
            ),
            ..Default::default()
        };

        assert_eq!(torrent_label(&entry, true), "Torrent");
        assert_eq!(source_label(&entry, true), "/path/to/source");

        let details = detail_text(Some(&entry), true);
        assert!(!details.contains("Sample Alpha"));
        assert!(!details.contains("/alpha/beta/watch_files/sample.torrent"));
        assert!(details.contains("Torrent"));
        assert!(details.contains("/path/to/source"));
    }

    #[test]
    fn selected_detail_text_prefers_recorded_source_path_over_live_magnet() {
        let mut app_state = base_state();
        let info_hash = vec![0x22; 20];
        app_state.event_journal_state.entries[0].info_hash_hex = Some(hex::encode(&info_hash));
        app_state.event_journal_state.entries[0].source_path =
            Some(Path::new("/alpha/archive/sample.magnet").to_path_buf());
        app_state.torrents.insert(
            info_hash,
            TorrentDisplayState {
                latest_state: TorrentMetrics {
                    torrent_name: "Sample Alpha".to_string(),
                    torrent_or_magnet:
                        "magnet:?xt=urn:btih:2222222222222222222222222222222222222222".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let details =
            selected_detail_text(&app_state, Some(&app_state.event_journal_state.entries[0]));
        assert_eq!(details, "/alpha/archive/sample.magnet");
    }

    #[test]
    fn command_filter_uses_action_label_and_reduced_columns() {
        let entry = EventJournalEntry {
            details: EventDetails::Control {
                origin: crate::persistence::event_journal::ControlOrigin::CliOnline,
                action: "pause".to_string(),
                target_info_hash_hex: None,
                file_index: None,
                file_path: None,
                priority: None,
            },
            ..Default::default()
        };

        assert_eq!(command_action_label(&entry), "pause");
        assert_eq!(columns_for_filter(JournalFilter::Commands).len(), 3);
        assert_eq!(
            column_header(columns_for_filter(JournalFilter::Commands)[1]),
            "Event"
        );
        assert_eq!(
            column_header(columns_for_filter(JournalFilter::Commands)[2]),
            "Source"
        );
        assert_eq!(command_action_label(&entry), "pause");
    }

    #[test]
    fn health_filter_hides_source_column() {
        let columns = columns_for_filter(JournalFilter::Health);
        assert_eq!(columns.len(), 3);
        assert!(columns
            .iter()
            .all(|column| !matches!(column, JournalColumn::Source)));
    }

    #[test]
    fn shift_y_replays_selected_magnet_source() {
        let mut app_state = base_state();
        app_state.ui.journal.filter = JournalFilter::Queue;
        let replay_path = std::env::temp_dir().join(format!(
            "superseedr-journal-replay-{}.magnet",
            std::process::id()
        ));
        fs::write(
            &replay_path,
            "magnet:?xt=urn:btih:4444444444444444444444444444444444444444",
        )
        .expect("write replay file");
        app_state.event_journal_state.entries[0].source_path = Some(replay_path.clone());
        let (tx, mut rx) = mpsc::channel(1);

        handle_event(
            CrosstermEvent::Key(KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::SHIFT)),
            &mut app_state,
            &tx,
        );

        match rx.try_recv() {
            Ok(AppCommand::AddMagnetFromFile(path)) => assert_eq!(path, replay_path),
            Ok(_) => panic!("expected replayed magnet command"),
            Err(error) => panic!("expected replay command, got {error:?}"),
        }

        fs::remove_file(&replay_path).ok();
    }

    #[test]
    fn shift_y_reports_missing_replay_source() {
        let mut app_state = base_state();
        let (tx, _rx) = mpsc::channel(1);

        handle_event(
            CrosstermEvent::Key(KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::SHIFT)),
            &mut app_state,
            &tx,
        );

        assert_eq!(
            app_state.ui.journal.status_message.as_deref(),
            Some("Selected entry has no replayable source file")
        );
    }
}
