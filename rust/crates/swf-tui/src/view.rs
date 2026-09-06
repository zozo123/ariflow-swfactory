//! Drawing the model, and nothing else.
//!
//! No function here awaits, mutates the model, reads a clock or dials a service: given a [`Model`]
//! it produces the same frame every time, which is what makes a `TestBackend` snapshot a real
//! assertion rather than a screenshot of a race.
//!
//! The boundary it defends is legibility under the conditions this tool is actually used in.
//! A terminal that is 80 columns wide because it is one pane of a tmux split still has to show the
//! job table, so the detail pane folds away at 100 columns and the navigation at 72. A terminal
//! with no colour — a CI log, a screenshot in a ticket, an operator who cannot tell red from green
//! — still has to distinguish a failed job from a finished one, so every state renders a glyph and
//! its word and colour is only ever the third signal. And every string that came from a service
//! goes through `sanitize` on its way to a widget, because a log line that repositions the cursor
//! is a security bug (rule 7).

use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row as TableRow, Table, Wrap};
use ratatui::Frame;
use swf_domain::sanitize::{sanitize_block, sanitize_line};
use unicode_width::UnicodeWidthStr;

use crate::event::HELP;
use crate::model::{source_status, Model, Row as ModelRow, RowKind, View};
use crate::theme::{state_label, Theme, Tone};

/// The separator `herd` used between header parts: two spaces, a middle dot, two spaces.
pub const SEP: &str = "  ·  ";

/// How tall the activity pane is when there is room for it.
pub const LOG_ROWS: u16 = 9;

/// How tall it shrinks to on a short terminal, before it is worth having at all.
pub const LOG_ROWS_SHORT: u16 = 6;

/// Width of the left navigation.
pub const NAV_COLS: u16 = 20;

/// Width of the right detail pane.
pub const DETAIL_COLS: u16 = 38;

/// Draw the whole screen.
pub fn render(frame: &mut Frame, model: &Model, theme: Theme) {
    let area = frame.area();
    frame.render_widget(Block::default().style(theme.base()), area);

    let log_rows = if !model.log_open || area.height < 18 {
        0
    } else if area.height < 30 {
        LOG_ROWS_SHORT
    } else {
        LOG_ROWS
    };
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(log_rows),
        Constraint::Length(1),
    ])
    .split(area);

    header(frame, model, theme, chunks[0]);
    body(frame, model, theme, chunks[1]);
    if log_rows > 0 {
        activity(frame, model, theme, chunks[2]);
    }
    footer(frame, model, theme, chunks[3]);

    if let Some(confirm) = &model.confirm {
        modal(
            frame,
            theme,
            area,
            "confirm",
            vec![
                Line::styled(sanitize_line(&confirm.question), theme.title()),
                Line::from(""),
                Line::from(vec![
                    Span::styled("y", theme.label()),
                    Span::raw(" confirm  ·  "),
                    Span::styled("n", theme.label()),
                    Span::raw(" cancel"),
                ]),
            ],
        );
    } else if let Some(prompt) = &model.prompt {
        modal(
            frame,
            theme,
            area,
            "input",
            vec![
                Line::styled(sanitize_line(&prompt.title), theme.title()),
                Line::from(""),
                Line::from(vec![
                    Span::styled("> ", theme.label()),
                    Span::raw(prompt.input.clone()),
                    Span::styled("_", theme.muted()),
                ]),
                Line::styled(format!("e.g. {}", prompt.placeholder), theme.muted()),
                Line::from(""),
                Line::from(vec![
                    Span::styled("enter", theme.label()),
                    Span::raw(" submit  ·  "),
                    Span::styled("esc", theme.label()),
                    Span::raw(" cancel"),
                ]),
            ],
        );
    } else if model.palette.open {
        palette(frame, model, theme, area);
    } else if model.help_open {
        help(frame, theme, area);
    }
}

/// Which environment, as whom, and how old what is on screen is.
fn header(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let block = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(theme.border())
        .style(theme.panel());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut first: Vec<Span> = Vec::new();
    for (label, value) in [
        ("swf", model.env.context.as_str()),
        ("airflow", model.env.airflow_url.as_str()),
        ("repo", model.env.repo.as_str()),
        ("owner", model.env.owner.as_str()),
    ] {
        if !first.is_empty() {
            first.push(Span::raw(SEP));
        }
        first.push(Span::styled(label, theme.label()));
        first.push(Span::raw(" "));
        first.push(Span::raw(sanitize_line(value)));
    }

    let refreshed = match model.last_refresh {
        Some(at) => format!(
            "{} ({} ago)",
            at.format("%H:%M:%S"),
            swf_domain::rollup::age(Some(at), model.now)
        ),
        None => "never".to_string(),
    };
    let mut second: Vec<Span> = vec![
        Span::styled("actor", theme.label()),
        Span::raw(" "),
        Span::raw(sanitize_line(&model.env.actor)),
        Span::raw(SEP),
        Span::styled("refreshed", theme.label()),
        Span::raw(" "),
        Span::raw(refreshed),
    ];
    if model.in_flight > 0 {
        second.push(Span::raw(SEP));
        second.push(Span::styled("↻ loading", theme.tone(Tone::Active)));
    }
    if model.stale() {
        second.push(Span::raw(SEP));
        second.push(Span::styled("◔ stale", theme.tone(Tone::Warn)));
    }
    if model.truncated() {
        second.push(Span::raw(SEP));
        second.push(Span::styled("⋯ truncated", theme.tone(Tone::Warn)));
    }
    // One badge per failing source, sorted, so the header is stable between refreshes and an
    // operator can tell at a glance which pane is lying to them (`02-herd-tui.md` §9.3). The
    // glyph is inside the badge because reverse video is the one signal that does not survive a
    // screenshot pasted into a ticket.
    for source in model.unhealthy() {
        second.push(Span::raw(" "));
        second.push(Span::styled(
            format!(" ✗ {} ", sanitize_line(&source)),
            theme.badge(),
        ));
    }

    frame.render_widget(
        Paragraph::new(vec![Line::from(first), Line::from(second)]),
        inner,
    );
}

/// Navigation, table, detail — as many of the three as the terminal has room for.
fn body(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let mut constraints: Vec<Constraint> = Vec::new();
    let nav = !model.very_narrow();
    let detail = !model.narrow();
    if nav {
        constraints.push(Constraint::Length(NAV_COLS));
    }
    constraints.push(Constraint::Min(24));
    if detail {
        constraints.push(Constraint::Length(DETAIL_COLS));
    }
    let panes = Layout::horizontal(constraints).split(area);

    let mut index = 0;
    if nav {
        navigation(frame, model, theme, panes[index]);
        index += 1;
    }
    centre(frame, model, theme, panes[index]);
    if detail {
        index += 1;
        details(frame, model, theme, panes[index]);
    }
}

fn navigation(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let block = Block::default()
        .borders(Borders::RIGHT)
        .border_style(theme.border());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let lines: Vec<Line> = View::ALL
        .iter()
        .map(|view| {
            let count = match view {
                View::Attention => Some(model.attention.count()),
                View::Jobs => Some(model.snapshot.job_count()),
                View::Deliveries => Some(model.snapshot.prs.len()),
                View::Infrastructure => Some(model.snapshot.sandboxes.len() + model.sources.len()),
                _ => None,
            };
            let tail = count.map(|n| n.to_string()).unwrap_or_default();
            let label = format!("{} {}", view.number(), view.title());
            let pad = (inner.width as usize)
                .saturating_sub(label.width() + tail.width() + 2)
                .max(1);
            let text = format!(" {label}{}{tail} ", " ".repeat(pad));
            if *view == model.view {
                Line::styled(text, theme.selected())
            } else {
                Line::styled(text, theme.muted())
            }
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}

fn centre(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let rows = model.rows();
    let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);

    let mut title: Vec<Span> = vec![Span::styled(
        format!(" {} ", model.view.title()),
        theme.title(),
    )];
    if model.view != View::Review {
        title.push(Span::styled(format!("{} rows", rows.len()), theme.muted()));
    }
    if !model.search.query.is_empty() {
        title.push(Span::raw("  "));
        title.push(Span::styled(
            format!("/{}", sanitize_line(&model.search.query)),
            theme.tone(Tone::Warn),
        ));
    }
    if model.search.open {
        title.push(Span::styled("_", theme.tone(Tone::Warn)));
    }
    frame.render_widget(Paragraph::new(Line::from(title)), chunks[0]);

    if model.view == View::Review {
        review(frame, model, theme, chunks[1]);
        return;
    }
    if rows.is_empty() {
        frame.render_widget(
            Paragraph::new(Text::styled(empty_hint(model), theme.muted()))
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
        return;
    }

    let selected = model.selected_index();
    let header = TableRow::new(
        model
            .columns()
            .iter()
            .map(|c| Cell::from(*c))
            .collect::<Vec<_>>(),
    )
    .style(theme.header());
    let body: Vec<TableRow> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| table_row(row, theme, Some(i) == selected))
        .collect();
    frame.render_widget(
        Table::new(body, widths(model.view))
            .header(header)
            .column_spacing(1),
        chunks[1],
    );
}

fn table_row<'a>(row: &'a ModelRow, theme: Theme, selected: bool) -> TableRow<'a> {
    let style = if selected {
        theme.selected()
    } else {
        theme.tone(row.tone)
    };
    TableRow::new(
        row.cells
            .iter()
            .map(|cell| Cell::from(sanitize_line(cell)))
            .collect::<Vec<_>>(),
    )
    .style(style)
}

/// The column widths of one view. Written out per view rather than computed, because a column
/// whose width changes with the data is a column nobody can scan down.
fn widths(view: View) -> Vec<Constraint> {
    use Constraint::{Length, Min};
    match view {
        View::Attention => vec![Length(7), Length(20), Min(12), Length(14), Length(5)],
        View::Jobs => vec![
            Length(8),
            Length(16),
            Length(3),
            Length(6),
            Min(8),
            Length(16),
        ],
        View::JobDetail => vec![Min(30), Length(3), Length(17)],
        View::Deliveries => vec![Length(5), Min(20), Length(27), Length(6)],
        View::Infrastructure => vec![Length(7), Length(20), Length(13), Min(13), Length(5)],
        View::History => vec![Length(22), Min(10)],
        View::Review => Vec::new(),
    }
}

/// What an empty table means here, which is never the same thing twice.
fn empty_hint(model: &Model) -> String {
    if !model.search.query.is_empty() {
        return format!("nothing matches /{}  ·  esc clears it", model.search.query);
    }
    match model.view {
        View::Attention => "nothing needs a human right now.".into(),
        View::Jobs => "no jobs in this pass  ·  t triggers a blueprint".into(),
        View::JobDetail => "select a job on the jobs view, then press enter.".into(),
        View::Review => String::new(),
        View::Deliveries => "no delivery pull requests carry the factory label yet.".into(),
        View::Infrastructure => {
            "no sandboxes and no sources — this context names neither an owner nor a repo.".into()
        }
        View::History => "no committed metrics under this context's metrics root.".into(),
    }
}

/// The approval screen: the exact job, the current gate, the evidence revision and the actor.
///
/// Everything an answer depends on is on this one screen, spelled out, because approving the wrong
/// job is the mistake this whole product exists to make hard. Pressing `a` from here goes through
/// `Ops::gate_answer`, which re-reads and re-validates — the same path the CLI takes, so the
/// conflict case cannot behave differently depending on which face of the product was used.
fn review(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let Some(review) = &model.review else {
        frame.render_widget(
            Paragraph::new(Text::styled(
                "no gate under review  ·  select one on attention and press enter",
                theme.muted(),
            ))
            .wrap(Wrap { trim: true }),
            area,
        );
        return;
    };

    let ready = if review.ready {
        Span::styled("◆ answerable", theme.tone(Tone::Good))
    } else {
        Span::styled("⋯ not parked yet", theme.tone(Tone::Warn))
    };
    let mut lines: Vec<Line> = vec![
        field(theme, "gate", &review.id.to_string()),
        field(theme, "job", &review.id.job.to_string()),
        field(theme, "stage", &review.stage),
        field(theme, "job state", &state_label(&review.job_state)),
        Line::from(vec![
            Span::styled(format!("{:<12} ", "task state"), theme.label()),
            Span::raw(state_label(&review.task_state)),
            Span::raw("  "),
            ready,
        ]),
        field(theme, "revision", &review.revision),
        field(theme, "actor", &model.env.actor),
        field(theme, "options", &review.options.join(", ")),
        field(theme, "url", &review.url),
        Line::from(""),
        Line::styled("evidence", theme.header()),
        Line::styled(sanitize_line(&review.gate.subject), theme.title()),
    ];
    for line in sanitize_block(&review.gate.body).lines() {
        lines.push(Line::raw(line.to_string()));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("a", theme.label()),
        Span::raw(" approve  ·  "),
        Span::styled("x", theme.label()),
        Span::raw(" reject  ·  both re-read the gate before writing"),
    ]));
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn field<'a>(theme: Theme, label: &'a str, value: &str) -> Line<'a> {
    Line::from(vec![
        // Padded to twelve *plus* a literal space: a label that is exactly twelve characters
        // long would otherwise run straight into its value.
        Span::styled(format!("{label:<12} "), theme.label()),
        Span::raw(sanitize_line(value)),
    ])
}

/// The evidence for whatever is selected, and the keys that act on it.
fn details(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(theme.border());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // The Review screen has no table, so there is no selected *row* — but there is very much a
    // selected job, and the tasks it has already finished are the context an approver reads
    // before answering.
    if model.view == View::Review {
        let mut lines = vec![Line::styled(
            format!(" {}", model.current_key().unwrap_or_default()),
            theme.title(),
        )];
        lines.extend(job_details(model, theme));
        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
        return;
    }

    let Some(row) = model.current_row() else {
        frame.render_widget(
            Paragraph::new(Text::styled(" nothing selected", theme.muted())),
            inner,
        );
        return;
    };

    let mut lines: Vec<Line> = vec![Line::styled(format!(" {}", row.key), theme.title())];
    match row.kind {
        RowKind::Gate => lines.extend(gate_details(model, theme)),
        RowKind::Job | RowKind::Failure | RowKind::Task => lines.extend(job_details(model, theme)),
        RowKind::Delivery => lines.extend(delivery_details(model, theme, &row)),
        RowKind::Sandbox => lines.extend(sandbox_details(model, theme, &row)),
        RowKind::Source => lines.extend(source_details(model, theme, &row)),
        RowKind::Metric => {
            lines.push(field(theme, "value", row.cells.get(1).map_or("", |v| v)));
        }
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn gate_details<'a>(model: &Model, theme: Theme) -> Vec<Line<'a>> {
    let Some(gate) = model.selected_gate() else {
        return vec![Line::styled(
            " this gate is no longer waiting for an answer",
            theme.muted(),
        )];
    };
    let mut lines = vec![
        field(theme, "job", &gate.job().to_string()),
        field(theme, "gate", gate.short_name()),
        field(theme, "revision", &gate.current_revision()),
        field(theme, "options", &gate.options.join(", ")),
        field(theme, "actor", &model.env.actor),
        Line::from(""),
        Line::styled("subject", theme.header()),
        Line::raw(sanitize_line(&gate.subject)),
        Line::from(""),
        Line::styled("evidence", theme.header()),
    ];
    for line in sanitize_block(&gate.body).lines().take(24) {
        lines.push(Line::raw(line.to_string()));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        "enter reviews it in full before answering",
        theme.muted(),
    ));
    lines
}

fn job_details<'a>(model: &Model, theme: Theme) -> Vec<Line<'a>> {
    let Some(job) = model.selected_job_row() else {
        return vec![Line::styled(
            " this job is not in the last pass",
            theme.muted(),
        )];
    };
    let mut lines = vec![
        field(theme, "issue", &job.issue),
        field(theme, "state", &state_label(&job.state)),
        field(
            theme,
            "stage",
            &swf_domain::rollup::stage_progress(&job.tasks),
        ),
        Line::from(""),
        Line::styled("tasks", theme.header()),
    ];
    for task in &job.tasks {
        let state = task.state_or_none();
        lines.push(Line::from(vec![
            Span::styled(
                format!("{:<20}", sanitize_line(&task.task_id)),
                theme.tone(Tone::Plain),
            ),
            Span::styled(state_label(state), theme.state(state)),
        ]));
    }
    let gates: Vec<&swf_domain::model::Gate> = model
        .snapshot
        .gates
        .iter()
        .filter(|g| g.job() == job.id())
        .collect();
    if !gates.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::styled("gates", theme.header()));
        for gate in gates {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:<20}", sanitize_line(gate.short_name())),
                    theme.tone(Tone::Waiting),
                ),
                Span::raw(if gate.ready {
                    "◆ ready"
                } else {
                    "⋯ arming"
                }),
            ]));
        }
    }
    lines
}

fn delivery_details<'a>(model: &Model, theme: Theme, row: &ModelRow) -> Vec<Line<'a>> {
    let Some(delivery) = model
        .deliveries()
        .into_iter()
        .find(|d| d.id == row.key || format!("pr#{}", d.number) == row.key)
    else {
        return vec![Line::styled(
            " no such delivery in this pass",
            theme.muted(),
        )];
    };
    // The three claims are only ever separated by `deliveries verify`, so the pane says what it
    // actually knows — that a PR exists and what its checks said — and names the command that can
    // answer the third question (non-negotiable 10).
    vec![
        field(theme, "branch", &delivery.branch),
        field(theme, "issue", &delivery.issue_id),
        field(theme, "run", &delivery.run_id),
        field(theme, "state", &delivery.state),
        field(theme, "labels", &delivery.labels.join(", ")),
        field(theme, "checks", &delivery.checks),
        field(
            theme,
            "blocked",
            if delivery.blocked { "yes" } else { "no" },
        ),
        field(theme, "url", &delivery.url),
        Line::from(""),
        Line::styled(
            "checks passing is not the same as independently verified — v runs the target's own \
             tests from a fresh checkout and says which of the three claims it could prove.",
            theme.muted(),
        ),
    ]
}

fn sandbox_details<'a>(model: &Model, theme: Theme, row: &ModelRow) -> Vec<Line<'a>> {
    let name = row.cells.get(1).cloned().unwrap_or_default();
    let Some(sandbox) = model.snapshot.sandboxes.iter().find(|s| s.name == name) else {
        return vec![Line::styled(" no such sandbox in this pass", theme.muted())];
    };
    vec![
        field(theme, "status", &sandbox.status),
        field(theme, "created by", &sandbox.created_by),
        field(
            theme,
            "age",
            &swf_domain::rollup::age(
                sandbox.created_at.map(|t| t.with_timezone(&chrono::Utc)),
                model.now,
            ),
        ),
        field(
            theme,
            "factory name",
            if sandbox.factory_named() { "yes" } else { "no" },
        ),
        Line::from(""),
        Line::styled(
            "x removes it. The provider is re-listed immediately before the delete and a sandbox \
             that is not this owner's is refused.",
            theme.muted(),
        ),
    ]
}

fn source_details<'a>(model: &Model, theme: Theme, row: &ModelRow) -> Vec<Line<'a>> {
    let name = row.cells.get(1).cloned().unwrap_or_default();
    let Some(health) = model.sources.get(&name) else {
        return vec![
            field(theme, "source", &name),
            field(theme, "detail", row.cells.get(2).map_or("", |v| v)),
        ];
    };
    let (status, tone) = source_status(health, model.now, model.stale_after());
    vec![
        field(theme, "source", &name),
        Line::from(vec![
            Span::styled(format!("{:<12} ", "status"), theme.label()),
            Span::styled(status, theme.tone(tone)),
        ]),
        field(
            theme,
            "fetched",
            &swf_domain::rollup::age(health.fetched_at, model.now),
        ),
        field(
            theme,
            "truncated",
            if health.truncated { "yes" } else { "no" },
        ),
        Line::from(""),
        Line::styled("error", theme.header()),
        Line::raw(sanitize_line(health.error.as_deref().unwrap_or("-"))),
        Line::from(""),
        Line::styled(
            "one source failing never blanks another's pane; this one keeps its last good stamp.",
            theme.muted(),
        ),
    ]
}

/// The activity log: what was asked for, what happened, and how much has scrolled away.
fn activity(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let title = if model.log.dropped() > 0 {
        format!(" activity ({} lines dropped) ", model.log.dropped())
    } else {
        " activity ".to_string()
    };
    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(theme.border())
        .title(Span::styled(title, theme.header()));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = inner.height as usize;
    let lines: Vec<Line> = model
        .log
        .lines()
        .rev()
        .take(rows)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|line| Line::styled(sanitize_line(line), theme.muted()))
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}

/// The keys that do something here, ending with the ones that always do.
fn footer(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let contextual: &[(&str, &str)] = match model.view {
        View::Attention => &[("enter", "review"), ("a", "approve"), ("x", "reject")],
        View::Jobs | View::JobDetail => &[
            ("enter", "detail"),
            ("t", "trigger"),
            ("s", "stop"),
            ("o", "open"),
            ("L", "logs"),
        ],
        View::Review => &[("a", "approve"), ("x", "reject"), ("esc", "back")],
        View::Deliveries => &[("o", "open"), ("v", "verify")],
        View::Infrastructure => &[("x", "remove")],
        View::History => &[],
    };
    let mut spans: Vec<Span> = Vec::new();
    for (key, label) in contextual.iter().chain(
        [
            ("r", "refresh"),
            ("/", "search"),
            (":", "cmd"),
            ("?", "help"),
            ("q", "quit"),
        ]
        .iter(),
    ) {
        if !spans.is_empty() {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(*key, theme.label()));
        spans.push(Span::raw(" "));
        spans.push(Span::styled(*label, theme.muted()));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn palette(frame: &mut Frame, model: &Model, theme: Theme, area: Rect) {
    let matches = model.palette.matches();
    let mut lines = vec![
        Line::from(vec![
            Span::styled(": ", theme.label()),
            Span::raw(sanitize_line(&model.palette.input)),
            Span::styled("_", theme.muted()),
        ]),
        Line::from(""),
    ];
    if matches.is_empty() {
        lines.push(Line::styled("no command matches", theme.muted()));
    }
    let chosen = model.palette.index.min(matches.len().saturating_sub(1));
    for (i, command) in matches.iter().enumerate().take(12) {
        let text = format!(" {:<16}{}", command.name, command.help);
        lines.push(if i == chosen {
            Line::styled(text, theme.selected())
        } else {
            Line::styled(text, theme.muted())
        });
    }
    modal(frame, theme, area, "command", lines);
}

fn help(frame: &mut Frame, theme: Theme, area: Rect) {
    let mut lines: Vec<Line> = HELP
        .iter()
        .map(|(key, what)| {
            Line::from(vec![
                Span::styled(format!(" {key:<14}"), theme.label()),
                Span::styled(*what, theme.muted()),
            ])
        })
        .collect();
    lines.push(Line::from(""));
    lines.push(Line::styled(
        " closing this window leaves every remote job running.",
        theme.muted(),
    ));
    modal(frame, theme, area, "keys", lines);
}

/// A centred box over the screen. `Clear` first, or the table shows through it.
fn modal(frame: &mut Frame, theme: Theme, area: Rect, title: &str, lines: Vec<Line>) {
    let width = lines
        .iter()
        .map(|line| line.width())
        .max()
        .unwrap_or(20)
        .clamp(20, area.width.saturating_sub(4).max(20) as usize) as u16
        + 4;
    let height = (lines.len() as u16 + 2).min(area.height.saturating_sub(2).max(3));
    let rect = centred(area, width.min(area.width), height);
    frame.render_widget(Clear, rect);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border())
        .title(Span::styled(format!(" {title} "), theme.title()))
        .style(theme.panel());
    let inner = block.inner(rect);
    frame.render_widget(block, rect);
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Left)
            .style(Style::default()),
        inner,
    );
}

fn centred(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Env, Model, DEFAULT_REFRESH};
    use chrono::{TimeZone, Utc};

    fn model() -> Model {
        Model::new(
            Env {
                context: "local".into(),
                airflow_url: "http://localhost:8080".into(),
                repo: "-".into(),
                owner: "-".into(),
                actor: "anonymous".into(),
                dag_ids: Vec::new(),
            },
            DEFAULT_REFRESH,
            Utc.timestamp_opt(1_700_000_000, 0)
                .single()
                .unwrap_or_else(Utc::now),
        )
    }

    #[test]
    fn every_view_has_as_many_widths_as_it_has_columns() {
        let mut model = model();
        for view in View::ALL {
            model.view = view;
            assert_eq!(
                widths(view).len(),
                model.columns().len(),
                "{view:?} would render a ragged table"
            );
        }
    }

    #[test]
    fn an_empty_table_always_says_what_empty_means_here() {
        let mut model = model();
        for view in View::ALL {
            model.view = view;
            if view == View::Review {
                continue;
            }
            assert!(!empty_hint(&model).is_empty(), "{view:?} says nothing");
        }
        model.search.query = "nope".into();
        assert!(empty_hint(&model).contains("esc clears it"));
    }

    #[test]
    fn a_modal_never_asks_for_more_room_than_the_terminal_has() {
        let area = Rect::new(0, 0, 40, 10);
        let rect = centred(area, 200, 200);
        assert!(rect.width <= area.width && rect.height <= area.height);
        assert_eq!(rect.x, 0);
    }
}
