//! Rendering snapshots: what the operator actually sees, in the five conditions that matter.
//!
//! These are checked in as readable text on purpose. The `.snap` files under `tests/snapshots/`
//! are the review artefact — a diff there is a diff in what an operator will read at 2 a.m., and
//! it should be as easy to judge in a pull request as a paragraph of prose. Every case here is one
//! of the five `00-architecture.md` §8 names: normal, narrow, empty, stale and failed-source.

use chrono::{DateTime, TimeZone, Utc};
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::Terminal;
use serde_json::json;
use swf_app::gates::GateReview;
use swf_domain::ids::GateId;
use swf_domain::metrics::MetricsSummary;
use swf_domain::model::{
    Gate, JobRow, PullRequest, Run, SandboxRef, Snapshot, SourceHealth, TaskState,
};
use swf_tui::model::{Env, Model, View, DEFAULT_REFRESH};
use swf_tui::theme::Theme;
use swf_tui::view;

/// A fixed clock. Every age, every stamp and every "n ago" in these snapshots is derived from it.
fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 14, 9, 30, 0)
        .single()
        .expect("a real instant")
}

fn env() -> Env {
    Env {
        context: "prod".into(),
        airflow_url: "https://airflow.example.com".into(),
        repo: "acme/widgets".into(),
        owner: "me@example.com".into(),
        actor: "admin".into(),
        dag_ids: vec!["factory".into(), "hotfix".into()],
    }
}

fn model() -> Model {
    Model::new(env(), DEFAULT_REFRESH, now())
}

fn task(id: &str, index: i32, state: &str) -> TaskState {
    TaskState::new(id, index, Some(state.to_string()))
}

fn job(
    dag: &str,
    run: &str,
    index: i32,
    issue: &str,
    state: &str,
    tasks: Vec<TaskState>,
) -> JobRow {
    let mut row = JobRow::new(dag, run, index);
    row.issue = issue.into();
    row.state = state.into();
    row.tasks = tasks;
    row
}

/// A factory mid-flight: two runs, four jobs, two gates, two deliveries, two sandboxes.
fn busy() -> Snapshot {
    let mut snap = Snapshot::new(now());

    let mut factory = Run::new("factory", "manual__2026-03-14T09", "running");
    factory.jobs = vec![
        job(
            "factory",
            "manual__2026-03-14T09",
            0,
            "142",
            "awaiting_input",
            vec![
                task("job.spec", 0, "success"),
                task("job.approve_plan", 0, "awaiting_input"),
            ],
        ),
        job(
            "factory",
            "manual__2026-03-14T09",
            1,
            "143",
            "running",
            vec![
                task("job.spec", 1, "success"),
                task("job.build_and_test", 1, "running"),
            ],
        ),
    ];

    let mut hotfix = Run::new("hotfix", "manual__2026-03-14T08", "failed");
    hotfix.jobs = vec![
        job(
            "hotfix",
            "manual__2026-03-14T08",
            0,
            "sre-9",
            "failed",
            vec![
                task("job.spec", 0, "success"),
                task("job.build_and_test", 0, "failed"),
            ],
        ),
        job(
            "hotfix",
            "manual__2026-03-14T08",
            1,
            "sre-10",
            "success",
            vec![task("job.deliver", 1, "success")],
        ),
    ];
    snap.runs = vec![factory, hotfix];

    let mut ready = Gate::new(
        "factory",
        "manual__2026-03-14T09",
        "job.approve_plan",
        0,
        "plan for issue 142: split the ingest worker",
        "three files change; the migration is reversible.",
        Some(now() - chrono::Duration::minutes(4)),
        vec!["approve".into(), "reject".into()],
    );
    ready.ready = true;
    let arming = Gate::new(
        "factory",
        "manual__2026-03-14T09",
        "job.approve_intent",
        1,
        "intent for issue 143: cache the target listing",
        "one file changes.",
        Some(now() - chrono::Duration::seconds(20)),
        vec!["approve".into(), "reject".into()],
    );
    snap.gates = vec![ready, arming];

    snap.prs = vec![
        PullRequest {
            number: 512,
            title: "factory: split the ingest worker".into(),
            url: "https://github.com/acme/widgets/pull/512".into(),
            labels: vec!["factory".into(), "agent-authored".into()],
            state: "OPEN".into(),
            checks: "3 pass / 0 fail / 1 pending".into(),
            head: "factory/142-manual2026".into(),
        },
        PullRequest {
            number: 511,
            title: "[BLOCKED] hotfix: retry the flaky upload".into(),
            url: "https://github.com/acme/widgets/pull/511".into(),
            labels: vec!["factory".into(), "factory:blocked".into()],
            state: "OPEN".into(),
            checks: "1 pass / 2 fail / 0 pending".into(),
            head: "factory/sre-9-manual2026".into(),
        },
    ];

    snap.sandboxes = vec![
        SandboxRef::new(
            "swf-ingest-1a2b3c4d",
            "running",
            "me@example.com",
            Some(now() - chrono::Duration::minutes(12)),
        ),
        SandboxRef::new(
            "swf-upload-9f8e7d6c",
            "stopped",
            "me@example.com",
            Some(now() - chrono::Duration::hours(9)),
        ),
    ];

    snap.metrics = serde_json::to_value(MetricsSummary {
        runs: 48,
        scripted_runs: 6,
        first_pass_rate: 0.75,
        mean_iterations: 1.42,
        p50_cycle_s: 612.5,
        tests_pass_rate: 0.9375,
        findings_by_severity: serde_json::from_value(json!({
            "blocker": 1, "major": 4, "minor": 11, "nit": 23
        }))
        .expect("a findings histogram"),
        blockers: 1,
        total_cost_usd: 18.4213,
    })
    .expect("a metrics summary serialises");

    for source in ["airflow", "gates", "github", "islo", "metrics"] {
        snap.health
            .insert(source.to_string(), SourceHealth::fresh(now()));
    }
    snap
}

/// The same pass, but nothing has answered in twelve minutes.
fn stale() -> Snapshot {
    let mut snap = busy();
    for health in snap.health.values_mut() {
        health.fetched_at = Some(now() - chrono::Duration::minutes(12));
    }
    snap
}

/// The same pass with `gh` missing. Airflow, gates and metrics are untouched — rule 4.
fn failed_source() -> Snapshot {
    let mut snap = busy();
    snap.prs.clear();
    snap.set_error("github", "gh: no such file or directory");
    snap.health.insert(
        "github".to_string(),
        SourceHealth::failed(now(), "gh: no such file or directory"),
    );
    snap
}

/// A context that has just been pointed at a factory which has not run anything yet.
fn empty() -> Snapshot {
    let mut snap = Snapshot::new(now());
    for source in ["airflow", "gates"] {
        snap.health
            .insert(source.to_string(), SourceHealth::fresh(now()));
    }
    snap
}

fn draw(model: &Model, theme: Theme, width: u16, height: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("a test terminal");
    terminal
        .draw(|frame| view::render(frame, model, theme))
        .expect("a frame");
    text(terminal.backend().buffer())
}

/// The rendered buffer as plain text, one line per row, trailing blanks trimmed.
fn text(buffer: &Buffer) -> String {
    let area = *buffer.area();
    (0..area.height)
        .map(|y| {
            let row: String = (0..area.width)
                .map(|x| buffer.cell((x, y)).map_or(" ", |cell| cell.symbol()))
                .collect();
            row.trim_end().to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn loaded(view: View, snapshot: Snapshot) -> Model {
    let mut model = model();
    model.view = view;
    model.log_open = true;
    model.note(format!(
        "watching {} at {}",
        model.env.context, model.env.airflow_url
    ));
    model.apply_snapshot(snapshot);
    model
}

#[test]
fn normal_attention_120x40() {
    let model = loaded(View::Attention, busy());
    insta::assert_snapshot!(
        "normal_attention_120x40",
        draw(&model, Theme::mono(), 120, 40)
    );
}

#[test]
fn normal_jobs_120x40() {
    let mut model = loaded(View::Jobs, busy());
    model.size = (120, 40);
    insta::assert_snapshot!("normal_jobs_120x40", draw(&model, Theme::mono(), 120, 40));
}

#[test]
fn normal_deliveries_120x40() {
    let model = loaded(View::Deliveries, busy());
    insta::assert_snapshot!(
        "normal_deliveries_120x40",
        draw(&model, Theme::mono(), 120, 40)
    );
}

#[test]
fn normal_infrastructure_120x40() {
    let model = loaded(View::Infrastructure, busy());
    insta::assert_snapshot!(
        "normal_infrastructure_120x40",
        draw(&model, Theme::mono(), 120, 40)
    );
}

#[test]
fn normal_history_120x40() {
    let model = loaded(View::History, busy());
    insta::assert_snapshot!(
        "normal_history_120x40",
        draw(&model, Theme::mono(), 120, 40)
    );
}

/// The approval screen: the exact job, the current gate, the evidence revision and the actor, all
/// on one screen before anything is written.
#[test]
fn review_120x40() {
    let mut model = loaded(View::Attention, busy());
    let id: GateId = "factory/manual__2026-03-14T09#0:job.approve_plan"
        .parse()
        .expect("a gate identity");
    let gate = model.snapshot.gate(&id).cloned().expect("the gate");
    model.review = Some(GateReview {
        id: id.clone(),
        revision: gate.current_revision(),
        ready: true,
        task_state: "awaiting_input".into(),
        job_state: "awaiting_input".into(),
        stage: "approve_plan".into(),
        options: gate.options.clone(),
        url: "https://airflow.example.com/dags/factory/runs/manual__2026-03-14T09".into(),
        gate,
    });
    model.selection.gate = Some(id.clone());
    model.selection.job = Some(id.job);
    model.view = View::Review;
    insta::assert_snapshot!("review_120x40", draw(&model, Theme::mono(), 120, 40));
}

/// Below 100 columns the detail pane folds away and the navigation stays.
#[test]
fn narrow_jobs_80x24() {
    let mut model = loaded(View::Jobs, busy());
    model.size = (80, 24);
    assert!(model.narrow() && !model.very_narrow());
    insta::assert_snapshot!("narrow_jobs_80x24", draw(&model, Theme::mono(), 80, 24));
}

/// Below 72 the navigation goes too, and the table gets the whole width.
#[test]
fn very_narrow_jobs_64x20() {
    let mut model = loaded(View::Jobs, busy());
    model.size = (64, 20);
    assert!(model.very_narrow());
    insta::assert_snapshot!(
        "very_narrow_jobs_64x20",
        draw(&model, Theme::mono(), 64, 20)
    );
}

#[test]
fn empty_120x40() {
    let model = loaded(View::Jobs, empty());
    insta::assert_snapshot!("empty_120x40", draw(&model, Theme::mono(), 120, 40));
}

#[test]
fn stale_120x40() {
    let mut model = loaded(View::Jobs, stale());
    // Twelve minutes with a five second refresh: every source is far past three intervals.
    assert!(model.stale());
    model.size = (120, 40);
    insta::assert_snapshot!("stale_120x40", draw(&model, Theme::mono(), 120, 40));
}

#[test]
fn failed_source_120x40() {
    let model = loaded(View::Deliveries, failed_source());
    insta::assert_snapshot!("failed_source_120x40", draw(&model, Theme::mono(), 120, 40));
}

#[test]
fn help_overlay_120x40() {
    let mut model = loaded(View::Jobs, busy());
    model.help_open = true;
    insta::assert_snapshot!("help_overlay_120x40", draw(&model, Theme::mono(), 120, 40));
}

#[test]
fn confirm_modal_120x40() {
    let mut model = loaded(View::Attention, busy());
    let key = model
        .rows()
        .first()
        .map(|row| row.key.clone())
        .expect("a row");
    model.select(&key);
    swf_tui::model::update(
        &mut model,
        swf_tui::model::Msg::Key(crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('a'),
            crossterm::event::KeyModifiers::NONE,
        )),
    );
    assert!(model.confirm.is_some(), "`a` must ask before it answers");
    insta::assert_snapshot!("confirm_modal_120x40", draw(&model, Theme::mono(), 120, 40));
}

/// A failing source must not blank the panes that are fine.
#[test]
fn one_dead_service_never_empties_another_pane() {
    let jobs = loaded(View::Jobs, failed_source());
    assert_eq!(jobs.rows().len(), 4, "airflow answered; jobs must be there");
    let screen = draw(&jobs, Theme::mono(), 120, 40);
    assert!(screen.contains("github"), "the failure must be named");
    assert!(screen.contains("manual__2026-03-14T09"), "{screen}");
}

/// Colour is the third signal, never the only one.
#[test]
fn turning_the_colour_off_removes_no_information() {
    for view in View::ALL {
        let model = loaded(view, busy());
        assert_eq!(
            draw(&model, Theme::mono(), 120, 40),
            draw(&model, Theme::new(true), 120, 40),
            "{view:?} says something in colour that it does not say in words"
        );
    }
}

/// Every state on screen carries its word as well as its glyph.
#[test]
fn a_state_is_readable_without_a_legend() {
    let screen = draw(&loaded(View::Jobs, busy()), Theme::mono(), 120, 40);
    for (glyph, word) in [
        ("◆", "awaiting_input"),
        ("▸", "running"),
        ("✗", "failed"),
        ("✓", "success"),
    ] {
        assert!(screen.contains(word), "{word} is missing: {screen}");
        assert!(screen.contains(glyph), "{word} lost its glyph: {screen}");
    }
}

/// A control sequence in a service payload must not reach the terminal.
#[test]
fn a_hostile_pr_title_cannot_repaint_the_screen() {
    let mut snapshot = busy();
    snapshot.prs[0].title = "own\u{1b}[2J\u{1b}]0;pwned\u{7}ed".into();
    let model = loaded(View::Deliveries, snapshot);
    let screen = draw(&model, Theme::mono(), 120, 40);
    assert!(screen.contains("owned"));
    assert!(!screen.contains('\u{1b}'));
    assert!(!screen.contains('\u{7}'));
}
