//! Printable, parseable identities for the things an operator acts on.
//!
//! The boundary this module defends is rule 2 of the architecture: *the job is the unit*. An issue
//! number, a row index or "the run I was looking at" are not identities — a job is
//! `(dag_id, run_id, map_index)` and a gate is that plus a task id. Every command argument, every
//! TUI selection key and every audit line goes through these types, so a refresh cannot move the
//! cursor under the operator and a `--json` consumer can round-trip what it was given.
//!
//! Parsing is deliberately paranoid about separators, because Airflow's own ids are hostile:
//! `manual__2026-09-06T12:00:00+00:00` contains both `:` and `+`, and nothing forbids a `#`. So a
//! job id splits on the **last** `#` and only when the tail is an integer, and a gate id splits on
//! the **last** `:` and only when the tail looks like a task name. Anything else stays part of the
//! run id, which is the failure mode that loses the least information.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Every HITL task the shipped DAGs can create is `job.approve_<stage>` (`04-dags-blueprints.md`
/// §1.4), so a bare gate name typed at a prompt expands to exactly one real task id.
pub const GATE_TASK_PREFIX: &str = "job.approve_";

/// `map_index` for a task that was never expanded. It means "there is no job index yet", not
/// "job number minus one" — see `JobRow::mapped`.
pub const UNMAPPED: i32 = -1;

/// What a delivery branch is called before the issue/run tail (`06-delivery-evidence.md` §4.3).
pub const DELIVERY_BRANCH_PREFIX: &str = "factory/";

/// Why an identity could not be read back. Kept small on purpose: the operator gets one sentence
/// naming the shape that was expected, never a parser trace.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The whole argument was blank.
    #[error("{what} is empty")]
    Empty {
        /// Which identity was being read.
        what: &'static str,
    },
    /// No `/` at all, so there is no way to tell a DAG from a run.
    #[error("{input:?} is not a run reference; expected dag_id/run_id")]
    NotARunRef {
        /// What the operator typed.
        input: String,
    },
    /// The shape was right but one side of a separator was blank.
    #[error("{input:?} has an empty {part}")]
    EmptyPart {
        /// What the operator typed.
        input: String,
        /// The blank component.
        part: &'static str,
    },
    /// No task segment, so this addresses a job and not one of its gates.
    #[error("{input:?} is not a gate; expected dag_id/run_id#index:gate")]
    NotAGate {
        /// What the operator typed.
        input: String,
    },
    /// None of the four accepted delivery spellings.
    #[error(
        "{input:?} is not a delivery; expected factory/<issue>-<run>, a PR number or URL, \
         or a path to a checkout"
    )]
    NotADelivery {
        /// What the operator typed.
        input: String,
    },
}

/// A run, addressable without a job index. Rendered `dag/run`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RunRef {
    /// The DAG this run belongs to.
    pub dag_id: String,
    /// Airflow's `dag_run_id`, verbatim — it may contain `:`, `+` and `#`.
    pub run_id: String,
}

impl RunRef {
    /// Build a reference without going through the textual form.
    pub fn new(dag_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            dag_id: dag_id.into(),
            run_id: run_id.into(),
        }
    }

    /// Read `dag/run`. Splits on the **first** `/`: DAG ids never contain one, run ids might.
    pub fn parse(input: &str) -> Result<Self, IdError> {
        let text = input.trim();
        if text.is_empty() {
            return Err(IdError::Empty {
                what: "run reference",
            });
        }
        let Some((dag_id, run_id)) = text.split_once('/') else {
            return Err(IdError::NotARunRef {
                input: text.to_string(),
            });
        };
        if dag_id.is_empty() {
            return Err(IdError::EmptyPart {
                input: text.to_string(),
                part: "dag_id",
            });
        }
        if run_id.is_empty() {
            return Err(IdError::EmptyPart {
                input: text.to_string(),
                part: "run_id",
            });
        }
        Ok(Self::new(dag_id, run_id))
    }

    /// The whole run, seen as the unmapped job — what a collapsed table row addresses.
    pub fn job(&self) -> JobId {
        JobId {
            dag_id: self.dag_id.clone(),
            run_id: self.run_id.clone(),
            map_index: UNMAPPED,
        }
    }
}

impl fmt::Display for RunRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.dag_id, self.run_id)
    }
}

impl FromStr for RunRef {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// One mapped job of a DAG run — the unit a gate, a sandbox and a delivery all belong to.
///
/// Rendered `dag/run#index`; a run id may contain `:`, `+` and even `#`, so parsing splits on the
/// LAST `#` and only accepts the split when the tail is an integer. `dag/run` alone is accepted as
/// the unmapped job (`map_index == -1`), which is how a run that has not fanned out yet is named.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobId {
    /// The DAG this job's run belongs to.
    pub dag_id: String,
    /// Airflow's `dag_run_id`, verbatim.
    pub run_id: String,
    /// The index `fan_out` assigned, or `-1` when there is no job index yet.
    pub map_index: i32,
}

impl JobId {
    /// Build an identity without going through the textual form.
    pub fn new(dag_id: impl Into<String>, run_id: impl Into<String>, map_index: i32) -> Self {
        Self {
            dag_id: dag_id.into(),
            run_id: run_id.into(),
            map_index,
        }
    }

    /// Read `dag/run#3`, or `dag/run` for the unmapped job.
    pub fn parse(input: &str) -> Result<Self, IdError> {
        let text = input.trim();
        if text.is_empty() {
            return Err(IdError::Empty { what: "job id" });
        }
        // Only a trailing integer after the last `#` is an index. `manual__x#y` keeps its `#`.
        let (head, map_index) = match text.rsplit_once('#') {
            Some((head, tail)) if !head.is_empty() => match tail.parse::<i32>() {
                Ok(index) => (head, index),
                Err(_) => (text, UNMAPPED),
            },
            _ => (text, UNMAPPED),
        };
        let run = RunRef::parse(head)?;
        Ok(Self {
            dag_id: run.dag_id,
            run_id: run.run_id,
            map_index,
        })
    }

    /// The run this job belongs to, for the endpoints that take no index.
    pub fn run(&self) -> RunRef {
        RunRef::new(self.dag_id.clone(), self.run_id.clone())
    }

    /// True once `fan_out` has given this job a real index.
    pub fn mapped(&self) -> bool {
        self.map_index >= 0
    }

    /// Address one of this job's gates.
    pub fn gate(&self, task_id: impl Into<String>) -> GateId {
        GateId {
            job: self.clone(),
            task_id: task_id.into(),
        }
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.dag_id, self.run_id)?;
        if self.map_index != UNMAPPED {
            write!(f, "#{}", self.map_index)?;
        }
        Ok(())
    }
}

impl FromStr for JobId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// One approval gate of one job. Rendered `dag/run#index:task_id`.
///
/// The short form `dag/run#3:intent` expands to the real task id `job.approve_intent`, because
/// that is the only HITL task a blueprint can produce for the `intent` stage. Answering the wrong
/// task id is not a typo an operator should be able to make.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GateId {
    /// The job whose stage is waiting.
    pub job: JobId,
    /// The full Airflow task id, e.g. `job.approve_intent`.
    pub task_id: String,
}

impl GateId {
    /// Build an identity from parts that are already full task ids.
    pub fn new(job: JobId, task_id: impl Into<String>) -> Self {
        Self {
            job,
            task_id: task_id.into(),
        }
    }

    /// Read `dag/run#3:job.approve_intent` or the short `dag/run#3:intent`.
    ///
    /// Splits on the LAST `:` and only accepts the split when the tail contains a letter — run ids
    /// end in `12:00:00+00:00`, and `00` is not a task name.
    pub fn parse(input: &str) -> Result<Self, IdError> {
        let text = input.trim();
        if text.is_empty() {
            return Err(IdError::Empty { what: "gate id" });
        }
        let Some((head, tail)) = text.rsplit_once(':') else {
            return Err(IdError::NotAGate {
                input: text.to_string(),
            });
        };
        if head.is_empty() || !tail.chars().any(|c| c.is_ascii_alphabetic()) {
            return Err(IdError::NotAGate {
                input: text.to_string(),
            });
        }
        let job = JobId::parse(head)?;
        Ok(Self {
            job,
            task_id: expand_gate_task(tail),
        })
    }

    /// The bare stage name an operator recognises: `job.approve_plan` -> `approve_plan`.
    pub fn short_name(&self) -> &str {
        self.task_id.rsplit('.').next().unwrap_or(&self.task_id)
    }
}

/// Expand a bare gate name to the task id Airflow actually created.
///
/// `intent` and `approve_intent` both mean `job.approve_intent`; anything already carrying a `.`
/// is taken verbatim, because that is a full Airflow task id and the caller knows better than we
/// do what a future blueprint named its gates.
fn expand_gate_task(raw: &str) -> String {
    let name = raw.trim();
    if name.contains('.') {
        return name.to_string();
    }
    let stage = name.strip_prefix("approve_").unwrap_or(name);
    format!("{GATE_TASK_PREFIX}{stage}")
}

impl fmt::Display for GateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.job, self.task_id)
    }
}

impl FromStr for GateId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// What `swf deliveries verify` was pointed at.
///
/// Four spellings reach the same delivery and an operator will use whichever one their last
/// command printed: the published branch, the PR number, a URL pasted from a browser, or a local
/// checkout to verify with no network at all. Keeping them as distinct variants means the verifier
/// never has to guess whether `1234` was a PR or a directory.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryId {
    /// `factory/<issue_id>-<run_id>` — the branch a delivery publishes.
    Branch {
        /// The issue this delivery answers.
        issue_id: String,
        /// The factory run that produced it.
        run_id: String,
    },
    /// A pull request number in the context's repo.
    Pr(u64),
    /// A pull request or branch URL, kept verbatim.
    Url(String),
    /// A local checkout, for `--no-network` verification.
    Path(String),
}

impl DeliveryId {
    /// Read whichever of the four spellings the operator used.
    ///
    /// Order matters: a PR number, then a URL, then a `factory/` branch, and only then a path.
    /// A path that begins with `factory/` and contains a `-` is therefore read as a branch — that
    /// ambiguity is real, and resolving it toward the branch is the reading an operator meant.
    pub fn parse(input: &str) -> Result<Self, IdError> {
        let text = input.trim();
        if text.is_empty() {
            return Err(IdError::Empty { what: "delivery" });
        }
        let digits = text.strip_prefix('#').unwrap_or(text);
        if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
            return match digits.parse::<u64>() {
                Ok(number) => Ok(Self::Pr(number)),
                Err(_) => Err(IdError::NotADelivery {
                    input: text.to_string(),
                }),
            };
        }
        if text.starts_with("http://") || text.starts_with("https://") {
            return Ok(Self::Url(text.to_string()));
        }
        if let Some(tail) = text.strip_prefix(DELIVERY_BRANCH_PREFIX) {
            // Issue ids may contain `-` and `.`, so the split is on the LAST hyphen
            // (`06-delivery-evidence.md` §5.5) and is still only a heuristic.
            let Some((issue_id, run_id)) = tail.rsplit_once('-') else {
                return Err(IdError::NotADelivery {
                    input: text.to_string(),
                });
            };
            if issue_id.is_empty() {
                return Err(IdError::EmptyPart {
                    input: text.to_string(),
                    part: "issue_id",
                });
            }
            if run_id.is_empty() {
                return Err(IdError::EmptyPart {
                    input: text.to_string(),
                    part: "run_id",
                });
            }
            return Ok(Self::Branch {
                issue_id: issue_id.to_string(),
                run_id: run_id.to_string(),
            });
        }
        Ok(Self::Path(text.to_string()))
    }

    /// The git branch this delivery lives on, when the identity carries enough to know it.
    pub fn branch_name(&self) -> Option<String> {
        match self {
            Self::Branch { issue_id, run_id } => {
                Some(format!("{DELIVERY_BRANCH_PREFIX}{issue_id}-{run_id}"))
            }
            _ => None,
        }
    }
}

impl fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Branch { issue_id, run_id } => {
                write!(f, "{DELIVERY_BRANCH_PREFIX}{issue_id}-{run_id}")
            }
            Self::Pr(number) => write!(f, "#{number}"),
            Self::Url(url) => f.write_str(url),
            Self::Path(path) => f.write_str(path),
        }
    }
}

impl FromStr for DeliveryId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The run ids that actually occur, plus the ones chosen to break a naive split.
    const RUN_IDS: &[&str] = &[
        "manual__2026-09-06T12:00:00+00:00",
        "scheduled__2026-09-06T12:00:00+00:00",
        "r1",
        "run#with#hash",
        "run#",
        "weird:run+id",
        "run-with-dashes",
    ];

    #[test]
    fn job_id_round_trips_for_every_hostile_run_id() {
        for run_id in RUN_IDS {
            for map_index in [-1, 0, 1, 7, 4096] {
                let id = JobId::new("factory", *run_id, map_index);
                let back = JobId::parse(&id.to_string()).expect("round trip");
                assert_eq!(back, id, "run_id={run_id} map_index={map_index}");
            }
        }
    }

    #[test]
    fn gate_id_round_trips_for_every_hostile_run_id() {
        for run_id in RUN_IDS {
            for task in ["job.approve_intent", "job.approve_plan"] {
                let id = GateId::new(JobId::new("factory", *run_id, 0), task);
                let back = GateId::parse(&id.to_string()).expect("round trip");
                assert_eq!(back, id, "run_id={run_id} task={task}");
            }
        }
    }

    #[test]
    fn run_ref_round_trips() {
        for run_id in RUN_IDS {
            let id = RunRef::new("factory", *run_id);
            assert_eq!(RunRef::parse(&id.to_string()), Ok(id));
        }
    }

    #[test]
    fn a_bare_run_is_the_unmapped_job() {
        let id = JobId::parse("factory/r1").expect("parse");
        assert_eq!(id.map_index, UNMAPPED);
        assert!(!id.mapped());
        assert_eq!(id.to_string(), "factory/r1");
    }

    #[test]
    fn a_hash_that_is_not_an_index_stays_in_the_run_id() {
        let id = JobId::parse("factory/manual__a#b").expect("parse");
        assert_eq!(id.run_id, "manual__a#b");
        assert_eq!(id.map_index, UNMAPPED);
    }

    #[test]
    fn an_empty_map_index_stays_in_the_run_id() {
        let id = JobId::parse("factory/r1#").expect("parse");
        assert_eq!(id.run_id, "r1#");
        assert_eq!(id.map_index, UNMAPPED);
        assert_eq!(JobId::parse(&id.to_string()), Ok(id));
    }

    #[test]
    fn the_last_hash_wins() {
        let id = JobId::parse("factory/run#with#hash#3").expect("parse");
        assert_eq!(id.run_id, "run#with#hash");
        assert_eq!(id.map_index, 3);
    }

    #[test]
    fn timestamped_run_ids_survive_the_gate_colon_split() {
        let id =
            GateId::parse("factory/manual__2026-09-06T12:00:00+00:00#0:intent").expect("parse");
        assert_eq!(id.job.run_id, "manual__2026-09-06T12:00:00+00:00");
        assert_eq!(id.job.map_index, 0);
        assert_eq!(id.task_id, "job.approve_intent");
    }

    #[test]
    fn a_run_id_alone_is_not_a_gate() {
        let err = GateId::parse("factory/manual__2026-09-06T12:00:00+00:00#0").unwrap_err();
        assert!(matches!(err, IdError::NotAGate { .. }));
    }

    #[test]
    fn bare_gate_names_expand_to_real_task_ids() {
        for (typed, expected) in [
            ("intent", "job.approve_intent"),
            ("plan", "job.approve_plan"),
            ("approve_intent", "job.approve_intent"),
            ("job.approve_plan", "job.approve_plan"),
        ] {
            let id = GateId::parse(&format!("factory/r1#0:{typed}")).expect("parse");
            assert_eq!(id.task_id, expected, "typed={typed}");
            assert_eq!(id.short_name(), expected.trim_start_matches("job."));
        }
    }

    #[test]
    fn malformed_identities_say_what_was_expected() {
        assert!(matches!(JobId::parse("  "), Err(IdError::Empty { .. })));
        assert!(matches!(
            JobId::parse("factory"),
            Err(IdError::NotARunRef { .. })
        ));
        assert!(matches!(
            JobId::parse("/r1"),
            Err(IdError::EmptyPart { part: "dag_id", .. })
        ));
        assert!(matches!(
            JobId::parse("factory/"),
            Err(IdError::EmptyPart { part: "run_id", .. })
        ));
        assert!(matches!(
            GateId::parse("factory/r1#0:"),
            Err(IdError::NotAGate { .. })
        ));
    }

    #[test]
    fn delivery_ids_round_trip_in_all_four_spellings() {
        let cases = [
            DeliveryId::Branch {
                issue_id: "demo.1-a".into(),
                run_id: "r42".into(),
            },
            DeliveryId::Pr(1234),
            DeliveryId::Url("https://github.com/o/r/pull/7".into()),
            DeliveryId::Path("/tmp/checkout".into()),
            DeliveryId::Path("../work/tree".into()),
        ];
        for id in cases {
            let back = DeliveryId::parse(&id.to_string()).expect("round trip");
            assert_eq!(back, id);
        }
    }

    #[test]
    fn delivery_branch_splits_on_the_last_hyphen() {
        let id = DeliveryId::parse("factory/demo-issue-1-r99").expect("parse");
        assert_eq!(
            id,
            DeliveryId::Branch {
                issue_id: "demo-issue-1".into(),
                run_id: "r99".into()
            }
        );
        assert_eq!(
            id.branch_name().as_deref(),
            Some("factory/demo-issue-1-r99")
        );
    }

    #[test]
    fn a_bare_number_is_a_pull_request() {
        assert_eq!(DeliveryId::parse("42"), Ok(DeliveryId::Pr(42)));
        assert_eq!(DeliveryId::parse("#42"), Ok(DeliveryId::Pr(42)));
        assert!(DeliveryId::parse("").is_err());
        assert!(matches!(
            DeliveryId::parse("factory/nohyphen"),
            Err(IdError::NotADelivery { .. })
        ));
    }

    #[test]
    fn job_and_gate_and_run_convert_between_each_other() {
        let job = JobId::new("factory", "r1", 2);
        assert_eq!(job.run(), RunRef::new("factory", "r1"));
        assert_eq!(job.run().job().map_index, UNMAPPED);
        assert_eq!(
            job.gate("job.approve_plan").to_string(),
            "factory/r1#2:job.approve_plan"
        );
    }

    #[test]
    fn from_str_is_wired_for_clap() {
        assert_eq!(
            "factory/r1#0".parse::<JobId>(),
            Ok(JobId::new("factory", "r1", 0))
        );
        assert_eq!(
            "factory/r1".parse::<RunRef>(),
            Ok(RunRef::new("factory", "r1"))
        );
        assert_eq!("7".parse::<DeliveryId>(), Ok(DeliveryId::Pr(7)));
        assert!("factory/r1#0:plan".parse::<GateId>().is_ok());
    }
}
