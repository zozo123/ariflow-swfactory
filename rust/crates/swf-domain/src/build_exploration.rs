//! Stochastic build exploration with deterministic authority boundaries.
//!
//! A model may shape the distribution of hypotheses the factory explores. It cannot add values
//! outside the declared search space, score its own children, waive evidence, or participate in
//! promotion. Sampling is local and replayable from the retained distribution plus entropy token.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};

pub const BUILD_EXPLORATION_SCHEMA_VERSION: u32 = 1;
pub const BUILD_EXPLORATION_AUTHORITY: &str = "exploration-only";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildAxis {
    pub name: String,
    pub options: Vec<String>,
}

impl BuildAxis {
    pub fn validate(&self) -> Result<(), BuildExplorationError> {
        if self.name.trim().is_empty() {
            return Err(BuildExplorationError::InvalidAxis);
        }
        if self.options.is_empty()
            || self.options.iter().any(|value| value.trim().is_empty())
            || self.options.iter().collect::<BTreeSet<_>>().len() != self.options.len()
        {
            return Err(BuildExplorationError::InvalidOptions);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildExplorationRequest {
    pub schema_version: u32,
    pub rubric_version: String,
    pub problem: String,
    pub axes: Vec<BuildAxis>,
}

impl BuildExplorationRequest {
    pub fn new(
        rubric_version: impl Into<String>,
        problem: impl Into<String>,
        axes: Vec<BuildAxis>,
    ) -> Self {
        Self {
            schema_version: BUILD_EXPLORATION_SCHEMA_VERSION,
            rubric_version: rubric_version.into(),
            problem: problem.into(),
            axes,
        }
    }

    pub fn validate(&self) -> Result<(), BuildExplorationError> {
        if self.schema_version != BUILD_EXPLORATION_SCHEMA_VERSION {
            return Err(BuildExplorationError::UnsupportedSchema);
        }
        if self.rubric_version.trim().is_empty() {
            return Err(BuildExplorationError::MissingRubricVersion);
        }
        if self.problem.trim().is_empty() {
            return Err(BuildExplorationError::MissingProblem);
        }
        if self.axes.is_empty() {
            return Err(BuildExplorationError::MissingAxes);
        }
        let mut names = BTreeSet::new();
        for axis in &self.axes {
            axis.validate()?;
            if !names.insert(axis.name.as_str()) {
                return Err(BuildExplorationError::DuplicateAxis);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxisDistribution {
    pub axis: String,
    pub probabilities: BTreeMap<String, f64>,
}

impl AxisDistribution {
    fn normalized(
        &self,
        declared: &BuildAxis,
    ) -> Result<BTreeMap<String, f64>, BuildExplorationError> {
        if self.axis != declared.name {
            return Err(BuildExplorationError::UnknownAxis);
        }
        let declared_values: BTreeSet<&str> =
            declared.options.iter().map(String::as_str).collect();
        let provided_values: BTreeSet<&str> =
            self.probabilities.keys().map(String::as_str).collect();
        if declared_values != provided_values {
            return Err(BuildExplorationError::UnknownOption);
        }
        let mut total = 0.0;
        for value in self.probabilities.values() {
            if !value.is_finite() || *value < 0.0 {
                return Err(BuildExplorationError::InvalidProbability);
            }
            total += *value;
        }
        if !total.is_finite() || total <= 0.0 {
            return Err(BuildExplorationError::InvalidProbability);
        }
        Ok(self
            .probabilities
            .iter()
            .map(|(name, value)| (name.clone(), value / total))
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildDistribution {
    pub schema_version: u32,
    pub model: String,
    pub axes: Vec<AxisDistribution>,
}

impl BuildDistribution {
    pub fn validate(
        &self,
        request: &BuildExplorationRequest,
    ) -> Result<(), BuildExplorationError> {
        request.validate()?;
        if self.schema_version != BUILD_EXPLORATION_SCHEMA_VERSION {
            return Err(BuildExplorationError::UnsupportedSchema);
        }
        if self.model.trim().is_empty() {
            return Err(BuildExplorationError::MissingModel);
        }
        if self.axes.len() != request.axes.len() {
            return Err(BuildExplorationError::AxisSetMismatch);
        }
        for (declared, distribution) in request.axes.iter().zip(&self.axes) {
            distribution.normalized(declared)?;
        }
        Ok(())
    }

    pub fn normalized(
        &self,
        request: &BuildExplorationRequest,
    ) -> Result<Self, BuildExplorationError> {
        self.validate(request)?;
        Ok(Self {
            schema_version: self.schema_version,
            model: self.model.clone(),
            axes: request
                .axes
                .iter()
                .zip(&self.axes)
                .map(|(declared, distribution)| {
                    Ok(AxisDistribution {
                        axis: declared.name.clone(),
                        probabilities: distribution.normalized(declared)?,
                    })
                })
                .collect::<Result<Vec<_>, BuildExplorationError>>()?,
        })
    }

    pub fn digest(
        &self,
        request: &BuildExplorationRequest,
    ) -> Result<String, BuildExplorationError> {
        let normalized = self.normalized(request)?;
        let payload =
            serde_json::to_vec(&normalized).map_err(|_| BuildExplorationError::Serialization)?;
        Ok(format!("builddist:v1:{}", sha256_hex(&payload)))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BuildHypothesisReceipt {
    pub schema_version: u32,
    pub authority: String,
    pub model: String,
    pub rubric_version: String,
    pub distribution_digest: String,
    pub entropy_token: String,
    pub choices: BTreeMap<String, String>,
}

impl BuildHypothesisReceipt {
    pub fn digest(&self) -> Result<String, BuildExplorationError> {
        let payload =
            serde_json::to_vec(self).map_err(|_| BuildExplorationError::Serialization)?;
        Ok(format!("buildsample:v1:{}", sha256_hex(&payload)))
    }
}

pub fn sample_build_hypothesis(
    request: &BuildExplorationRequest,
    distribution: &BuildDistribution,
    entropy_token: &str,
) -> Result<BuildHypothesisReceipt, BuildExplorationError> {
    request.validate()?;
    if entropy_token.trim().is_empty() {
        return Err(BuildExplorationError::MissingEntropy);
    }
    let normalized = distribution.normalized(request)?;
    let distribution_digest = normalized.digest(request)?;
    let mut choices = BTreeMap::new();

    for (axis_index, (axis, weights)) in request.axes.iter().zip(&normalized.axes).enumerate() {
        let draw = deterministic_draw(
            entropy_token,
            &distribution_digest,
            axis_index,
            &axis.name,
        );
        let mut cumulative = 0.0;
        let mut chosen = axis
            .options
            .last()
            .cloned()
            .ok_or(BuildExplorationError::InvalidOptions)?;
        for option in &axis.options {
            let probability = *weights
                .probabilities
                .get(option)
                .ok_or(BuildExplorationError::UnknownOption)?;
            cumulative += probability;
            if draw < cumulative {
                chosen = option.clone();
                break;
            }
        }
        choices.insert(axis.name.clone(), chosen);
    }

    Ok(BuildHypothesisReceipt {
        schema_version: BUILD_EXPLORATION_SCHEMA_VERSION,
        authority: BUILD_EXPLORATION_AUTHORITY.into(),
        model: normalized.model,
        rubric_version: request.rubric_version.clone(),
        distribution_digest,
        entropy_token: entropy_token.into(),
        choices,
    })
}

pub fn uniform_distribution(
    request: &BuildExplorationRequest,
) -> Result<BuildDistribution, BuildExplorationError> {
    request.validate()?;
    Ok(BuildDistribution {
        schema_version: BUILD_EXPLORATION_SCHEMA_VERSION,
        model: "local-uniform-fallback".into(),
        axes: request
            .axes
            .iter()
            .map(|axis| AxisDistribution {
                axis: axis.name.clone(),
                probabilities: axis
                    .options
                    .iter()
                    .map(|option| (option.clone(), 1.0))
                    .collect(),
            })
            .collect(),
    })
}

fn deterministic_draw(
    entropy_token: &str,
    distribution_digest: &str,
    axis_index: usize,
    axis_name: &str,
) -> f64 {
    let payload = format!(
        "jev-build-sample-v1\0{entropy_token}\0{distribution_digest}\0{axis_index}\0{axis_name}"
    );
    let hash = digest(&SHA256, payload.as_bytes());
    let bytes: [u8; 8] = hash.as_ref()[..8]
        .try_into()
        .expect("SHA-256 is always at least eight bytes");
    let integer = u64::from_be_bytes(bytes) >> 11;
    // A binary64 carries 53 integer bits exactly. Dividing by 2^53 therefore stays in [0, 1)
    // without the u64::MAX -> 2^64 rounding edge.
    (integer as f64) / ((1_u64 << 53) as f64)
}

fn sha256_hex(payload: &[u8]) -> String {
    let hash = digest(&SHA256, payload);
    let mut out = String::with_capacity(64);
    for byte in hash.as_ref() {
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildExplorationError {
    #[error("unsupported build exploration schema")]
    UnsupportedSchema,
    #[error("build exploration rubric version is required")]
    MissingRubricVersion,
    #[error("build exploration problem is required")]
    MissingProblem,
    #[error("build exploration requires at least one axis")]
    MissingAxes,
    #[error("build exploration axis name is invalid")]
    InvalidAxis,
    #[error("build exploration axis names must be unique")]
    DuplicateAxis,
    #[error("build exploration options must be nonempty and unique")]
    InvalidOptions,
    #[error("build distribution model is required")]
    MissingModel,
    #[error("build distribution axis set does not match the request")]
    AxisSetMismatch,
    #[error("build distribution contains an unknown axis")]
    UnknownAxis,
    #[error("build distribution contains a missing or unknown option")]
    UnknownOption,
    #[error("build distribution probability is invalid")]
    InvalidProbability,
    #[error("build sampling entropy token is required")]
    MissingEntropy,
    #[error("build exploration serialization failed")]
    Serialization,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> BuildExplorationRequest {
        BuildExplorationRequest::new(
            "jev-build-v1",
            "Make the stale-execution fence robust",
            vec![
                BuildAxis {
                    name: "strategy".into(),
                    options: vec!["repair".into(), "rethink".into(), "scratch".into()],
                },
                BuildAxis {
                    name: "review_lens".into(),
                    options: vec!["correctness".into(), "security".into()],
                },
            ],
        )
    }

    fn distribution() -> BuildDistribution {
        BuildDistribution {
            schema_version: BUILD_EXPLORATION_SCHEMA_VERSION,
            model: "jev-1.13.0".into(),
            axes: vec![
                AxisDistribution {
                    axis: "strategy".into(),
                    probabilities: BTreeMap::from([
                        ("repair".into(), 0.2),
                        ("rethink".into(), 0.6),
                        ("scratch".into(), 0.2),
                    ]),
                },
                AxisDistribution {
                    axis: "review_lens".into(),
                    probabilities: BTreeMap::from([
                        ("correctness".into(), 0.4),
                        ("security".into(), 0.6),
                    ]),
                },
            ],
        }
    }

    #[test]
    fn same_distribution_and_entropy_replay_the_same_hypothesis() {
        let first = sample_build_hypothesis(&request(), &distribution(), "entropy-17").unwrap();
        let second = sample_build_hypothesis(&request(), &distribution(), "entropy-17").unwrap();

        assert_eq!(first, second);
        assert_eq!(first.authority, "exploration-only");
        assert_eq!(first.digest().unwrap(), second.digest().unwrap());
    }

    #[test]
    fn provider_weights_are_normalized_before_sampling_and_digesting() {
        let mut scaled = distribution();
        for axis in &mut scaled.axes {
            for value in axis.probabilities.values_mut() {
                *value *= 100.0;
            }
        }
        assert_eq!(
            distribution().digest(&request()).unwrap(),
            scaled.digest(&request()).unwrap()
        );
        assert_eq!(
            sample_build_hypothesis(&request(), &distribution(), "same").unwrap(),
            sample_build_hypothesis(&request(), &scaled, "same").unwrap()
        );
    }

    #[test]
    fn provider_distribution_can_change_only_the_explored_hypothesis() {
        let mut repair = distribution();
        repair.axes[0].probabilities = BTreeMap::from([
            ("repair".into(), 1.0),
            ("rethink".into(), 0.0),
            ("scratch".into(), 0.0),
        ]);
        let mut scratch = distribution();
        scratch.axes[0].probabilities = BTreeMap::from([
            ("repair".into(), 0.0),
            ("rethink".into(), 0.0),
            ("scratch".into(), 1.0),
        ]);

        let repair_sample =
            sample_build_hypothesis(&request(), &repair, "same-entropy").unwrap();
        let scratch_sample =
            sample_build_hypothesis(&request(), &scratch, "same-entropy").unwrap();

        assert_eq!(repair_sample.choices["strategy"], "repair");
        assert_eq!(scratch_sample.choices["strategy"], "scratch");
        assert_eq!(repair_sample.authority, "exploration-only");
        assert_eq!(scratch_sample.authority, "exploration-only");
    }

    #[test]
    fn undeclared_values_are_refused_even_if_their_probability_is_zero() {
        let mut hostile = distribution();
        hostile.axes[0]
            .probabilities
            .insert("merge_without_tests".into(), 0.0);

        assert_eq!(
            hostile.validate(&request()),
            Err(BuildExplorationError::UnknownOption)
        );
    }

    #[test]
    fn malformed_probabilities_fail_closed() {
        for value in [f64::NAN, f64::INFINITY, -0.1] {
            let mut malformed = distribution();
            malformed.axes[0]
                .probabilities
                .insert("repair".into(), value);
            assert_eq!(
                malformed.validate(&request()),
                Err(BuildExplorationError::InvalidProbability)
            );
        }
    }

    #[test]
    fn local_uniform_distribution_is_a_safe_provider_fallback() {
        let fallback = uniform_distribution(&request()).unwrap();
        let receipt = sample_build_hypothesis(&request(), &fallback, "fallback-entropy").unwrap();

        assert_eq!(fallback.model, "local-uniform-fallback");
        assert_eq!(receipt.authority, "exploration-only");
        assert_eq!(receipt.choices.len(), request().axes.len());
    }
}
