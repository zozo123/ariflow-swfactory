//! Bounded TypeSafe/Jev adapter for advisory-only backlog triage.
//!
//! This client has no mutation authority. It sends a redacted proposal/candidate state to one
//! pinned model and returns typed relationship suggestions. There is deliberately no fallback
//! model, retry loop, or auto-action threshold in the first shadow implementation.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use swf_domain::advisory::{AdvisoryRelation, AdvisoryRequest, AdvisorySuggestion, ExistingIssue};
use swf_domain::build_exploration::{
    uniform_distribution, AxisDistribution, BuildDistribution, BuildExplorationRequest,
    BUILD_EXPLORATION_SCHEMA_VERSION,
};
use tokio_util::sync::CancellationToken;

use crate::error::{AdapterError, Result};

pub const JEV_ENDPOINT: &str = "https://api.typesafe.ai/v1/systemone";
pub const JEV_MODEL: &str = "jev-1.13.0";

pub struct JevApi {
    endpoint: String,
    token: String,
    http: Client,
    timeout: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BuildOracleResult {
    pub distribution: BuildDistribution,
    pub provider_available: bool,
    pub diagnostic: Option<String>,
}

impl JevApi {
    pub fn new(token: String, timeout: Duration) -> Result<Self> {
        Self::build(JEV_ENDPOINT, token, timeout)
    }

    fn build(endpoint: &str, token: String, timeout: Duration) -> Result<Self> {
        let url =
            url::Url::parse(endpoint).map_err(|_| AdapterError::refused("invalid Jev endpoint"))?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(AdapterError::refused(
                "Jev endpoint must use HTTPS without credentials, query, or fragment",
            ));
        }
        if token.len() < 24 || token.chars().any(char::is_whitespace) {
            return Err(AdapterError::Auth {
                detail: "set TYPESAFE_API_KEY to a valid Jev API key".into(),
            });
        }
        let http = Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AdapterError::refused("could not create Jev client"))?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            token,
            http,
            timeout,
        })
    }

    /// Ask the pinned model for probability mass over an already-bounded build search space.
    ///
    /// The response is a distribution only. Sampling happens locally in swf-domain, and neither
    /// this adapter nor Jev can add an axis/value or participate in candidate scoring/promotion.
    pub async fn build_distribution(
        &self,
        request: &BuildExplorationRequest,
        cancel: &CancellationToken,
    ) -> Result<BuildDistribution> {
        request
            .validate()
            .map_err(|error| AdapterError::refused(error.to_string()))?;

        let body = build_request_body(request);
        let call = async {
            let response = self
                .http
                .post(&self.endpoint)
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        AdapterError::Timeout {
                            what: "Jev build exploration".into(),
                            after: self.timeout,
                        }
                    } else {
                        AdapterError::Unreachable {
                            what: "Jev build exploration".into(),
                            detail: "request failed".into(),
                        }
                    }
                })?;

            let status = response.status().as_u16();
            if status >= 300 {
                return Err(match status {
                    401 | 403 => AdapterError::Auth {
                        detail: format!(
                            "Jev build exploration rejected credentials (HTTP {status})"
                        ),
                    },
                    _ => AdapterError::Status {
                        code: Some(status),
                        detail: format!("Jev build exploration failed (HTTP {status})"),
                    },
                });
            }

            let wire: BuildWireResponse =
                response.json().await.map_err(|_| AdapterError::Decode {
                    what: "Jev build exploration".into(),
                    detail: "response does not match the expected typed JSON shape".into(),
                })?;
            parse_build_response(request, wire)
        };

        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(AdapterError::Cancelled),
            result = call => result,
        }
    }

    /// Shadow-mode fallback: provider failure changes exploration quality, never build liveness.
    ///
    /// Cancellation remains cancellation. Every other provider failure degrades to the ordinary
    /// local uniform exploration distribution and returns a bounded diagnostic for evidence.
    pub async fn build_distribution_or_uniform(
        &self,
        request: &BuildExplorationRequest,
        cancel: &CancellationToken,
    ) -> Result<BuildOracleResult> {
        match self.build_distribution(request, cancel).await {
            Ok(distribution) => Ok(BuildOracleResult {
                distribution,
                provider_available: true,
                diagnostic: None,
            }),
            Err(AdapterError::Cancelled) => Err(AdapterError::Cancelled),
            Err(error) => {
                let distribution = uniform_distribution(request)
                    .map_err(|domain| AdapterError::refused(domain.to_string()))?;
                Ok(BuildOracleResult {
                    distribution,
                    provider_available: false,
                    diagnostic: Some(format!("{}: {}", error.kind(), error)),
                })
            }
        }
    }

    pub async fn relate(
        &self,
        request: &AdvisoryRequest,
        cancel: &CancellationToken,
    ) -> Result<Vec<AdvisorySuggestion>> {
        request
            .validate()
            .map_err(|error| AdapterError::refused(error.to_string()))?;

        let body = request_body(request);
        let call = async {
            let response = self
                .http
                .post(&self.endpoint)
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        AdapterError::Timeout {
                            what: "Jev advisory".into(),
                            after: self.timeout,
                        }
                    } else {
                        AdapterError::Unreachable {
                            what: "Jev advisory".into(),
                            detail: "request failed".into(),
                        }
                    }
                })?;

            let status = response.status().as_u16();
            if status >= 300 {
                return Err(match status {
                    401 | 403 => AdapterError::Auth {
                        detail: format!("Jev advisory rejected credentials (HTTP {status})"),
                    },
                    _ => AdapterError::Status {
                        code: Some(status),
                        detail: format!("Jev advisory failed (HTTP {status})"),
                    },
                });
            }

            let wire: WireResponse = response.json().await.map_err(|_| AdapterError::Decode {
                what: "Jev advisory".into(),
                detail: "response does not match the expected typed JSON shape".into(),
            })?;
            parse_response(request, wire)
        };

        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(AdapterError::Cancelled),
            result = call => result,
        }
    }
}

fn build_request_body(request: &BuildExplorationRequest) -> Value {
    let state = json!({
        "problem": request.problem,
        "rubric_version": request.rubric_version,
        "authority": "exploration-only",
        "axes": request.axes,
    });
    let mut questions = serde_json::Map::new();
    for (index, axis) in request.axes.iter().enumerate() {
        let criteria: serde_json::Map<String, Value> = axis
            .options
            .iter()
            .map(|option| {
                (
                    option.clone(),
                    Value::String(format!(
                        "Allocate exploration probability to {option:?} only as a build hypothesis; it receives no correctness or promotion authority."
                    )),
                )
            })
            .collect();
        questions.insert(
            build_question_id(index),
            json!({
                "type": "choice",
                "instructions": format!(
                    "For build axis {:?}, estimate which declared option is worth exploring. Return probabilities over the declared choices only. This is hypothesis generation, not verification, ranking, publication, or promotion.",
                    axis.name
                ),
                "criteria": criteria,
            }),
        );
    }
    json!({
        "model": JEV_MODEL,
        "state": state,
        "questions": questions,
    })
}

fn build_question_id(index: usize) -> String {
    format!("build_axis_{index}")
}

#[derive(Debug, Deserialize)]
struct BuildWireResponse {
    model: String,
    answers: BTreeMap<String, WireChoice>,
    #[serde(default)]
    usage: Option<WireUsage>,
}

fn parse_build_response(
    request: &BuildExplorationRequest,
    wire: BuildWireResponse,
) -> Result<BuildDistribution> {
    if wire.model != JEV_MODEL {
        return Err(AdapterError::Decode {
            what: "Jev build exploration".into(),
            detail: "resolved model does not match pinned jev-1.13.0".into(),
        });
    }
    if wire.answers.len() != request.axes.len() {
        return Err(AdapterError::Decode {
            what: "Jev build exploration".into(),
            detail: "answer set does not match requested build axes".into(),
        });
    }
    if let Some(usage) = &wire.usage {
        let _ = (usage.input_tokens, usage.output_tokens);
    }

    let mut axes = Vec::with_capacity(request.axes.len());
    for (index, declared) in request.axes.iter().enumerate() {
        let id = build_question_id(index);
        let answer = wire.answers.get(&id).ok_or_else(|| AdapterError::Decode {
            what: "Jev build exploration".into(),
            detail: format!("missing typed answer for build axis {:?}", declared.name),
        })?;
        if answer.kind != "choice" {
            return Err(AdapterError::Decode {
                what: "Jev build exploration".into(),
                detail: format!("answer for build axis {:?} is not a choice", declared.name),
            });
        }
        if !answer.confidence.is_finite() || !(0.0..=1.0).contains(&answer.confidence) {
            return Err(AdapterError::Decode {
                what: "Jev build exploration".into(),
                detail: format!("invalid confidence for build axis {:?}", declared.name),
            });
        }
        if !declared.options.iter().any(|option| option == &answer.choice) {
            return Err(AdapterError::Decode {
                what: "Jev build exploration".into(),
                detail: format!("unknown selected option for build axis {:?}", declared.name),
            });
        }
        if !answer.probabilities.contains_key(&answer.choice) {
            return Err(AdapterError::Decode {
                what: "Jev build exploration".into(),
                detail: format!(
                    "selected option has no probability for build axis {:?}",
                    declared.name
                ),
            });
        }
        axes.push(AxisDistribution {
            axis: declared.name.clone(),
            probabilities: answer.probabilities.clone(),
        });
    }

    let distribution = BuildDistribution {
        schema_version: BUILD_EXPLORATION_SCHEMA_VERSION,
        model: wire.model,
        axes,
    };
    distribution
        .validate(request)
        .map_err(|error| AdapterError::Decode {
            what: "Jev build exploration".into(),
            detail: error.to_string(),
        })?;
    distribution
        .normalized(request)
        .map_err(|error| AdapterError::Decode {
            what: "Jev build exploration".into(),
            detail: error.to_string(),
        })
}

fn request_body(request: &AdvisoryRequest) -> Value {
    let state = json!({
        "proposal": request.proposal,
        "candidates": request.candidates,
        "rubric_version": request.rubric_version,
    });
    let mut questions = serde_json::Map::new();
    for issue in &request.candidates {
        questions.insert(
            question_id(issue),
            json!({
                "type": "choice",
                "instructions": format!(
                    "How does existing issue #{} relate to the proposal? Judge observable acceptance criteria. If evidence is insufficient, choose insufficient_evidence. Never infer that work may be closed or suppressed.",
                    issue.number
                ),
                "criteria": {
                    "potential_duplicate": "same observable outcome and acceptance criteria",
                    "extends": "adds independently testable acceptance criteria",
                    "conflicts": "requires an incompatible observable outcome",
                    "related": "relevant dependency or neighboring work, not a duplicate",
                    "unrelated": "no material overlap",
                    "insufficient_evidence": "acceptance criteria or context are too incomplete to decide"
                }
            }),
        );
    }
    json!({
        "model": JEV_MODEL,
        "state": state,
        "questions": questions,
    })
}

fn question_id(issue: &ExistingIssue) -> String {
    format!("issue_{}", issue.number)
}

#[derive(Debug, Deserialize)]
struct WireResponse {
    model: String,
    answers: BTreeMap<String, WireChoice>,
    #[serde(default)]
    usage: Option<WireUsage>,
}

#[derive(Debug, Deserialize)]
struct WireChoice {
    #[serde(rename = "type")]
    kind: String,
    choice: String,
    confidence: f64,
    probabilities: BTreeMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct WireUsage {
    input_tokens: u64,
    output_tokens: u64,
}

fn parse_response(
    request: &AdvisoryRequest,
    wire: WireResponse,
) -> Result<Vec<AdvisorySuggestion>> {
    if wire.model != JEV_MODEL {
        return Err(AdapterError::Decode {
            what: "Jev advisory".into(),
            detail: "resolved model does not match pinned jev-1.13.0".into(),
        });
    }
    if wire.answers.len() != request.candidates.len() {
        return Err(AdapterError::Decode {
            what: "Jev advisory".into(),
            detail: "answer set does not match requested candidates".into(),
        });
    }
    if let Some(usage) = &wire.usage {
        let _ = (usage.input_tokens, usage.output_tokens);
    }

    let mut out = Vec::with_capacity(request.candidates.len());
    for issue in &request.candidates {
        let id = question_id(issue);
        let answer = wire.answers.get(&id).ok_or_else(|| AdapterError::Decode {
            what: "Jev advisory".into(),
            detail: format!("missing typed answer for issue #{}", issue.number),
        })?;
        if answer.kind != "choice" {
            return Err(AdapterError::Decode {
                what: "Jev advisory".into(),
                detail: format!("answer for issue #{} is not a choice", issue.number),
            });
        }
        if !answer.confidence.is_finite() || !(0.0..=1.0).contains(&answer.confidence) {
            return Err(AdapterError::Decode {
                what: "Jev advisory".into(),
                detail: format!("invalid confidence for issue #{}", issue.number),
            });
        }
        if answer.probabilities.is_empty()
            || answer
                .probabilities
                .values()
                .any(|value| !value.is_finite() || !(0.0..=1.0).contains(value))
        {
            return Err(AdapterError::Decode {
                what: "Jev advisory".into(),
                detail: format!(
                    "invalid probability distribution for issue #{}",
                    issue.number
                ),
            });
        }
        let relation =
            serde_json::from_value::<AdvisoryRelation>(Value::String(answer.choice.clone()))
                .map_err(|_| AdapterError::Decode {
                    what: "Jev advisory".into(),
                    detail: format!("unknown relationship for issue #{}", issue.number),
                })?;
        if !answer.probabilities.contains_key(&answer.choice) {
            return Err(AdapterError::Decode {
                what: "Jev advisory".into(),
                detail: format!(
                    "selected relationship has no probability for issue #{}",
                    issue.number
                ),
            });
        }

        out.push(AdvisorySuggestion {
            existing_issue: issue.number,
            relationship: relation,
            confidence: answer.confidence,
            // Initial rollout is shadow-only: every model result is review-required.
            needs_review: true,
            rationale: String::new(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use swf_domain::advisory::{ExistingIssue, WorkItem};
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TOKEN: &str = "apikey_aaaaaaaa_bbbbbbbbbbbbbbbbbbbbbbbb";

    fn request() -> AdvisoryRequest {
        AdvisoryRequest::new(
            "jev-triage-v1",
            WorkItem {
                id: "proposal-1".into(),
                title: "Reject stale execution requests before a stage handler runs".into(),
                body: String::new(),
                acceptance_criteria: vec!["stale request is rejected before handler entry".into()],
            },
            vec![ExistingIssue {
                number: 2255,
                title: "Require a valid fence before invoking a stage handler".into(),
                body: String::new(),
                acceptance_criteria: vec!["handler checks current epoch fence".into()],
            }],
        )
    }

    fn test_client(server: &MockServer) -> JevApi {
        let endpoint = format!("{}/v1/systemone", server.uri());
        let http = Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        JevApi {
            endpoint,
            token: TOKEN.into(),
            http,
            timeout: Duration::from_secs(2),
        }
    }

    fn good_body() -> Value {
        json!({
            "model": JEV_MODEL,
            "answers": {
                "issue_2255": {
                    "type": "choice",
                    "choice": "potential_duplicate",
                    "confidence": 0.93,
                    "probabilities": {
                        "potential_duplicate": 0.93,
                        "extends": 0.02,
                        "conflicts": 0.01,
                        "related": 0.02,
                        "unrelated": 0.01,
                        "insufficient_evidence": 0.01
                    }
                }
            },
            "usage": {"input_tokens": 120, "output_tokens": 12}
        })
    }

    #[tokio::test]
    async fn sends_pinned_model_and_returns_review_only_suggestion() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .and(body_partial_json(json!({"model": JEV_MODEL})))
            .respond_with(ResponseTemplate::new(200).set_body_json(good_body()))
            .expect(1)
            .mount(&server)
            .await;

        let suggestions = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(suggestions.len(), 1);
        assert_eq!(
            suggestions[0].relationship,
            AdvisoryRelation::PotentialDuplicate
        );
        assert_eq!(suggestions[0].confidence, 0.93);
        assert!(suggestions[0].needs_review);
    }

    #[tokio::test]
    async fn resolved_model_mismatch_is_refused() {
        let server = MockServer::start().await;
        let mut body = good_body();
        body["model"] = json!("jev-latest");
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, AdapterError::Decode { .. }));
    }

    #[tokio::test]
    async fn unknown_choice_is_refused() {
        let server = MockServer::start().await;
        let mut body = good_body();
        body["answers"]["issue_2255"]["choice"] = json!("automatic_close");
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, AdapterError::Decode { .. }));
    }

    #[tokio::test]
    async fn invalid_confidence_is_refused() {
        let server = MockServer::start().await;
        let mut body = good_body();
        body["answers"]["issue_2255"]["confidence"] = json!(1.2);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, AdapterError::Decode { .. }));
    }

    #[tokio::test]
    async fn auth_failure_never_echoes_the_credential() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string(TOKEN))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, AdapterError::Auth { .. }));
        assert!(!error.to_string().contains(TOKEN));
    }


    fn build_request() -> BuildExplorationRequest {
        BuildExplorationRequest::new(
            "jev-build-v1",
            "Make stale execution fencing robust",
            vec![
                swf_domain::build_exploration::BuildAxis {
                    name: "strategy".into(),
                    options: vec!["repair".into(), "rethink".into(), "scratch".into()],
                },
                swf_domain::build_exploration::BuildAxis {
                    name: "review_lens".into(),
                    options: vec!["correctness".into(), "security".into()],
                },
            ],
        )
    }

    fn good_build_body() -> Value {
        json!({
            "model": JEV_MODEL,
            "answers": {
                "build_axis_0": {
                    "type": "choice",
                    "choice": "rethink",
                    "confidence": 0.71,
                    "probabilities": {
                        "repair": 0.20,
                        "rethink": 0.60,
                        "scratch": 0.20
                    }
                },
                "build_axis_1": {
                    "type": "choice",
                    "choice": "security",
                    "confidence": 0.76,
                    "probabilities": {
                        "correctness": 0.35,
                        "security": 0.65
                    }
                }
            },
            "usage": {"input_tokens": 180, "output_tokens": 20}
        })
    }

    #[tokio::test]
    async fn returns_a_typed_build_distribution_without_execution_authority() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(header("authorization", format!("Bearer {TOKEN}")))
            .and(body_partial_json(json!({
                "model": JEV_MODEL,
                "state": {"authority": "exploration-only"}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(good_build_body()))
            .expect(1)
            .mount(&server)
            .await;

        let result = test_client(&server)
            .build_distribution_or_uniform(&build_request(), &CancellationToken::new())
            .await
            .unwrap();

        assert!(result.provider_available);
        assert!(result.diagnostic.is_none());
        assert_eq!(result.distribution.model, JEV_MODEL);
        assert_eq!(result.distribution.axes.len(), 2);
        assert_eq!(result.distribution.axes[0].probabilities["rethink"], 0.60);
    }

    #[tokio::test]
    async fn undeclared_build_option_is_refused() {
        let server = MockServer::start().await;
        let mut body = good_build_body();
        body["answers"]["build_axis_0"]["probabilities"]["merge_without_tests"] = json!(0.1);
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .build_distribution(&build_request(), &CancellationToken::new())
            .await
            .unwrap_err();

        assert!(matches!(error, AdapterError::Decode { .. }));
    }

    #[tokio::test]
    async fn provider_failure_degrades_to_local_uniform_exploration() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string(TOKEN))
            .mount(&server)
            .await;

        let result = test_client(&server)
            .build_distribution_or_uniform(&build_request(), &CancellationToken::new())
            .await
            .unwrap();

        assert!(!result.provider_available);
        assert_eq!(result.distribution.model, "local-uniform-fallback");
        assert_eq!(result.distribution.axes[0].probabilities["repair"], 1.0);
        assert!(!result.diagnostic.unwrap().contains(TOKEN));
    }

    #[tokio::test]
    async fn build_model_substitution_is_refused() {
        let server = MockServer::start().await;
        let mut body = good_build_body();
        body["model"] = json!("jev-latest");
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .build_distribution(&build_request(), &CancellationToken::new())
            .await
            .unwrap_err();

        assert!(matches!(error, AdapterError::Decode { .. }));
    }

    #[tokio::test]
    async fn malformed_response_is_unavailable_as_decode() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
            .mount(&server)
            .await;

        let error = test_client(&server)
            .relate(&request(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, AdapterError::Decode { .. }));
    }
}
