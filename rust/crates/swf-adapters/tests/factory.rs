use std::time::Duration;

use serde_json::{json, Value};
use swf_adapters::error::AdapterError;
use swf_adapters::factory::FactoryApi;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn client(server: &MockServer) -> FactoryApi {
    FactoryApi::new(&server.uri(), TOKEN.to_string(), Duration::from_secs(5))
        .expect("factory client")
}

#[tokio::test]
async fn call_sends_bearer_json_and_decodes_api_v1() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/compatibility"))
        .and(header("authorization", format!("Bearer {TOKEN}")))
        .and(body_json(json!({"probe": true})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"api": 1, "ready": true})))
        .expect(1)
        .mount(&server)
        .await;

    let value: Value = client(&server)
        .call(
            "/compatibility",
            json!({"probe": true}),
            &CancellationToken::new(),
        )
        .await
        .expect("valid response");
    assert_eq!(value, json!({"api": 1, "ready": true}));
}

#[tokio::test]
async fn non_json_backend_response_fails_closed_as_decode() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/doctor"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
        .mount(&server)
        .await;

    let error = client(&server)
        .call::<Value>("/doctor", json!({}), &CancellationToken::new())
        .await
        .expect_err("non-json must be rejected");
    assert!(matches!(error, AdapterError::Decode { .. }));
}

#[tokio::test]
async fn backend_status_is_classified_before_schema_decode() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/doctor"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"detail": "bad token"})))
        .mount(&server)
        .await;

    let error = client(&server)
        .call::<Value>("/doctor", json!({}), &CancellationToken::new())
        .await
        .expect_err("401 must be auth");
    assert!(matches!(error, AdapterError::Auth { .. }));
}
