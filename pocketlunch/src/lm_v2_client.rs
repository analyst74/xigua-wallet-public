use reqwest::StatusCode;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
pub enum LmClientError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("http status {status}: {body}")]
    Http {
        status: u16,
        body: String,
        retry_after_seconds: Option<u64>,
    },
    #[error("invalid response format: {0}")]
    InvalidFormat(String),
}

impl LmClientError {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            LmClientError::Http { status, .. } => Some(*status),
            _ => None,
        }
    }

    pub fn retry_after_seconds(&self) -> Option<u64> {
        match self {
            LmClientError::Http {
                retry_after_seconds,
                ..
            } => *retry_after_seconds,
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct LmV2Client {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl LmV2Client {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
        }
    }

    pub async fn list_categories(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/categories", self.base_url);
        let payload = self
            .request(self.client.get(url).query(&[("format", "flattened")]))
            .await?;
        extract_array(payload, &["categories", "data"])
    }

    pub async fn get_me_optional(&self) -> Result<Option<Value>, LmClientError> {
        let url = format!("{}/v2/me", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => Ok(Some(payload)),
            Err(LmClientError::Http { status: 404, .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn list_assets(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/assets", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => extract_array(payload, &["assets", "data"]),
            Err(LmClientError::Http { status: 404, .. }) => {
                let fallback_url = format!("{}/v2/plaid_accounts", self.base_url);
                let payload = self.request(self.client.get(fallback_url)).await?;
                extract_array(payload, &["assets", "plaid_accounts", "data"])
            }
            Err(err) => Err(err),
        }
    }

    pub async fn list_plaid_accounts(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/plaid_accounts", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => extract_array(payload, &["plaid_accounts", "assets", "data"]),
            Err(LmClientError::Http { status: 404, .. }) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    pub async fn list_manual_accounts(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/manual_accounts", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => extract_array(payload, &["manual_accounts", "assets", "data"]),
            Err(LmClientError::Http { status: 404, .. }) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    pub async fn list_tags(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/tags", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => extract_array(payload, &["tags", "data"]),
            Err(LmClientError::Http { status: 404, .. }) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    pub async fn list_recurring_items(&self) -> Result<Vec<Value>, LmClientError> {
        let url = format!("{}/v2/recurring_items", self.base_url);
        match self.request(self.client.get(url)).await {
            Ok(payload) => extract_array(payload, &["recurring_items", "data"]),
            Err(LmClientError::Http { status: 404, .. }) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    pub async fn get_summary_optional(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Option<Value>, LmClientError> {
        let url = format!("{}/v2/summary", self.base_url);
        match self
            .request(
                self.client
                    .get(url)
                    .query(&[("start_date", start_date), ("end_date", end_date)]),
            )
            .await
        {
            Ok(payload) => Ok(Some(payload)),
            Err(LmClientError::Http { status: 404, .. }) => Ok(None),
            Err(LmClientError::Http { status: 400, .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn list_transactions(
        &self,
        updated_since: Option<&str>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Value>, LmClientError> {
        let mut query = vec![
            ("offset".to_string(), offset.to_string()),
            ("limit".to_string(), limit.to_string()),
        ];
        if let Some(value) = updated_since {
            query.push(("updated_since".to_string(), value.to_string()));
        }

        let url = format!("{}/v2/transactions", self.base_url);
        let payload = self.request(self.client.get(url).query(&query)).await?;
        extract_array(payload, &["transactions", "data"])
    }

    pub async fn update_transaction_category(
        &self,
        transaction_id: i64,
        category_id: i64,
    ) -> Result<Value, LmClientError> {
        let url = format!("{}/v2/transactions/{transaction_id}", self.base_url);
        let payload = self
            .request(
                self.client
                    .put(url)
                    .json(&json!({ "category_id": category_id })),
            )
            .await?;

        if payload.get("id").is_some() {
            return Ok(payload);
        }

        if let Some(transaction) = payload.get("transaction") {
            return Ok(transaction.clone());
        }

        if let Some(data) = payload.get("data") {
            if data.get("id").is_some() {
                return Ok(data.clone());
            }
        }

        Err(LmClientError::InvalidFormat(
            "update transaction response missing transaction payload".to_string(),
        ))
    }

    pub async fn update_transactions_categories_batch(
        &self,
        updates: &[(i64, i64)],
    ) -> Result<Vec<Value>, LmClientError> {
        let transactions = updates
            .iter()
            .map(|(id, category_id)| json!({ "id": id, "category_id": category_id }))
            .collect::<Vec<_>>();
        let url = format!("{}/v2/transactions", self.base_url);
        let payload = self
            .request(
                self.client
                    .put(url)
                    .json(&json!({ "transactions": transactions })),
            )
            .await?;
        extract_transaction_array(payload)
    }

    pub async fn create_category(&self, payload: &Value) -> Result<Value, LmClientError> {
        let url = format!("{}/v2/categories", self.base_url);
        let payload = self.request(self.client.post(url).json(payload)).await?;
        extract_category_payload(payload)
    }

    pub async fn update_category(
        &self,
        category_id: i64,
        payload: &Value,
    ) -> Result<Value, LmClientError> {
        let url = format!("{}/v2/categories/{category_id}", self.base_url);
        let payload = self.request(self.client.put(url).json(payload)).await?;
        extract_category_payload(payload)
    }

    pub async fn delete_category(
        &self,
        category_id: i64,
        force: bool,
    ) -> Result<Option<Value>, LmClientError> {
        let url = format!("{}/v2/categories/{category_id}", self.base_url);
        let payload = self
            .request(self.client.delete(url).query(&[("force", force)]))
            .await?;
        normalize_delete_category_payload(payload)
    }

    async fn request(&self, request: reqwest::RequestBuilder) -> Result<Value, LmClientError> {
        let response = request
            .bearer_auth(&self.token)
            .header("Accept", "application/json")
            .send()
            .await?;

        let status = response.status();
        let retry_after_seconds = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(parse_retry_after_seconds);
        let body = response.text().await?;

        if status == StatusCode::NO_CONTENT {
            return Ok(json!({}));
        }

        if !status.is_success() {
            return Err(LmClientError::Http {
                status: status.as_u16(),
                body,
                retry_after_seconds,
            });
        }

        serde_json::from_str(&body).map_err(|err| {
            LmClientError::InvalidFormat(format!("json parse failed: {err}; body={body}"))
        })
    }
}

fn extract_array(payload: Value, object_keys: &[&str]) -> Result<Vec<Value>, LmClientError> {
    match payload {
        Value::Array(items) => Ok(items),
        Value::Object(obj) => {
            for key in object_keys {
                if let Some(Value::Array(items)) = obj.get(*key) {
                    return Ok(items.clone());
                }
            }
            Err(LmClientError::InvalidFormat(
                "expected array or object containing array".to_string(),
            ))
        }
        _ => Err(LmClientError::InvalidFormat(
            "expected array response".to_string(),
        )),
    }
}

fn extract_category_payload(payload: Value) -> Result<Value, LmClientError> {
    if payload.get("id").is_some() {
        return Ok(payload);
    }

    if let Some(category) = payload.get("category") {
        if category.get("id").is_some() {
            return Ok(category.clone());
        }
    }

    if let Some(data) = payload.get("data") {
        if data.get("id").is_some() {
            return Ok(data.clone());
        }
    }

    Err(LmClientError::InvalidFormat(
        "category response missing category payload".to_string(),
    ))
}

fn normalize_delete_category_payload(payload: Value) -> Result<Option<Value>, LmClientError> {
    if let Ok(category) = extract_category_payload(payload.clone()) {
        return Ok(Some(category));
    }

    if payload
        .as_object()
        .is_some_and(|object| object.is_empty() || object.contains_key("deleted"))
    {
        return Ok(None);
    }

    Err(LmClientError::InvalidFormat(
        "delete category response missing category payload".to_string(),
    ))
}

fn extract_transaction_array(payload: Value) -> Result<Vec<Value>, LmClientError> {
    match payload {
        Value::Array(items) => Ok(items),
        Value::Object(obj) => {
            if let Some(Value::Array(items)) = obj.get("transactions") {
                return Ok(items.clone());
            }
            if let Some(Value::Array(items)) = obj.get("data") {
                return Ok(items.clone());
            }
            if let Some(transaction) = obj.get("transaction") {
                if transaction.get("id").is_some() {
                    return Ok(vec![transaction.clone()]);
                }
            }
            if let Some(data) = obj.get("data") {
                if data.get("id").is_some() {
                    return Ok(vec![data.clone()]);
                }
            }
            if obj.get("id").is_some() {
                return Ok(vec![Value::Object(obj)]);
            }
            Err(LmClientError::InvalidFormat(
                "batch update transaction response missing transactions payload".to_string(),
            ))
        }
        _ => Err(LmClientError::InvalidFormat(
            "expected batch update transaction response".to_string(),
        )),
    }
}

fn parse_retry_after_seconds(value: &reqwest::header::HeaderValue) -> Option<u64> {
    let raw = value.to_str().ok()?.trim();
    if raw.is_empty() {
        return None;
    }
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(seconds);
    }

    let retry_at = OffsetDateTime::parse(raw, &Rfc2822).ok()?;
    let delay = (retry_at - OffsetDateTime::now_utc()).whole_seconds();
    Some(delay.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::{LmClientError, LmV2Client};
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn list_categories_supports_object_wrapped_array() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/categories"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "categories": [{"id": 1, "name": "Food"}]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let categories = client.list_categories().await.unwrap();
        assert_eq!(categories.len(), 1);
        assert_eq!(categories[0]["id"], 1);
    }

    #[tokio::test]
    async fn list_assets_supports_data_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/assets"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": 10, "name": "Checking"}]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let assets = client.list_assets().await.unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0]["id"], 10);
    }

    #[tokio::test]
    async fn list_assets_falls_back_to_plaid_accounts_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/assets"))
            .respond_with(ResponseTemplate::new(404).set_body_string("missing"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v2/plaid_accounts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "plaid_accounts": [{"id": 11, "name": "Fallback"}]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let assets = client.list_assets().await.unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0]["id"], 11);
    }

    #[tokio::test]
    async fn list_transactions_passes_expected_query_params() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/transactions"))
            .and(query_param("offset", "20"))
            .and(query_param("limit", "50"))
            .and(query_param("updated_since", "2026-01-01T00:00:00Z"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transactions": [{"id": 99}]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let txns = client
            .list_transactions(Some("2026-01-01T00:00:00Z"), 20, 50)
            .await
            .unwrap();
        assert_eq!(txns[0]["id"], 99);
    }

    #[tokio::test]
    async fn update_transaction_accepts_transaction_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/transactions/7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transaction": {"id": 7, "category_id": 3}
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let updated = client.update_transaction_category(7, 3).await.unwrap();
        assert_eq!(updated["id"], 7);
        assert_eq!(updated["category_id"], 3);
    }

    #[tokio::test]
    async fn update_transaction_accepts_data_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/transactions/8"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {"id": 8, "category_id": 4}
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let updated = client.update_transaction_category(8, 4).await.unwrap();
        assert_eq!(updated["id"], 8);
        assert_eq!(updated["category_id"], 4);
    }

    #[tokio::test]
    async fn batch_update_transactions_posts_transactions_payload_and_parses_response() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/transactions"))
            .and(body_json(json!({
                "transactions": [
                    { "id": 7, "category_id": 3 },
                    { "id": 8, "category_id": 4 }
                ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transactions": [
                    { "id": 7, "category_id": 3 },
                    { "id": 8, "category_id": 4 }
                ]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let updated = client
            .update_transactions_categories_batch(&[(7, 3), (8, 4)])
            .await
            .unwrap();
        assert_eq!(updated.len(), 2);
        assert_eq!(updated[0]["id"], 7);
        assert_eq!(updated[1]["id"], 8);
    }

    #[tokio::test]
    async fn batch_update_transactions_accepts_data_array_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/transactions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 11, "category_id": 5 }
                ]
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let updated = client
            .update_transactions_categories_batch(&[(11, 5)])
            .await
            .unwrap();
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0]["id"], 11);
        assert_eq!(updated[0]["category_id"], 5);
    }

    #[tokio::test]
    async fn create_category_posts_payload_and_accepts_direct_object() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v2/categories"))
            .and(body_json(json!({
                "name": "Income",
                "is_income": true
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": 123,
                "name": "Income",
                "is_income": true
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let created = client
            .create_category(&json!({ "name": "Income", "is_income": true }))
            .await
            .unwrap();
        assert_eq!(created["id"], 123);
        assert_eq!(created["name"], "Income");
    }

    #[tokio::test]
    async fn update_category_accepts_category_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v2/categories/55"))
            .and(body_json(json!({
                "name": "Dining"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "category": {
                    "id": 55,
                    "name": "Dining"
                }
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let updated = client
            .update_category(55, &json!({ "name": "Dining" }))
            .await
            .unwrap();
        assert_eq!(updated["id"], 55);
        assert_eq!(updated["name"], "Dining");
    }

    #[tokio::test]
    async fn delete_category_accepts_deleted_payload() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v2/categories/9"))
            .and(query_param("force", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "deleted": true
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let deleted = client.delete_category(9, false).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn delete_category_accepts_category_wrapper() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v2/categories/10"))
            .and(query_param("force", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "category": {
                    "id": 10,
                    "name": "Test",
                    "archived": true
                }
            })))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let deleted = client.delete_category(10, true).await.unwrap();
        assert_eq!(deleted.unwrap()["id"], 10);
    }

    #[tokio::test]
    async fn http_errors_include_status_code() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/assets"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate limit"))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let err = client.list_assets().await.unwrap_err();
        match err {
            LmClientError::Http {
                status,
                body,
                retry_after_seconds,
            } => {
                assert_eq!(status, 429);
                assert!(body.contains("rate limit"));
                assert_eq!(retry_after_seconds, None);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn http_errors_capture_retry_after_seconds() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/assets"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "7")
                    .set_body_string("rate limit"),
            )
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let err = client.list_assets().await.unwrap_err();
        match err {
            LmClientError::Http {
                status,
                retry_after_seconds,
                ..
            } => {
                assert_eq!(status, 429);
                assert_eq!(retry_after_seconds, Some(7));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_json_surfaces_format_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/categories"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
            .mount(&server)
            .await;

        let client = LmV2Client::new(server.uri(), "token");
        let err = client.list_categories().await.unwrap_err();
        match err {
            LmClientError::InvalidFormat(message) => {
                assert!(message.contains("json parse failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
