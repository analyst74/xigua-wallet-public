use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategorizeDecision {
    pub transaction_id: i64,
    pub category_id: i64,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DecisionFile {
    Direct(Vec<CategorizeDecisionRaw>),
    Wrapped {
        decisions: Vec<CategorizeDecisionRaw>,
    },
}

#[derive(Debug, Deserialize)]
struct CategorizeDecisionRaw {
    #[serde(alias = "transaction_id", alias = "transactionId", alias = "id")]
    transaction_id: Option<i64>,
    #[serde(
        alias = "category_id",
        alias = "categoryId",
        alias = "suggested_category_id"
    )]
    category_id: Option<i64>,
    reason: Option<String>,
}

impl DecisionFile {
    fn into_vec(self) -> Vec<CategorizeDecisionRaw> {
        match self {
            DecisionFile::Direct(v) => v,
            DecisionFile::Wrapped { decisions } => decisions,
        }
    }
}

pub fn parse_decisions(raw: &str) -> anyhow::Result<Vec<CategorizeDecision>> {
    let parsed: DecisionFile = serde_json::from_str(raw)?;
    let mut decisions = Vec::new();

    for (idx, row) in parsed.into_vec().into_iter().enumerate() {
        let transaction_id = row
            .transaction_id
            .ok_or_else(|| anyhow::anyhow!("missing transaction_id at decision index {idx}"))?;
        let category_id = row
            .category_id
            .ok_or_else(|| anyhow::anyhow!("missing category_id at decision index {idx}"))?;

        decisions.push(CategorizeDecision {
            transaction_id,
            category_id,
            reason: row.reason,
        });
    }

    Ok(decisions)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionUpdatePayload {
    pub category_id: i64,
    pub base_remote_updated_at: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryDeletePayload {
    pub base_remote_updated_at: Option<String>,
    pub force: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryUpsertPayload {
    pub name: String,
    pub description: Option<String>,
    pub is_income: Option<bool>,
    pub exclude_from_budget: Option<bool>,
    pub exclude_from_totals: Option<bool>,
    pub is_group: Option<bool>,
    pub group_id: Option<i64>,
    pub archived: Option<bool>,
    pub base_remote_updated_at: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub struct OutboxChange {
    pub change_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub op: String,
    pub payload_json: String,
    pub idempotency_key: String,
    pub status: String,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct EventRow {
    pub event_id: String,
    pub run_id: Option<String>,
    pub source: String,
    pub action: String,
    pub entity_type: String,
    pub entity_id: String,
    pub before_json: Option<String>,
    pub after_json: Option<String>,
    pub meta_json: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct SyncStateRow {
    pub entity: String,
    pub last_remote_updated_at: String,
    pub lookback_seconds: i64,
    pub last_success_run_id: Option<String>,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::parse_decisions;

    #[test]
    fn parse_direct_decisions_with_alias_fields() {
        let input = r#"
        [
          {"transactionId": 101, "categoryId": 5, "reason": "match"},
          {"id": 102, "suggested_category_id": 6}
        ]
        "#;

        let decisions = parse_decisions(input).unwrap();
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].transaction_id, 101);
        assert_eq!(decisions[0].category_id, 5);
        assert_eq!(decisions[0].reason.as_deref(), Some("match"));
        assert_eq!(decisions[1].transaction_id, 102);
        assert_eq!(decisions[1].category_id, 6);
        assert!(decisions[1].reason.is_none());
    }

    #[test]
    fn parse_wrapped_decisions() {
        let input = r#"
        {
          "decisions": [
            {"transaction_id": 201, "category_id": 9}
          ]
        }
        "#;

        let decisions = parse_decisions(input).unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].transaction_id, 201);
        assert_eq!(decisions[0].category_id, 9);
    }

    #[test]
    fn parse_decisions_errors_when_transaction_missing() {
        let input = r#"[{"category_id": 3}]"#;
        let err = parse_decisions(input).unwrap_err().to_string();
        assert!(err.contains("missing transaction_id"));
    }

    #[test]
    fn parse_decisions_errors_when_category_missing() {
        let input = r#"[{"transaction_id": 3}]"#;
        let err = parse_decisions(input).unwrap_err().to_string();
        assert!(err.contains("missing category_id"));
    }

    #[test]
    fn parse_decisions_errors_on_invalid_json() {
        let err = parse_decisions("not json").unwrap_err().to_string();
        assert!(err.contains("expected"));
    }
}
