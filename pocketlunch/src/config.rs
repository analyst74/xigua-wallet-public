#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub base_url: String,
    pub api_token: Option<String>,
    pub default_lookback_seconds: i64,
    pub pull_page_size: i64,
    pub outbox_batch_size: i64,
    pub transaction_update_batch_size: i64,
    pub max_push_attempts: i64,
}

impl AppConfig {
    pub fn new(
        database_url: String,
        base_url: String,
        api_token: Option<String>,
        default_lookback_seconds: i64,
        pull_page_size: i64,
        outbox_batch_size: i64,
        transaction_update_batch_size: i64,
        max_push_attempts: i64,
    ) -> anyhow::Result<Self> {
        if database_url.trim().is_empty() {
            anyhow::bail!("database_url cannot be empty");
        }
        if outbox_batch_size <= 0 {
            anyhow::bail!("outbox_batch_size must be positive");
        }
        if transaction_update_batch_size <= 0 {
            anyhow::bail!("transaction_update_batch_size must be positive");
        }
        if max_push_attempts <= 0 {
            anyhow::bail!("max_push_attempts must be positive");
        }

        Ok(Self {
            database_url,
            base_url,
            api_token,
            default_lookback_seconds,
            pull_page_size,
            outbox_batch_size,
            transaction_update_batch_size,
            max_push_attempts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    fn make_config(database_url: &str) -> AppConfig {
        AppConfig::new(
            database_url.to_string(),
            "https://api.lunchmoney.dev".to_string(),
            Some("token".to_string()),
            300,
            100,
            100,
            50,
            8,
        )
        .unwrap()
    }

    #[test]
    fn new_rejects_empty_database_url() {
        let result = AppConfig::new(
            "   ".to_string(),
            "https://api.lunchmoney.dev".to_string(),
            None,
            300,
            100,
            100,
            50,
            8,
        );
        assert!(result.is_err());
    }

    #[test]
    fn database_url_is_preserved() {
        let config = make_config("postgres://user:pass@localhost:5432/pocketlunch");
        assert_eq!(
            config.database_url,
            "postgres://user:pass@localhost:5432/pocketlunch"
        );
    }

    #[test]
    fn new_rejects_non_positive_transaction_update_batch_size() {
        let result = AppConfig::new(
            "postgres://user:pass@localhost:5432/pocketlunch".to_string(),
            "https://api.lunchmoney.dev".to_string(),
            Some("token".to_string()),
            300,
            100,
            100,
            0,
            8,
        );
        assert!(result.is_err());
    }
}
