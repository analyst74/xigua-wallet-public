use anyhow::Context;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub fn now_rfc3339() -> anyhow::Result<String> {
    format_rfc3339(OffsetDateTime::now_utc())
}

pub fn format_rfc3339(ts: OffsetDateTime) -> anyhow::Result<String> {
    ts.format(&Rfc3339)
        .context("failed to format timestamp as RFC3339")
}

pub fn parse_rfc3339(input: &str) -> anyhow::Result<OffsetDateTime> {
    OffsetDateTime::parse(input, &Rfc3339)
        .with_context(|| format!("failed to parse RFC3339 timestamp: {input}"))
}

#[cfg(test)]
mod tests {
    use super::{format_rfc3339, now_rfc3339, parse_rfc3339};

    #[test]
    fn now_is_parseable() {
        let now = now_rfc3339().unwrap();
        let parsed = parse_rfc3339(&now).unwrap();
        let reformatted = format_rfc3339(parsed).unwrap();
        assert_eq!(reformatted, now);
    }

    #[test]
    fn parse_invalid_value_fails() {
        let result = parse_rfc3339("2026-99-99T00:00:00Z");
        assert!(result.is_err());
    }
}
