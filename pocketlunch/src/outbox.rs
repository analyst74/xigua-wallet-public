#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxStatus {
    Failed,
    DeadLetter,
}

pub fn next_failure_status(
    attempt_after_increment: i64,
    retryable: bool,
    max_attempts: i64,
) -> OutboxStatus {
    if !retryable || attempt_after_increment >= max_attempts {
        OutboxStatus::DeadLetter
    } else {
        OutboxStatus::Failed
    }
}

pub fn classify_retryable_http(status: Option<u16>) -> bool {
    match status {
        Some(code) => code >= 500 || code == 429,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{next_failure_status, OutboxStatus};

    #[test]
    fn retryable_before_max_is_failed() {
        let status = next_failure_status(3, true, 8);
        assert_eq!(status, OutboxStatus::Failed);
    }

    #[test]
    fn retryable_at_max_is_dead_letter() {
        let status = next_failure_status(8, true, 8);
        assert_eq!(status, OutboxStatus::DeadLetter);
    }

    #[test]
    fn terminal_failure_is_dead_letter() {
        let status = next_failure_status(1, false, 8);
        assert_eq!(status, OutboxStatus::DeadLetter);
    }
}
