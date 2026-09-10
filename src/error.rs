use serde::Serialize;

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct SqlrestError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,
    #[serde(skip)]
    pub status: u16,
    #[serde(skip)]
    diagnostic: Option<std::sync::Arc<str>>,
}

impl SqlrestError {
    pub fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            parameter: None,
            diagnostic: None,
        }
    }

    /// Private driver details for trusted runtime diagnostics only.
    /// Never include this value (or this error's Debug output) in a data response.
    pub fn diagnostic(&self) -> Option<&str> {
        self.diagnostic.as_deref()
    }

    pub(crate) fn with_diagnostic(mut self, backend: &str, detail: impl std::fmt::Debug) -> Self {
        use std::io::Write;
        let diagnostic = format!("{backend}: {detail:?}");
        // Record at the conversion boundary: cancellation/rollback may replace
        // the returned error later. JSON escaping prevents multiline log injection.
        // Logging failure must never panic or change transaction outcomes.
        let record = serde_json::json!({
            "event": "sqlrest.database_error",
            "code": self.code,
            "diagnostic": diagnostic,
        });
        let _ = writeln!(std::io::stderr().lock(), "{record}");
        self.diagnostic = Some(diagnostic.into());
        self
    }

    pub fn definition(message: impl Into<String>) -> Self {
        Self::new(400, "invalid_definition", message)
    }

    pub fn parameter(code: &str, path: &str, message: impl Into<String>) -> Self {
        let mut error = Self::new(400, code, message);
        error.parameter = Some(path.into());
        error
    }

    pub fn contract(message: impl Into<String>) -> Self {
        Self::new(500, "response_contract_mismatch", message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_diagnostics_never_enter_public_serialization() {
        let error = SqlrestError::new(500, "database_error", "Database operation failed")
            .with_diagnostic("postgres", "private_secret\nsecond line");
        assert!(error.diagnostic().unwrap().contains("private_secret"));
        assert!(
            error
                .clone()
                .diagnostic()
                .unwrap()
                .contains("private_secret")
        );
        assert!(!error.to_string().contains("private_secret"));
        let public = serde_json::to_string(&error).unwrap();
        assert!(!public.contains("private_secret"));
        assert!(!public.contains("diagnostic"));
        assert!(format!("{error:?}").contains("private_secret"));
    }
}
