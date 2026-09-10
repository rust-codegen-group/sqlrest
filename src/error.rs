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
}

impl SqlrestError {
    pub fn new(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
            parameter: None,
        }
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
