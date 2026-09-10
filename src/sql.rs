use crate::{
    SqlrestError,
    params::{Parameter, validate_parameters},
};
use sqlparser::{
    dialect::{Dialect, PostgreSqlDialect, SQLiteDialect},
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Turso,
    Postgres,
}

impl Backend {
    pub fn dialect(self) -> Box<dyn Dialect> {
        match self {
            Self::Turso => Box::new(SQLiteDialect {}),
            Self::Postgres => Box::new(PostgreSqlDialect {}),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Statement {
    pub sql: String,
    pub parameters: Vec<Parameter>,
}

pub fn compile(source: &str, backend: Backend) -> Result<Vec<Statement>, SqlrestError> {
    // Replace references before tokenization. Quotes and comments remain opaque.
    let bytes = source.as_bytes();
    let mut i = 0;
    let mut rewritten = String::new();
    let mut parameters = Vec::new();
    let mut original = String::new();
    let mut originals = Vec::new();
    let mut local_parameters = 0;
    while i < bytes.len() {
        let start = i;
        if bytes[i..].starts_with(b"--") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth != 0 {
                return Err(SqlrestError::definition("unterminated comment"));
            }
        } else if matches!(bytes[i], b'\'' | b'"' | b'`')
            || (backend == Backend::Turso && bytes[i] == b'[')
        {
            let quote = if bytes[i] == b'[' { b']' } else { bytes[i] };
            let escaped = backend == Backend::Postgres
                && quote == b'\''
                && i > 0
                && matches!(bytes[i - 1], b'e' | b'E');
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                if escaped && bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                    continue;
                }
                if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote {
                        i += 1;
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            if !closed {
                return Err(SqlrestError::definition("unterminated quoted SQL"));
            }
        } else if bytes[i..].starts_with(b"${") {
            let end = source[i + 2..]
                .find('}')
                .map(|n| n + i + 2)
                .ok_or_else(|| SqlrestError::definition("unterminated parameter"))?;
            parameters.push(Parameter::parse(&source[i + 2..end])?);
            rewritten.push_str(&format!("$sqlrest_internal_{}", parameters.len()));
            local_parameters += 1;
            original.push_str(&format!("${local_parameters}"));
            i = end + 1;
            continue;
        } else if bytes[i] == b'$' && backend == Backend::Postgres {
            let mut end = i + 1;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            if end < bytes.len()
                && bytes[end] == b'$'
                && (end == i + 1 || !bytes[i + 1].is_ascii_digit())
            {
                let delimiter = &source[i..=end];
                let close = source[end + 1..]
                    .find(delimiter)
                    .ok_or_else(|| SqlrestError::definition("unterminated dollar quote"))?;
                i = end + 1 + close + delimiter.len();
            } else {
                return Err(SqlrestError::definition("native or invalid SQL parameter"));
            }
        } else if bytes[i] == b';' {
            originals.push(std::mem::take(&mut original));
            local_parameters = 0;
            rewritten.push(';');
            i += 1;
            continue;
        } else if backend == Backend::Postgres && bytes[i] == b'?' {
            original.push('?');
            rewritten.push('?');
            // Work around upstream bare-? lookahead consumption, without
            // changing the SQL submitted to the database.
            if !matches!(bytes.get(i + 1), Some(b'|' | b'&' | b'-' | b'#')) {
                rewritten.push(' ');
            }
            i += 1;
            continue;
        } else {
            if bytes[i] == b'$' {
                return Err(SqlrestError::definition("native SQL parameter"));
            }
            let ch = source[i..].chars().next().unwrap();
            i += ch.len_utf8();
        }
        rewritten.push_str(&source[start..i]);
        original.push_str(&source[start..i]);
    }
    originals.push(original);
    validate_parameters(&parameters)?;
    let dialect = backend.dialect();
    let tokens = Tokenizer::new(dialect.as_ref(), &rewritten)
        .tokenize()
        .map_err(|e| SqlrestError::definition(e.to_string()))?;
    let mut output = Vec::new();
    let mut text = String::new();
    let mut bound = Vec::new();
    let mut parsed_tokens = Vec::new();
    let mut originals = originals.into_iter();
    for token in tokens.into_iter().chain(std::iter::once(Token::SemiColon)) {
        if token == Token::SemiColon {
            let original = originals
                .next()
                .ok_or_else(|| SqlrestError::definition("SQL statement boundary mismatch"))?;
            if !text.trim().is_empty() {
                let ast = Parser::new(dialect.as_ref())
                    .with_tokens(std::mem::take(&mut parsed_tokens))
                    .parse_statements()
                    .map_err(|e| SqlrestError::definition(e.to_string()))?;
                if !ast.is_empty() {
                    // Endpoints are data operations, not connection/transaction administration.
                    if ast.iter().any(|s| {
                        !matches!(
                            s,
                            sqlparser::ast::Statement::Query(_)
                                | sqlparser::ast::Statement::Insert(_)
                                | sqlparser::ast::Statement::Update { .. }
                                | sqlparser::ast::Statement::Delete(_)
                        )
                    }) {
                        return Err(SqlrestError::definition(
                            "endpoint SQL must be SELECT, INSERT, UPDATE or DELETE; use migrations for DDL",
                        ));
                    }
                    output.push(Statement {
                        sql: original,
                        parameters: std::mem::take(&mut bound),
                    });
                }
            }
            text.clear();
            parsed_tokens.clear();
            bound.clear();
            continue;
        }
        parsed_tokens.push(token.clone());
        if let Token::Placeholder(ref p) = token {
            if backend == Backend::Postgres && p == "?" {
                text.push('?');
                continue;
            }
            if let Some(n) = p
                .strip_prefix("$sqlrest_internal_")
                .and_then(|s| s.parse::<usize>().ok())
            {
                let param = parameters
                    .get(n.wrapping_sub(1))
                    .ok_or_else(|| SqlrestError::definition("invalid internal parameter"))?;
                bound.push(param.clone());
                text.push_str(&format!("${}", bound.len()));
                continue;
            }
            return Err(SqlrestError::definition(
                "native SQL parameters cannot be mixed with typed references",
            ));
        }
        if backend == Backend::Turso && matches!(token, Token::Colon | Token::AtSign) {
            return Err(SqlrestError::definition(
                "native SQL parameters cannot be mixed with typed references",
            ));
        }
        text.push_str(&token.to_string());
        if token == Token::Question {
            // sqlparser's PG tokenizer consumes the following character for a
            // bare question operator. Preserve a separator for its next pass.
            text.push(' ');
        }
    }
    if output.is_empty() {
        return Err(SqlrestError::definition("empty endpoint"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_and_parameters() {
        let sql = "SELECT '${body.x:string};', $$${body.x:string};$$, ${body.x:string}; SELECT ${query.n:int64}";
        let statements = compile(sql, Backend::Postgres).unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].parameters.len(), 1);
        assert!(statements[0].sql.contains("$$${body.x:string};$$"));
    }

    #[test]
    fn reject_native_and_transactions() {
        for sql in [
            "SELECT $1",
            "BEGIN",
            "COMMIT",
            "PRAGMA query_only=off",
            "SELECT ?",
            "SELECT :name",
        ] {
            assert!(compile(sql, Backend::Turso).is_err(), "{sql}");
        }
    }

    #[test]
    fn postgres_json_operator() {
        compile("SELECT '{}'::jsonb ? 'key'", Backend::Postgres).unwrap();
        let source = "SELECT '{}'::jsonb?'key', 'it''s unchanged'";
        assert_eq!(compile(source, Backend::Postgres).unwrap()[0].sql, source);
    }
}
