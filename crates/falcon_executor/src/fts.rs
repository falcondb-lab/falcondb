use falcon_common::datum::Datum;
use falcon_common::error::ExecutionError;
use std::collections::BTreeMap;

// ── Stop words (English) ──

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with",
];

fn is_stop_word(w: &str) -> bool {
    STOP_WORDS.binary_search(&w).is_ok()
}

// ── Tokenizer ──

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|w| !w.is_empty())
        .map(|w| w.to_lowercase())
        .filter(|w| !is_stop_word(w))
        .collect()
}

// ── to_tsvector ──

pub fn to_tsvector(text: &str) -> Vec<(String, Vec<u16>)> {
    let tokens = tokenize(text);
    let mut map: BTreeMap<String, Vec<u16>> = BTreeMap::new();
    for (i, tok) in tokens.iter().enumerate() {
        let pos = (i + 1).min(u16::MAX as usize) as u16;
        map.entry(tok.clone()).or_default().push(pos);
    }
    map.into_iter().collect()
}

// ── TsQuery AST ──

#[derive(Debug, Clone)]
pub enum TsQueryNode {
    Term(String),
    Prefix(String),
    Not(Box<TsQueryNode>),
    And(Box<TsQueryNode>, Box<TsQueryNode>),
    Or(Box<TsQueryNode>, Box<TsQueryNode>),
}

// ── to_tsquery parser ──
// Supports: word, word & word, word | word, !word, prefix:*

pub fn parse_tsquery(input: &str) -> Result<TsQueryNode, ExecutionError> {
    let tokens = lex_tsquery(input)?;
    if tokens.is_empty() {
        return Err(ExecutionError::TypeError("empty tsquery".into()));
    }
    let (node, rest) = parse_or(&tokens)?;
    if !rest.is_empty() {
        return Err(ExecutionError::TypeError(format!(
            "unexpected token in tsquery: {:?}",
            rest[0]
        )));
    }
    Ok(node)
}

pub fn plainto_tsquery(text: &str) -> Result<TsQueryNode, ExecutionError> {
    let words: Vec<String> = tokenize(text);
    if words.is_empty() {
        return Err(ExecutionError::TypeError("empty tsquery".into()));
    }
    let mut iter = words.into_iter();
    let first = iter.next().unwrap();
    let mut node = TsQueryNode::Term(first);
    for w in iter {
        node = TsQueryNode::And(Box::new(node), Box::new(TsQueryNode::Term(w)));
    }
    Ok(node)
}

pub fn phraseto_tsquery(text: &str) -> Result<TsQueryNode, ExecutionError> {
    // Same as plainto for now (phrase search requires adjacency which we model as AND)
    plainto_tsquery(text)
}

// ── tsquery lexer ──

#[derive(Debug, Clone)]
enum QToken {
    Word(String),
    PrefixWord(String),
    And,
    Or,
    Not,
    LParen,
    RParen,
}

fn lex_tsquery(input: &str) -> Result<Vec<QToken>, ExecutionError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '&' => {
                chars.next();
                tokens.push(QToken::And);
            }
            '|' => {
                chars.next();
                tokens.push(QToken::Or);
            }
            '!' => {
                chars.next();
                tokens.push(QToken::Not);
            }
            '(' => {
                chars.next();
                tokens.push(QToken::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(QToken::RParen);
            }
            '\'' => {
                chars.next();
                let mut word = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == '\'' {
                        chars.next();
                        break;
                    }
                    word.push(ch);
                    chars.next();
                }
                let w = word.to_lowercase();
                if chars.peek() == Some(&':') {
                    chars.next();
                    if chars.peek() == Some(&'*') {
                        chars.next();
                        tokens.push(QToken::PrefixWord(w));
                    } else {
                        // skip weight labels like :A, :B etc
                        while chars.peek().is_some_and(|c| c.is_alphanumeric()) {
                            chars.next();
                        }
                        tokens.push(QToken::Word(w));
                    }
                } else {
                    tokens.push(QToken::Word(w));
                }
            }
            _ if c.is_alphanumeric() || c == '_' => {
                let mut word = String::new();
                while chars
                    .peek()
                    .is_some_and(|c| c.is_alphanumeric() || *c == '_')
                {
                    word.push(chars.next().unwrap());
                }
                let w = word.to_lowercase();
                if chars.peek() == Some(&':') {
                    chars.next();
                    if chars.peek() == Some(&'*') {
                        chars.next();
                        tokens.push(QToken::PrefixWord(w));
                    } else {
                        while chars.peek().is_some_and(|c| c.is_alphanumeric()) {
                            chars.next();
                        }
                        tokens.push(QToken::Word(w));
                    }
                } else {
                    tokens.push(QToken::Word(w));
                }
            }
            _ => {
                chars.next();
            }
        }
    }
    Ok(tokens)
}

// ── Recursive descent parser: or > and > not > atom ──

fn parse_or(tokens: &[QToken]) -> Result<(TsQueryNode, &[QToken]), ExecutionError> {
    let (mut left, mut rest) = parse_and(tokens)?;
    while matches!(rest.first(), Some(QToken::Or)) {
        let (right, r) = parse_and(&rest[1..])?;
        left = TsQueryNode::Or(Box::new(left), Box::new(right));
        rest = r;
    }
    Ok((left, rest))
}

fn parse_and(tokens: &[QToken]) -> Result<(TsQueryNode, &[QToken]), ExecutionError> {
    let (mut left, mut rest) = parse_not(tokens)?;
    while matches!(rest.first(), Some(QToken::And)) {
        let (right, r) = parse_not(&rest[1..])?;
        left = TsQueryNode::And(Box::new(left), Box::new(right));
        rest = r;
    }
    Ok((left, rest))
}

fn parse_not(tokens: &[QToken]) -> Result<(TsQueryNode, &[QToken]), ExecutionError> {
    if matches!(tokens.first(), Some(QToken::Not)) {
        let (inner, rest) = parse_atom(&tokens[1..])?;
        Ok((TsQueryNode::Not(Box::new(inner)), rest))
    } else {
        parse_atom(tokens)
    }
}

fn parse_atom(tokens: &[QToken]) -> Result<(TsQueryNode, &[QToken]), ExecutionError> {
    match tokens.first() {
        Some(QToken::Word(w)) => Ok((TsQueryNode::Term(w.clone()), &tokens[1..])),
        Some(QToken::PrefixWord(w)) => Ok((TsQueryNode::Prefix(w.clone()), &tokens[1..])),
        Some(QToken::LParen) => {
            let (node, rest) = parse_or(&tokens[1..])?;
            match rest.first() {
                Some(QToken::RParen) => Ok((node, &rest[1..])),
                _ => Err(ExecutionError::TypeError("expected ')' in tsquery".into())),
            }
        }
        _ => Err(ExecutionError::TypeError(
            "unexpected end of tsquery".into(),
        )),
    }
}

// ── @@ matching ──

pub fn ts_match(vector: &[(String, Vec<u16>)], query: &TsQueryNode) -> bool {
    match query {
        TsQueryNode::Term(w) => vector.iter().any(|(lex, _)| lex == w),
        TsQueryNode::Prefix(p) => vector.iter().any(|(lex, _)| lex.starts_with(p.as_str())),
        TsQueryNode::Not(inner) => !ts_match(vector, inner),
        TsQueryNode::And(a, b) => ts_match(vector, a) && ts_match(vector, b),
        TsQueryNode::Or(a, b) => ts_match(vector, a) || ts_match(vector, b),
    }
}

pub fn ts_match_datums(vec_datum: &Datum, query_datum: &Datum) -> Result<Datum, ExecutionError> {
    match (vec_datum, query_datum) {
        (Datum::TsVector(v), Datum::TsQuery(q)) => {
            let query = parse_tsquery(q)?;
            Ok(Datum::Boolean(ts_match(v, &query)))
        }
        (Datum::TsQuery(q), Datum::TsVector(v)) => {
            let query = parse_tsquery(q)?;
            Ok(Datum::Boolean(ts_match(v, &query)))
        }
        _ => Err(ExecutionError::TypeError(
            "@@ requires tsvector and tsquery operands".into(),
        )),
    }
}

// ── ts_rank ──

pub fn ts_rank(vector: &[(String, Vec<u16>)], query: &TsQueryNode) -> f64 {
    let total_lexemes = vector.len().max(1) as f64;
    let matching = count_matching_terms(vector, query) as f64;
    matching / total_lexemes
}

fn count_matching_terms(vector: &[(String, Vec<u16>)], query: &TsQueryNode) -> usize {
    match query {
        TsQueryNode::Term(w) => vector.iter().filter(|(lex, _)| lex == w).count(),
        TsQueryNode::Prefix(p) => vector
            .iter()
            .filter(|(lex, _)| lex.starts_with(p.as_str()))
            .count(),
        TsQueryNode::Not(_) => 0,
        TsQueryNode::And(a, b) => count_matching_terms(vector, a) + count_matching_terms(vector, b),
        TsQueryNode::Or(a, b) => count_matching_terms(vector, a) + count_matching_terms(vector, b),
    }
}

// ── ts_rank_cd (cover density ranking) ──

pub fn ts_rank_cd(vector: &[(String, Vec<u16>)], query: &TsQueryNode) -> f64 {
    let matching = count_matching_terms(vector, query) as f64;
    let total_positions: usize = vector.iter().map(|(_, p)| p.len()).sum();
    if total_positions == 0 {
        return 0.0;
    }
    matching / (total_positions as f64)
}

// ── ts_headline ──

pub fn ts_headline(text: &str, query: &TsQueryNode) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    let query_terms = collect_query_terms(query);
    let mut result = Vec::new();
    for w in &words {
        let lower = w.to_lowercase();
        let base: String = lower.chars().filter(|c| c.is_alphanumeric()).collect();
        if query_terms
            .iter()
            .any(|t| base == *t || base.starts_with(t.as_str()))
        {
            result.push(format!("<b>{w}</b>"));
        } else {
            result.push(w.to_string());
        }
    }
    result.join(" ")
}

fn collect_query_terms(query: &TsQueryNode) -> Vec<String> {
    match query {
        TsQueryNode::Term(w) => vec![w.clone()],
        TsQueryNode::Prefix(p) => vec![p.clone()],
        TsQueryNode::Not(_) => vec![],
        TsQueryNode::And(a, b) | TsQueryNode::Or(a, b) => {
            let mut v = collect_query_terms(a);
            v.extend(collect_query_terms(b));
            v
        }
    }
}

// ── setweight ──

pub fn setweight(vector: &[(String, Vec<u16>)], _weight: char) -> Vec<(String, Vec<u16>)> {
    // Weight tracking not implemented yet; return as-is
    vector.to_vec()
}

// ── strip (remove positions) ──

pub fn strip(vector: &[(String, Vec<u16>)]) -> Vec<(String, Vec<u16>)> {
    vector.iter().map(|(w, _)| (w.clone(), vec![])).collect()
}

// ── tsvector_length ──

pub fn tsvector_length(vector: &[(String, Vec<u16>)]) -> i32 {
    vector.len() as i32
}

// ── numnode (count nodes in tsquery) ──

pub fn numnode(query: &TsQueryNode) -> i32 {
    match query {
        TsQueryNode::Term(_) | TsQueryNode::Prefix(_) => 1,
        TsQueryNode::Not(inner) => 1 + numnode(inner),
        TsQueryNode::And(a, b) | TsQueryNode::Or(a, b) => 1 + numnode(a) + numnode(b),
    }
}

// ── querytree (display tsquery as text) ──

pub fn querytree(query: &TsQueryNode) -> String {
    match query {
        TsQueryNode::Term(w) => format!("'{w}'"),
        TsQueryNode::Prefix(p) => format!("'{p}':*"),
        TsQueryNode::Not(inner) => format!("!( {} )", querytree(inner)),
        TsQueryNode::And(a, b) => format!("{} & {}", querytree(a), querytree(b)),
        TsQueryNode::Or(a, b) => format!("{} | {}", querytree(a), querytree(b)),
    }
}

// ── tsvector concatenation (||) ──

pub fn tsvector_concat(
    a: &[(String, Vec<u16>)],
    b: &[(String, Vec<u16>)],
) -> Vec<(String, Vec<u16>)> {
    let offset = a
        .iter()
        .flat_map(|(_, p)| p.iter().copied())
        .max()
        .unwrap_or(0);
    let mut map: BTreeMap<String, Vec<u16>> = BTreeMap::new();
    for (w, pos) in a {
        map.entry(w.clone()).or_default().extend(pos);
    }
    for (w, pos) in b {
        let entry = map.entry(w.clone()).or_default();
        for p in pos {
            entry.push(p + offset);
        }
    }
    map.into_iter().collect()
}

// ── array_to_tsvector ──

pub fn array_to_tsvector(words: &[Datum]) -> Result<Vec<(String, Vec<u16>)>, ExecutionError> {
    let mut map: BTreeMap<String, Vec<u16>> = BTreeMap::new();
    for (i, d) in words.iter().enumerate() {
        match d {
            Datum::Text(s) => {
                let w = s.to_lowercase();
                map.entry(w)
                    .or_default()
                    .push((i + 1).min(u16::MAX as usize) as u16);
            }
            Datum::Null => {}
            _ => {
                return Err(ExecutionError::TypeError(
                    "array_to_tsvector requires text array".into(),
                ))
            }
        }
    }
    Ok(map.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_tsvector() {
        let v = to_tsvector("The quick brown fox jumps over the lazy dog");
        let words: Vec<&str> = v.iter().map(|(w, _)| w.as_str()).collect();
        assert!(words.contains(&"quick"));
        assert!(words.contains(&"fox"));
        assert!(!words.contains(&"the")); // stop word
    }

    #[test]
    fn test_tsquery_simple() {
        let q = parse_tsquery("fox & dog").unwrap();
        let v = to_tsvector("The quick brown fox jumps over the lazy dog");
        assert!(ts_match(&v, &q));
    }

    #[test]
    fn test_tsquery_or() {
        let q = parse_tsquery("cat | fox").unwrap();
        let v = to_tsvector("The fox is fast");
        assert!(ts_match(&v, &q));
    }

    #[test]
    fn test_tsquery_not() {
        let q = parse_tsquery("fox & !cat").unwrap();
        let v = to_tsvector("The fox is fast");
        assert!(ts_match(&v, &q));
    }

    #[test]
    fn test_tsquery_prefix() {
        let q = parse_tsquery("quic:*").unwrap();
        let v = to_tsvector("The quick brown fox");
        assert!(ts_match(&v, &q));
    }

    #[test]
    fn test_plainto_tsquery() {
        let q = plainto_tsquery("fat cat").unwrap();
        let v = to_tsvector("a fat cat sat on the mat");
        assert!(ts_match(&v, &q));
    }

    #[test]
    fn test_ts_rank() {
        let q = parse_tsquery("fox").unwrap();
        let v = to_tsvector("The quick brown fox");
        let rank = ts_rank(&v, &q);
        assert!(rank > 0.0);
    }

    #[test]
    fn test_ts_headline() {
        let q = parse_tsquery("fox").unwrap();
        let hl = ts_headline("The quick brown fox jumped", &q);
        assert!(hl.contains("<b>fox</b>"));
    }

    #[test]
    fn test_strip() {
        let v = to_tsvector("hello world");
        let s = strip(&v);
        assert!(s.iter().all(|(_, p)| p.is_empty()));
    }

    #[test]
    fn test_tsvector_length() {
        let v = to_tsvector("hello world hello");
        assert_eq!(tsvector_length(&v), 2); // "hello" and "world"
    }

    #[test]
    fn test_numnode() {
        let q = parse_tsquery("fox & (cat | dog)").unwrap();
        assert!(numnode(&q) >= 3);
    }
}
