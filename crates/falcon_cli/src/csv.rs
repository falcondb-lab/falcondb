/// CSV options shared by import and export.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: u8,
    pub null_as: String,
    pub header: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: b'"',
            null_as: String::new(),
            header: true,
        }
    }
}

/// Parse a delimiter token like `','` or `'\t'` into a byte.
pub fn parse_delimiter(s: &str) -> Option<u8> {
    let s = s.trim().trim_matches('\'');
    match s {
        "," => Some(b','),
        "\\t" | "\t" => Some(b'\t'),
        "|" => Some(b'|'),
        ";" => Some(b';'),
        _ if s.len() == 1 => Some(s.as_bytes()[0]),
        _ => None,
    }
}

/// Quote a single CSV field value according to the given options.
/// Always quotes if the value contains the delimiter, quote char, newline, or carriage return.
pub fn quote_field(value: &str, opts: &CsvOptions) -> String {
    let q = opts.quote as char;
    let d = opts.delimiter as char;
    let needs_quoting =
        value.contains(d) || value.contains(q) || value.contains('\n') || value.contains('\r');

    if needs_quoting {
        let escaped = value.replace(q, &format!("{}{}", q, q));
        format!("{}{}{}", q, escaped, q)
    } else {
        value.to_string()
    }
}

/// Parse a single CSV line into fields, respecting quoting and escaping.
/// Returns None if the line is malformed (unclosed quote).
pub fn parse_csv_line(line: &str, opts: &CsvOptions) -> Option<Vec<String>> {
    let mut fields = Vec::new();
    let mut chars = line.chars().peekable();
    let q = opts.quote as char;
    let d = opts.delimiter as char;

    loop {
        // Start of a field
        if chars.peek() == Some(&q) {
            // Quoted field
            chars.next(); // consume opening quote
            let mut field = String::new();
            let mut closed = false;
            while let Some(c) = chars.next() {
                if c == q {
                    if chars.peek() == Some(&q) {
                        // Escaped quote (doubled)
                        chars.next();
                        field.push(q);
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    field.push(c);
                }
            }
            if !closed {
                return None; // malformed
            }
            fields.push(field);
            // Expect delimiter or end
            match chars.next() {
                Some(c) if c == d => {}
                None => break,
                _ => return None, // unexpected char after closing quote
            }
        } else {
            // Unquoted field — read until delimiter or end
            let mut field = String::new();
            loop {
                match chars.peek() {
                    Some(&c) if c == d => {
                        chars.next();
                        break;
                    }
                    Some(_) => {
                        field.push(chars.next().unwrap());
                    }
                    None => {
                        fields.push(field);
                        return Some(fields);
                    }
                }
            }
            fields.push(field);
        }
    }

    Some(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_opts() -> CsvOptions {
        CsvOptions::default()
    }

    #[test]
    fn test_parse_simple_line() {
        let opts = default_opts();
        let result = parse_csv_line("a,b,c", &opts).unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_quoted_field() {
        let opts = default_opts();
        let result = parse_csv_line("\"hello, world\",b", &opts).unwrap();
        assert_eq!(result, vec!["hello, world", "b"]);
    }

    #[test]
    fn test_parse_escaped_quote() {
        let opts = default_opts();
        let result = parse_csv_line("\"say \"\"hi\"\"\",b", &opts).unwrap();
        assert_eq!(result, vec!["say \"hi\"", "b"]);
    }

    #[test]
    fn test_parse_empty_fields() {
        let opts = default_opts();
        let result = parse_csv_line(",,", &opts).unwrap();
        assert_eq!(result, vec!["", "", ""]);
    }

    #[test]
    fn test_parse_tab_delimiter() {
        let mut opts = default_opts();
        opts.delimiter = b'\t';
        let result = parse_csv_line("a\tb\tc", &opts).unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_unclosed_quote_returns_none() {
        let opts = default_opts();
        assert!(parse_csv_line("\"unclosed", &opts).is_none());
    }

    #[test]
    fn test_quote_field_no_special() {
        let opts = default_opts();
        assert_eq!(quote_field("hello", &opts), "hello");
    }

    #[test]
    fn test_quote_field_with_comma() {
        let opts = default_opts();
        assert_eq!(quote_field("a,b", &opts), "\"a,b\"");
    }

    #[test]
    fn test_quote_field_with_embedded_quote() {
        let opts = default_opts();
        assert_eq!(quote_field("say \"hi\"", &opts), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_parse_delimiter_tab() {
        assert_eq!(parse_delimiter("'\\t'"), Some(b'\t'));
    }

    #[test]
    fn test_parse_delimiter_comma() {
        assert_eq!(parse_delimiter("','"), Some(b','));
    }

    #[test]
    fn test_parse_delimiter_pipe() {
        assert_eq!(parse_delimiter("'|'"), Some(b'|'));
    }
}
