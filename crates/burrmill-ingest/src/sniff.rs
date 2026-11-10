//! Format, encoding, delimiter, quote, locale. The guess is reported, never hidden.
//!
//! Encoding is decided from the first `sniff_bytes` only. A file that is ASCII for that prefix
//! and Windows-1251 after it will be read as UTF-8.

use crate::encoding::{CP1251, CP1252, decode_table};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingKind {
    Utf8,
    Utf16Le,
    Utf16Be,
    Windows1251,
    Windows1252,
}

impl EncodingKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Utf16Le => "UTF-16LE",
            Self::Utf16Be => "UTF-16BE",
            Self::Windows1251 => "windows-1251",
            Self::Windows1252 => "windows-1252",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Locale {
    /// `.` is the decimal mark, `,` groups thousands.
    Point,
    /// `,` is the decimal mark, `.` groups thousands. Bulgarian and German exports.
    Comma,
}

/// What sniff decided. This is the `format` on the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dialect {
    pub delimiter: u8,
    pub quote: u8,
    pub encoding: EncodingKind,
    pub locale: Locale,
}

/// Magic this slice refuses to interpret as text.
pub fn refused_magic(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"PAR1") {
        Some("parquet")
    } else if bytes.starts_with(b"PK\x03\x04") {
        Some("zip")
    } else if bytes.starts_with(b"\x1f\x8b") {
        Some("gzip")
    } else {
        None
    }
}

pub fn sniff_encoding(sample: &[u8]) -> EncodingKind {
    if sample.starts_with(&[0xEF, 0xBB, 0xBF]) {
        return EncodingKind::Utf8;
    }
    if sample.starts_with(&[0xFF, 0xFE]) {
        return EncodingKind::Utf16Le;
    }
    if sample.starts_with(&[0xFE, 0xFF]) {
        return EncodingKind::Utf16Be;
    }
    if std::str::from_utf8(sample).is_ok() {
        return EncodingKind::Utf8;
    }
    let as_1251 = decode_table(sample, &CP1251);
    let cyrillic = as_1251
        .chars()
        .filter(|c| ('\u{0400}'..='\u{04FF}').contains(c))
        .count();
    let latin = as_1251.chars().filter(|c| c.is_ascii_alphabetic()).count();
    if cyrillic > latin {
        EncodingKind::Windows1251
    } else {
        EncodingKind::Windows1252
    }
}

pub fn decode(bytes: &[u8], kind: EncodingKind) -> String {
    let raw = match kind {
        EncodingKind::Utf8 if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) => &bytes[3..],
        EncodingKind::Utf16Le if bytes.starts_with(&[0xFF, 0xFE]) => &bytes[2..],
        EncodingKind::Utf16Be if bytes.starts_with(&[0xFE, 0xFF]) => &bytes[2..],
        _ => bytes,
    };
    let text = match kind {
        EncodingKind::Utf8 => match std::str::from_utf8(raw) {
            Ok(s) => s.to_string(),
            // The sniff window was valid UTF-8 and the tail was not. Replacement, not a panic.
            Err(_) => String::from_utf8_lossy(raw).into_owned(),
        },
        EncodingKind::Utf16Le => decode_utf16(raw, false),
        EncodingKind::Utf16Be => decode_utf16(raw, true),
        EncodingKind::Windows1251 => decode_table(raw, &CP1251),
        EncodingKind::Windows1252 => decode_table(raw, &CP1252),
    };
    text.trim_start_matches('\u{feff}').to_string()
}

fn decode_utf16(bytes: &[u8], be: bool) -> String {
    let mut units = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i + 1 < bytes.len() {
        let unit = if be {
            u16::from_be_bytes([bytes[i], bytes[i + 1]])
        } else {
            u16::from_le_bytes([bytes[i], bytes[i + 1]])
        };
        units.push(unit);
        i += 2;
    }
    String::from_utf16_lossy(&units)
}

pub fn sniff_dialect(text: &str, encoding: EncodingKind) -> Dialect {
    let sample = prefix(text, 64 * 1024);
    let quote = sniff_quote(sample);
    let delimiter = sniff_delimiter(sample, quote);
    let locale = sniff_locale(sample, delimiter, quote);
    Dialect {
        delimiter,
        quote,
        encoding,
        locale,
    }
}

fn prefix(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn sniff_quote(sample: &str) -> u8 {
    let doubles = sample.bytes().filter(|b| *b == b'"').count();
    let singles = sample.bytes().filter(|b| *b == b'\'').count();
    if singles > doubles { b'\'' } else { b'"' }
}

fn sniff_delimiter(sample: &str, quote: u8) -> u8 {
    const CANDIDATES: &[u8] = b",;\t|";
    let mut best = b',';
    let mut best_score = 0usize;
    for &delim in CANDIDATES {
        let counts = field_counts(sample, delim, quote);
        if counts.len() < 2 {
            continue;
        }
        let mode = mode_count(&counts);
        if mode < 2 {
            continue;
        }
        let consistent = counts.iter().filter(|c| **c == mode).count();
        // A delimiter that only some lines share is a decimal comma, not a column break.
        if consistent * 2 < counts.len() {
            continue;
        }
        let score = mode * consistent;
        if score > best_score {
            best_score = score;
            best = delim;
        }
    }
    best
}

fn field_counts(sample: &str, delim: u8, quote: u8) -> Vec<usize> {
    let mut counts = Vec::new();
    let mut fields = 1usize;
    let mut in_quotes = false;
    let mut saw = false;
    let quote_c = quote as char;
    let delim_c = delim as char;
    for c in sample.chars() {
        saw = true;
        if c == quote_c {
            in_quotes = !in_quotes;
            continue;
        }
        if in_quotes {
            continue;
        }
        if c == delim_c {
            fields += 1;
        } else if c == '\n' {
            counts.push(fields);
            fields = 1;
        }
    }
    if saw && fields > 1 {
        counts.push(fields);
    }
    counts
}

fn mode_count(counts: &[usize]) -> usize {
    let mut best = 0;
    let mut best_n = 0;
    for &c in counts {
        let n = counts.iter().filter(|x| **x == c).count();
        if n > best_n {
            best_n = n;
            best = c;
        }
    }
    best
}

fn sniff_locale(sample: &str, delim: u8, quote: u8) -> Locale {
    // One vote per token. The last mark is the decimal one, and a dotted date abstains,
    // otherwise `01.02.2024` outvotes `1.234,56`. Delimiter marks are not part of the token.
    let mut point = 0usize;
    let mut comma = 0usize;
    let mut in_quotes = false;
    let quote_c = quote as char;
    let chars: Vec<char> = sample.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == quote_c {
            in_quotes = !in_quotes;
            i += 1;
            continue;
        }
        if !in_quotes && c == delim as char {
            i += 1;
            continue;
        }
        if c.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < chars.len() {
                let n = chars[i];
                if n == quote_c || (!in_quotes && n == delim as char) {
                    break;
                }
                if n.is_ascii_digit() || n == '.' || n == ',' {
                    i += 1;
                } else {
                    break;
                }
            }
            vote_number(&chars[start..i], &mut point, &mut comma);
            continue;
        }
        i += 1;
    }
    if comma > point {
        Locale::Comma
    } else {
        Locale::Point
    }
}

fn vote_number(token: &[char], point: &mut usize, comma: &mut usize) {
    let text: String = token.iter().collect();
    if is_dotted_date(&text) {
        return;
    }
    match (text.rfind('.'), text.rfind(',')) {
        (Some(dot), Some(comma_at)) if comma_at > dot => *comma += 1,
        (Some(dot), Some(comma_at)) if dot > comma_at => *point += 1,
        (Some(_), None) => *point += 1,
        (None, Some(_)) => *comma += 1,
        _ => {}
    }
}

fn is_dotted_date(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    if parts
        .iter()
        .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return false;
    }
    let (a, b, c) = (parts[0].len(), parts[1].len(), parts[2].len());
    // Four-digit year. `1.234.567` is a grouped number and still votes.
    (c == 4 && a <= 2 && b <= 2) || (a == 4 && b <= 2 && c <= 2)
}
