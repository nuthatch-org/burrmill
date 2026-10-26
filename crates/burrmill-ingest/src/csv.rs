//! An owned CSV reader. Quoting and the error on a hostile field are the product, so this
//! does not rent `arrow-csv`.

use crate::budget::Budget;
use crate::error::Result;
use crate::sniff::Dialect;

#[derive(Debug)]
pub struct Parsed {
    pub rows: Vec<Vec<String>>,
    /// 1-based physical row numbers whose quoted field ran to EOF.
    pub unclosed_rows: Vec<u64>,
    pub memory_bytes: u64,
}

pub fn parse_csv(
    text: &str,
    dialect: &Dialect,
    budget: &Budget,
    memory_base: u64,
    mut clock: impl FnMut() -> Result<()>,
) -> Result<Parsed> {
    let delim = dialect.delimiter as char;
    let quote = dialect.quote as char;
    let mut rows = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quote = false;
    let mut closed_quote = false;
    let mut unclosed_rows = Vec::new();
    let mut memory = memory_base;
    let mut physical: u64 = 1;

    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if closed_quote {
            closed_quote = false;
            if c == quote {
                push_char(&mut field, c, budget)?;
                in_quote = true;
                continue;
            }
            // The closing quote ended the field. The current char is the separator or junk.
            if c == delim {
                end_field(&mut row, &mut field, &mut memory, budget)?;
                continue;
            }
            if c == '\n' || c == '\r' {
                end_field(&mut row, &mut field, &mut memory, budget)?;
                end_row(
                    &mut rows,
                    &mut row,
                    &mut physical,
                    budget,
                    &mut clock,
                    &mut unclosed_rows,
                    false,
                )?;
                if c == '\r' && chars.peek() == Some(&'\n') {
                    chars.next();
                }
                continue;
            }
            // Quote closed, then more text. Keep it in the field.
            push_char(&mut field, c, budget)?;
            continue;
        }

        if in_quote {
            if c == quote {
                in_quote = false;
                closed_quote = true;
            } else {
                push_char(&mut field, c, budget)?;
            }
            continue;
        }

        if c == quote && field.is_empty() {
            in_quote = true;
            continue;
        }
        if c == delim {
            end_field(&mut row, &mut field, &mut memory, budget)?;
            continue;
        }
        if c == '\n' || c == '\r' {
            end_field(&mut row, &mut field, &mut memory, budget)?;
            end_row(
                &mut rows,
                &mut row,
                &mut physical,
                budget,
                &mut clock,
                &mut unclosed_rows,
                false,
            )?;
            if c == '\r' && chars.peek() == Some(&'\n') {
                chars.next();
            }
            continue;
        }
        push_char(&mut field, c, budget)?;
    }

    if in_quote {
        end_field(&mut row, &mut field, &mut memory, budget)?;
        end_row(
            &mut rows,
            &mut row,
            &mut physical,
            budget,
            &mut clock,
            &mut unclosed_rows,
            true,
        )?;
    } else if closed_quote || !field.is_empty() || !row.is_empty() {
        end_field(&mut row, &mut field, &mut memory, budget)?;
        end_row(
            &mut rows,
            &mut row,
            &mut physical,
            budget,
            &mut clock,
            &mut unclosed_rows,
            false,
        )?;
    }

    Ok(Parsed {
        rows,
        unclosed_rows,
        memory_bytes: memory,
    })
}

fn push_char(field: &mut String, c: char, budget: &Budget) -> Result<()> {
    let next = field.len() as u64 + c.len_utf8() as u64;
    if next > budget.max_cell_bytes {
        return Err(budget.breach("max_cell_bytes", budget.max_cell_bytes, next));
    }
    field.push(c);
    Ok(())
}

fn end_field(
    row: &mut Vec<String>,
    field: &mut String,
    memory: &mut u64,
    budget: &Budget,
) -> Result<()> {
    let trimmed = field.trim().to_string();
    *memory += trimmed.len() as u64;
    budget.charge("max_memory_bytes", budget.max_memory_bytes, *memory)?;
    row.push(trimmed);
    field.clear();
    Ok(())
}

fn end_row(
    rows: &mut Vec<Vec<String>>,
    row: &mut Vec<String>,
    physical: &mut u64,
    budget: &Budget,
    clock: &mut impl FnMut() -> Result<()>,
    unclosed: &mut Vec<u64>,
    was_unclosed: bool,
) -> Result<()> {
    if row.len() as u64 > budget.max_columns {
        return Err(budget.breach("max_columns", budget.max_columns, row.len() as u64));
    }
    if was_unclosed {
        unclosed.push(*physical);
    }
    let finished = std::mem::take(row);
    let blank_line = finished.len() <= 1 && finished.iter().all(|c| c.is_empty());
    if !blank_line {
        // +1 leaves room for a header row; the caller re-checks once it knows.
        if rows.len() as u64 > budget.max_rows {
            return Err(budget.breach("max_rows", budget.max_rows, rows.len() as u64 + 1));
        }
        rows.push(finished);
    }
    *physical += 1;
    if physical.is_multiple_of(4096) {
        clock()?;
    }
    Ok(())
}
