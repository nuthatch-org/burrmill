//! One JSON object, keys `sql`, `chart`, `narration`. Anything else is a refusal.
//! An unknown chart kind becomes a table; it is not a reason to reject the object.

use serde::Serialize;
use serde_json::Value;

use crate::chart::{Chart, ChartKind};

#[derive(Debug, Serialize)]
pub struct Model {
    pub sql: String,
    pub chart: Chart,
    pub narration: String,
}

pub fn parse_model(text: &str) -> Result<Model, String> {
    let text = strip_fence(text.trim());
    let value: Value =
        serde_json::from_str(text).map_err(|_| "model reply is not one json object".to_string())?;
    let Some(obj) = value.as_object() else {
        return Err("model reply is not one json object".into());
    };
    for key in obj.keys() {
        if key != "sql" && key != "chart" && key != "narration" {
            return Err("model reply has an unexpected key".into());
        }
    }
    let sql = obj
        .get("sql")
        .and_then(Value::as_str)
        .ok_or("sql must be a string")?;
    let narration = obj
        .get("narration")
        .and_then(Value::as_str)
        .ok_or("narration must be a string")?;
    let chart = parse_chart(obj.get("chart").ok_or("chart is required")?)?;
    Ok(Model {
        sql: sql.to_string(),
        chart,
        narration: narration.to_string(),
    })
}

fn parse_chart(v: &Value) -> Result<Chart, String> {
    let Some(obj) = v.as_object() else {
        return Err("chart must be an object".into());
    };
    for key in obj.keys() {
        if key != "kind" && key != "x" && key != "y" {
            return Err("chart has an unexpected key".into());
        }
    }
    let kind = match obj.get("kind").and_then(Value::as_str) {
        Some(s) => ChartKind::parse(s).unwrap_or(ChartKind::Table),
        None => ChartKind::Table,
    };
    let x = match obj.get("x") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err("chart.x must be a string".into()),
    };
    let y = match obj.get("y") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => {
            let mut ys = Vec::with_capacity(items.len());
            for item in items {
                let Some(s) = item.as_str() else {
                    return Err("chart.y must be strings".into());
                };
                ys.push(s.to_string());
            }
            ys
        }
        Some(_) => return Err("chart.y must be a string or an array".into()),
    };
    Ok(Chart { kind, x, y })
}

fn strip_fence(t: &str) -> &str {
    let Some(rest) = t.strip_prefix("```") else {
        return t;
    };
    let rest = rest
        .get(..4)
        .filter(|s| s.eq_ignore_ascii_case("json"))
        .map(|_| &rest[4..])
        .unwrap_or(rest);
    let rest = rest.trim_start_matches(['\n', '\r']);
    match rest.strip_suffix("```") {
        Some(body) => body.trim(),
        None => t,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_object_and_a_fence() {
        let raw = "```json\n{\"sql\":\"SELECT 1\",\"chart\":{\"kind\":\"table\"},\"narration\":\"Ok\"}\n```";
        let m = parse_model(raw).unwrap();
        assert_eq!(m.sql, "SELECT 1");
        assert_eq!(m.narration, "Ok");
        assert_eq!(m.chart.kind, ChartKind::Table);
    }

    #[test]
    fn an_extra_key_is_refused_and_an_unknown_kind_is_a_table() {
        let extra = r#"{"sql":"SELECT 1","chart":{"kind":"table"},"narration":"Ok","note":"x"}"#;
        assert!(parse_model(extra).is_err());
        let pie = r#"{"sql":"SELECT 1","chart":{"kind":"pie","y":"total"},"narration":"Ok"}"#;
        assert_eq!(parse_model(pie).unwrap().chart.kind, ChartKind::Table);
        let y = r#"{"sql":"SELECT 1","chart":{"kind":"bar","x":"merchant","y":["total"]},"narration":"Ok"}"#;
        let m = parse_model(y).unwrap();
        assert_eq!(m.chart.y, vec!["total".to_string()]);
    }
}
