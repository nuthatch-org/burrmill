//! Exports for the page. No HTTP in here.

use wasm_bindgen::prelude::*;

use crate::chart::Chart;
use crate::model::parse_model as parse_model_text;
use crate::session::{Session, feedback_error, feedback_shape, present};

#[wasm_bindgen]
pub struct WasmSession {
    inner: Session,
}

#[wasm_bindgen]
impl WasmSession {
    pub fn open(bytes: &[u8]) -> Result<WasmSession, JsError> {
        let table = burrmill_ingest::ingest_bytes(
            bytes,
            burrmill_ingest::FormatHint::Sniff,
            &burrmill_ingest::Budget::untrusted(),
        )
        .map_err(|e| JsError::new(&e.to_string()))?;
        Session::open(table)
            .map(|inner| WasmSession { inner })
            .map_err(|e| JsError::new(&e))
    }

    pub fn columns(&self) -> String {
        serde_json::to_string(&self.inner.columns()).unwrap_or_else(|_| "[]".into())
    }

    pub fn preview(&self, n: u32) -> String {
        serde_json::to_string(&self.inner.preview(n as usize)).unwrap_or_else(|_| "[]".into())
    }

    pub fn rename(&mut self, from: &str, to: &str) -> Result<(), JsError> {
        self.inner.rename(from, to).map_err(|e| JsError::new(&e))
    }

    pub fn card(&self, signs_json: &str, dates_json: &str) -> Result<String, JsError> {
        let signs: Vec<String> =
            serde_json::from_str(signs_json).map_err(|e| JsError::new(&e.to_string()))?;
        let dates: Vec<String> =
            serde_json::from_str(dates_json).map_err(|e| JsError::new(&e.to_string()))?;
        self.inner
            .card(&signs, &dates)
            .map_err(|e| JsError::new(&e))
    }

    pub fn grammar(&self) -> String {
        self.inner.grammar()
    }

    pub async fn query(&self, sql: &str) -> Result<String, JsError> {
        let out = self.inner.query(sql).await.map_err(|e| JsError::new(&e))?;
        serde_json::to_string(&out).map_err(|e| JsError::new(&e.to_string()))
    }
}

fn json_out(value: impl serde::Serialize) -> Result<String, JsError> {
    serde_json::to_string(&value).map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn parse_model(text: &str) -> Result<String, JsError> {
    json_out(parse_model_text(text).map_err(|e| JsError::new(&e))?)
}

#[wasm_bindgen]
pub fn shape_feedback(answer_json: &str) -> Result<String, JsError> {
    let answer = serde_json::from_str(answer_json).map_err(|e| JsError::new(&e.to_string()))?;
    Ok(feedback_shape(&answer))
}

#[wasm_bindgen]
pub fn error_feedback(message: &str) -> String {
    feedback_error(message)
}

#[wasm_bindgen]
pub fn present_answer(
    answer_json: &str,
    chart_json: &str,
    narration: &str,
    group: &str,
    decimal: &str,
) -> Result<String, JsError> {
    let answer = serde_json::from_str(answer_json).map_err(|e| JsError::new(&e.to_string()))?;
    let chart: Chart =
        serde_json::from_str(chart_json).map_err(|e| JsError::new(&e.to_string()))?;
    json_out(present(&answer, &chart, narration, group, decimal))
}
