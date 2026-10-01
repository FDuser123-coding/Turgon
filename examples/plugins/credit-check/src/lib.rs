// credit-check: when a sales order is created, propose its credit status.
// "approved" when the order is within the customer's credit limit and the
// risk service (api.acme-risk.example) scores the customer 50 or more;
// "review" otherwise. If the risk service is unavailable, the credit limit
// decides alone. The outcome is published as credit.checked for other
// plugins (plugin.credit-check.credit.checked).
//
// The plugin only reads, calls the one host it is granted and proposes;
// Turgon puts the API key in ({{secret:acme-api-key}}), so the plugin never
// holds it, and the write guard, policies and approvals decide what is
// written.
wit_bindgen::generate!({ world: "logic-plugin", path: "../../../wit" });

use serde_json::{json, Value};
use turgon::stack::entities::{get, propose_change};
use turgon::stack::events::publish;
use turgon::stack::http::{send, Header, Request};
use turgon::stack::types::{EntityRef, ErrorCode};

struct CreditCheck;

fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn describe(e: ErrorCode) -> String {
    match e {
        ErrorCode::Denied => "denied".into(),
        ErrorCode::NotFound => "not found".into(),
        ErrorCode::Invalid(m) => m,
        ErrorCode::Unavailable(m) => format!("unavailable: {m}"),
    }
}

// The customer's risk score, or None if the service cannot tell.
fn risk_score(customer_id: &str) -> Option<f64> {
    let req = Request {
        method: "GET".into(),
        url: format!("https://api.acme-risk.example/v1/scores/{customer_id}"),
        headers: vec![
            Header { name: "Authorization".into(), value: "Bearer {{secret:acme-api-key}}".into() },
            Header { name: "Accept".into(), value: "application/json".into() },
        ],
        body: vec![],
    };
    let resp = send(&req).ok()?;
    if resp.status != 200 {
        return None;
    }
    let v: Value = serde_json::from_slice(&resp.body).ok()?;
    number(&v["score"])
}

impl Guest for CreditCheck {
    fn handle(event: Vec<u8>) -> Result<(), String> {
        let ev: Value = serde_json::from_slice(&event).map_err(|e| format!("event: {e}"))?;
        if ev["type"] != "model.SalesOrder.created" {
            return Ok(());
        }
        let order = &ev["record"];
        let id = ev["id"].as_str().ok_or("the event has no order id")?;
        let customer_id = order["customer_id"].as_str().ok_or("the order has no customer_id")?;
        let net = number(&order["net_value"]).ok_or("the order has no net_value")?;

        let customer = get(&EntityRef { kind: "Customer".into(), id: customer_id.into() })
            .map_err(|e| format!("customer {customer_id}: {}", describe(e)))?;
        let customer: Value = serde_json::from_str(&customer).map_err(|e| format!("customer: {e}"))?;
        let limit = number(&customer["credit_limit"]).ok_or("the customer has no credit_limit")?;

        let score = risk_score(customer_id);
        let ok = net <= limit && score.map_or(true, |s| s >= 50.0);
        let status = if ok { "approved" } else { "review" };
        let patch = json!({ "creditStatus": status }).to_string();
        propose_change(&EntityRef { kind: "SalesOrder".into(), id: id.into() }, &patch)
            .map_err(|e| format!("proposal for {id}: {}", describe(e)))?;

        let checked = json!({ "order": id, "customer": customer_id, "status": status, "score": score, "limit": limit, "netValue": net });
        publish("credit.checked", checked.to_string().as_bytes()).map_err(|e| format!("publish: {}", describe(e)))?;
        Ok(())
    }
}

export!(CreditCheck);
