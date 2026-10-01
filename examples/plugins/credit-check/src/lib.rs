// credit-check: when a sales order is created, compare its net value with
// the customer's credit limit and propose the order's credit status:
// "approved" within the limit, "review" above it. The plugin only reads
// and proposes; Turgon's write guard, policies and approvals decide what is
// written, and the audit log records it as the plugin's.
wit_bindgen::generate!({ world: "logic-plugin", path: "../../../wit" });

use serde_json::{json, Value};
use turgon::stack::entities::{get, propose_change};
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
    }
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

        let status = if net <= limit { "approved" } else { "review" };
        let patch = json!({ "creditStatus": status }).to_string();
        propose_change(&EntityRef { kind: "SalesOrder".into(), id: id.into() }, &patch)
            .map_err(|e| format!("proposal for {id}: {}", describe(e)))?;
        Ok(())
    }
}

export!(CreditCheck);
