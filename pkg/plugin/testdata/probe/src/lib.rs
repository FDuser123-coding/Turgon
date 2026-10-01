// Commands, one per event: "get KIND ID", "propose KIND ID PATCH",
// "publish TOPIC", "fail MESSAGE", "loop", "grow", "trap", "spam".
// Results are published on the topic "result", so the test host sees them.
wit_bindgen::generate!({ world: "logic-plugin", path: "../../../../wit" });

use turgon::stack::entities::{get, propose_change};
use turgon::stack::events::publish;
use turgon::stack::types::{EntityRef, ErrorCode};

struct Probe;

fn show<T: std::fmt::Display>(r: Result<T, ErrorCode>) -> String {
    match r {
        Ok(v) => format!("ok {}", v),
        Err(ErrorCode::Denied) => "err denied".to_string(),
        Err(ErrorCode::NotFound) => "err not-found".to_string(),
        Err(ErrorCode::Invalid(m)) => format!("err invalid {}", m),
    }
}

impl Guest for Probe {
    fn handle(event: Vec<u8>) -> Result<(), String> {
        let text = String::from_utf8(event).map_err(|_| "event is not UTF-8".to_string())?;
        let mut parts = text.splitn(4, ' ');
        let cmd = parts.next().unwrap_or("");
        let mut arg = || parts.next().unwrap_or("").to_string();
        let out = match cmd {
            "get" => {
                let (kind, id) = (arg(), arg());
                show(get(&EntityRef { kind, id }))
            }
            "propose" => {
                let (kind, id, patch) = (arg(), arg(), arg());
                show(propose_change(&EntityRef { kind, id }, &patch))
            }
            "publish" => {
                let topic = arg();
                let payload: Vec<u8> = (0u8..=255).collect();
                show(publish(&topic, &payload).map(|_| "published"))
            }
            "fail" => return Err(arg()),
            "loop" => loop {
                std::hint::black_box(0);
            },
            "grow" => {
                let mut v: Vec<Vec<u8>> = Vec::new();
                loop {
                    v.push(vec![1u8; 1 << 20]);
                    std::hint::black_box(&v);
                }
            }
            "trap" => unreachable!("trap requested"),
            "spam" => {
                for _ in 0..100 {
                    let _ = get(&EntityRef { kind: "Customer".into(), id: "C-100".into() });
                }
                "spammed".to_string()
            }
            other => return Err(format!("unknown command {:?}", other)),
        };
        let _ = publish("result", out.as_bytes());
        Ok(())
    }
}

export!(Probe);
