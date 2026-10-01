// Commands, one per event:
//   "http METHOD URL [HEADER-NAME HEADER-VALUE]"  then the body, if any, after a newline
//   "publish TOPIC PAYLOAD", "get KIND ID", "many-http URL"
// Results are published on the topic "result", so the test host sees them.
wit_bindgen::generate!({ world: "logic-plugin", path: "../../../../wit" });

use turgon::stack::entities::get;
use turgon::stack::events::publish;
use turgon::stack::http::{send, Header, Request};
use turgon::stack::types::{EntityRef, ErrorCode};

struct Probe;

fn err(e: ErrorCode) -> String {
    match e {
        ErrorCode::Denied => "err denied".into(),
        ErrorCode::NotFound => "err not-found".into(),
        ErrorCode::Invalid(m) => format!("err invalid {m}"),
        ErrorCode::Unavailable(m) => format!("err unavailable {m}"),
    }
}

impl Guest for Probe {
    fn handle(event: Vec<u8>) -> Result<(), String> {
        let text = String::from_utf8(event).map_err(|_| "event is not UTF-8".to_string())?;
        let (line, body) = text.split_once('\n').unwrap_or((&text, ""));
        let parts: Vec<&str> = line.split(' ').collect();
        let out = match parts[0] {
            "http" => {
                let mut headers = Vec::new();
                if parts.len() >= 5 {
                    headers.push(Header { name: parts[3].into(), value: parts[4..].join(" ") });
                }
                let req = Request { method: parts[1].into(), url: parts[2].into(), headers, body: body.as_bytes().to_vec() };
                match send(&req) {
                    Ok(r) => {
                        let hs: Vec<String> = r.headers.iter().map(|h| format!("{}={}", h.name, h.value)).collect();
                        format!("ok {} [{}] {}", r.status, hs.join(","), String::from_utf8_lossy(&r.body))
                    }
                    Err(e) => err(e),
                }
            }
            "many-http" => {
                let mut n = 0;
                for _ in 0..40 {
                    let req = Request { method: "GET".into(), url: parts[1].into(), headers: vec![], body: vec![] };
                    match send(&req) {
                        Ok(_) => n += 1,
                        Err(e) => return { let _ = publish("result", format!("after {n}: {}", err(e)).as_bytes()); Ok(()) },
                    }
                }
                format!("sent {n}")
            }
            "publish" => match publish(parts[1], parts[2..].join(" ").as_bytes()) {
                Ok(()) => "ok published".into(),
                Err(e) => err(e),
            },
            "get" => match get(&EntityRef { kind: parts[1].into(), id: parts[2].into() }) {
                Ok(v) => format!("ok {v}"),
                Err(e) => err(e),
            },
            other => return Err(format!("unknown command {other:?}")),
        };
        let _ = publish("result", out.as_bytes());
        Ok(())
    }
}

export!(Probe);
