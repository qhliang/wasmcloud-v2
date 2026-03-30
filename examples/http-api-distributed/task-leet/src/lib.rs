wit_bindgen::generate!({
    path: "../wit",
    world: "task",
    with: {
        "wasmcloud:messaging/types@0.2.0": generate,
        "wasmcloud:messaging/consumer@0.2.0": generate,
        "wasi:logging/logging@0.1.0-draft": generate,
    },
});

use crate::wasmcloud::messaging::types::BrokerMessage;
use wasmcloud::messaging::consumer;
use wasi::logging::logging::{log, Level};
#[allow(unused)]
use wstd::prelude::*;

const LOG_CTX: &str = "task-leet";

struct Component;

export!(Component);

impl exports::wasmcloud::messaging::handler::Guest for Component {
    fn handle_message(msg: BrokerMessage) -> Result<(), String> {
        log(Level::Info, LOG_CTX, &format!("Received message on subject: {}", msg.subject));

        let Some(subject) = msg.reply_to else {
            log(Level::Error, LOG_CTX, "Missing reply_to in message");
            return Err("missing reply_to".to_string());
        };

        let payload = String::from_utf8(msg.body.to_vec())
            .map_err(|e| {
                let err = format!("Failed to decode message body: {}", e);
                log(Level::Error, LOG_CTX, &err);
                err
            })?;

        log(Level::Debug, LOG_CTX, &format!("Processing payload: {} bytes", payload.len()));

        let leet_result = to_leet_speak(&payload);
        log(Level::Info, LOG_CTX, &format!("Converted: '{}' -> '{}'",
            if payload.len() > 50 { &payload[..50] } else { &payload },
            if leet_result.len() > 50 { &leet_result[..50] } else { &leet_result }
        ));

        let reply = BrokerMessage {
            subject,
            body: leet_result.into(),
            reply_to: None,
        };

        consumer::publish(&reply).map_err(|e| {
            log(Level::Error, LOG_CTX, &format!("Failed to publish reply: {}", e));
            e
        })?;

        log(Level::Debug, LOG_CTX, "Reply published successfully");
        Ok(())
    }
}

fn to_leet_speak(input: &str) -> String {
    input
        .chars()
        .map(|c| match c.to_ascii_lowercase() {
            'a' => '4',
            'e' => '3',
            'i' => '1',
            'o' => '0',
            's' => '5',
            't' => '7',
            'l' => '1',
            _ => c,
        })
        .collect()
}
