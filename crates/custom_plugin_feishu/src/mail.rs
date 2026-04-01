use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client, get_tenant_token};
use wash_runtime::engine::ctx::ActiveCtx;

macro_rules! with_token {
    ($self:ident) => {{
        let Some(plugin) = $self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, $self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        }
    }};
}

fn build_query_string(params: &serde_json::Value) -> String {
    params
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| format!("{k}={s}")))
                .collect::<Vec<_>>()
                .join("&")
        })
        .unwrap_or_default()
}

impl bindings::custom::feishu::mail_sender::Host for ActiveCtx<'_> {
    async fn send_mail(
        &mut self,
        user_mailbox_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_post(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages"
            ),
            request_json,
        )
        .await)
    }

    async fn list_mails(
        &mut self,
        user_mailbox_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        let params: serde_json::Value = serde_json::from_str(&request_json)
            .map_err(|e| FeishuError::Internal(e.to_string()))?;
        let query = build_query_string(&params);
        let url = format!(
            "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        Ok(crate::http::feishu_get(&token, &url).await)
    }

    async fn get_mail(
        &mut self,
        user_mailbox_id: String,
        message_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_get(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages/{message_id}"
            ),
        )
        .await)
    }
}
