use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client, get_tenant_token};
use wash_runtime::engine::ctx::ActiveCtx;

macro_rules! get_token_helper {
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

impl bindings::custom::feishu::group_sender::Host for ActiveCtx<'_> {
    async fn create_group(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = get_token_helper!(self);
        Ok(crate::http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/im/v1/chats",
            request_json,
        )
        .await)
    }

    async fn get_group(
        &mut self,
        chat_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = get_token_helper!(self);
        Ok(crate::http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}"),
        )
        .await)
    }

    async fn update_group(
        &mut self,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let token = get_token_helper!(self);
        Ok(crate::http::feishu_put(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}"),
            request_json,
        )
        .await)
    }

    async fn add_group_members(
        &mut self,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = get_token_helper!(self);
        Ok(crate::http::feishu_post(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members"),
            request_json,
        )
        .await)
    }

    async fn remove_group_members(
        &mut self,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let token = get_token_helper!(self);
        Ok(crate::http::feishu_delete(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members"),
            Some(request_json),
        )
        .await)
    }

    async fn list_group_members(
        &mut self,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = get_token_helper!(self);
        let params: serde_json::Value = serde_json::from_str(&request_json)
            .map_err(|e| FeishuError::Internal(e.to_string()))?;
        let query = build_query_string(&params);
        let url = if query.is_empty() {
            format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members")
        } else {
            format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members?{query}")
        };
        Ok(crate::http::feishu_get(&token, &url).await)
    }
}
