use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, bindings};
use crate::{PLUGIN_ID, get_client, get_tenant_token};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::calendar_sender::Host for ActiveCtx<'_> {
    async fn list_calendars(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let token = match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };

        let params: serde_json::Value = serde_json::from_str(&request_json)
            .map_err(|e| FeishuError::Internal(e.to_string()))?;
        let query = build_query_string(&params);

        let url = format!(
            "https://open.feishu.cn/open-apis/calendar/v4/calendars{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );

        Ok(crate::http::feishu_get(&token, &url).await)
    }

    async fn create_calendar_event(
        &mut self,
        calendar_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let token = match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };

        Ok(crate::http::feishu_post(
            &token,
            &format!("https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events"),
            request_json,
        )
        .await)
    }

    async fn list_calendar_events(
        &mut self,
        calendar_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let token = match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };

        let params: serde_json::Value = serde_json::from_str(&request_json)
            .map_err(|e| FeishuError::Internal(e.to_string()))?;
        let query = build_query_string(&params);

        let url = format!(
            "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );

        Ok(crate::http::feishu_get(&token, &url).await)
    }

    async fn get_calendar_event(
        &mut self,
        calendar_id: String,
        event_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let token = match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };

        Ok(crate::http::feishu_get(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}"
            ),
        )
        .await)
    }

    async fn delete_calendar_event(
        &mut self,
        calendar_id: String,
        event_id: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let token = match get_tenant_token(&client).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };

        Ok(crate::http::feishu_delete(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}"
            ),
            None,
        )
        .await)
    }
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
