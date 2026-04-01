use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, bindings};
use crate::{PLUGIN_ID, get_client, get_tenant_token};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::cardkit_sender::Host for ActiveCtx<'_> {
    async fn create_card(
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

        Ok(crate::http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/cardkit/v1/cards",
            request_json,
        )
        .await)
    }

    async fn update_card(
        &mut self,
        card_id: String,
        request_json: String,
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

        Ok(crate::http::feishu_patch(
            &token,
            &format!("https://open.feishu.cn/open-apis/cardkit/v1/cards/{card_id}"),
            request_json,
        )
        .await
        .map(|_| ()))
    }
}
