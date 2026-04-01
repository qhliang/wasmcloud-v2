use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client, get_tenant_token};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::ai_sender::Host for ActiveCtx<'_> {
    async fn recognize_text(
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
            "https://open.feishu.cn/open-apis/optical_char_recognition/v1/image/basic_recognize",
            request_json,
        )
        .await)
    }

    async fn translate(
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
            "https://open.feishu.cn/open-apis/translation/v1/text/translate",
            request_json,
        )
        .await)
    }
}
