use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::bot_sender::Host for ActiveCtx<'_> {
    async fn get_bot_info(&mut self) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        match client.bot.v3.info.get(None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }
}
