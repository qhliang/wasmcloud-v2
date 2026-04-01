use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::sender::Host for ActiveCtx<'_> {
    async fn send_text(
        &mut self,
        chat_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request = open_lark::service::im::v1::message::CreateMessageRequest::with_msg(
            &chat_id, msg, "chat_id",
        );

        match client.im.v1.message.create(request, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    async fn send_text_to_user(
        &mut self,
        receive_id: String,
        receive_id_type: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request = open_lark::service::im::v1::message::CreateMessageRequest::with_msg(
            &receive_id,
            msg,
            &receive_id_type,
        );

        match client.im.v1.message.create(request, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    async fn reply_message(
        &mut self,
        message_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request =
            open_lark::service::im::v1::message::CreateMessageRequest::with_msg("", msg, "");

        match client.im.v1.message.reply(&message_id, request, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    async fn get_access_token(&mut self) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let token_manager = client.config.token_manager.lock().await;
        match token_manager
            .get_tenant_access_token(&client.config, "", "", &client.config.app_ticket_manager)
            .await
        {
            Ok(token) => Ok(Ok(token)),
            Err(e) => Ok(Err(FeishuError::AuthFailed(e.to_string()))),
        }
    }
}
