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

impl bindings::custom::feishu::docs_sender::Host for ActiveCtx<'_> {
    async fn create_document(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/docx/v1/documents",
            request_json,
        )
        .await)
    }

    async fn get_document(
        &mut self,
        document_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}"),
        )
        .await)
    }

    async fn create_spreadsheet(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets",
            request_json,
        )
        .await)
    }

    async fn get_spreadsheet(
        &mut self,
        spreadsheet_token: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}"),
        )
        .await)
    }

    async fn create_bitable(
        &mut self,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/bitable/v1/apps",
            request_json,
        )
        .await)
    }

    async fn list_bitable_tables(
        &mut self,
        app_token: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"),
        )
        .await)
    }

    async fn list_bitable_records(
        &mut self,
        app_token: String,
        table_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let token = with_token!(self);
        Ok(crate::http::feishu_post(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
            ),
            request_json,
        )
        .await)
    }
}
