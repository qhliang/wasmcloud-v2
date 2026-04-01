use crate::bindings;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, PLUGIN_ID, get_client};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::task_sender::Host for ActiveCtx<'_> {
    async fn create_task(
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

        let req: open_lark::service::task::v2::task::CreateTaskRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.task.task.create(req, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn get_task(
        &mut self,
        task_guid: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        match client.task.task.get(&task_guid, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn update_task(
        &mut self,
        task_guid: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let req: open_lark::service::task::v2::task::UpdateTaskRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.task.task.patch(&task_guid, req, None, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn delete_task(
        &mut self,
        task_guid: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        match client.task.task.delete(&task_guid, None, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn list_tasks(
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

        let params: serde_json::Value = match serde_json::from_str(&request_json) {
            Ok(p) => p,
            Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
        };

        let page_size = params
            .get("page_size")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let page_token = params
            .get("page_token")
            .and_then(|v| v.as_str())
            .map(String::from);

        match client
            .task
            .task
            .list(
                page_size,
                page_token.as_deref(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn create_tasklist(
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

        let req: open_lark::service::task::v2::tasklist::CreateTasklistRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.task.tasklist.create(req, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn list_tasklists(
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

        let params: serde_json::Value = match serde_json::from_str(&request_json) {
            Ok(p) => p,
            Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
        };

        let page_size = params
            .get("page_size")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);
        let page_token = params
            .get("page_token")
            .and_then(|v| v.as_str())
            .map(String::from);

        match client
            .task
            .tasklist
            .list(page_size, page_token.as_deref(), None, None)
            .await
        {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }
}
