use crate::bindings::custom::feishu::types::FeishuError;
use crate::{Feishu, bindings};
use crate::{PLUGIN_ID, get_client};
use wash_runtime::engine::ctx::ActiveCtx;

impl bindings::custom::feishu::contact_sender::Host for ActiveCtx<'_> {
    async fn get_user(
        &mut self,
        user_id: String,
        user_id_type: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let req = open_lark::service::contact::v3::user::GetUserRequest {
            user_id_type: Some(user_id_type),
            ..Default::default()
        };

        match client.contact.v3.user.get(&user_id, &req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn batch_get_users(
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

        let req: open_lark::service::contact::v3::user::BatchGetUsersRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.contact.v3.user.batch(&req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn search_users(
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

        let req: open_lark::service::contact::v3::user::SearchUsersRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.contact.v3.user.search(&req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn list_department_users(
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

        let req: open_lark::service::contact::v3::user::FindUsersByDepartmentRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.contact.v3.user.find_by_department(&req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn get_department(
        &mut self,
        department_id: String,
        department_id_type: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Ok(Err(FeishuError::Internal("feishu plugin not found".into())));
        };
        let client = match get_client(&plugin, self.component_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };

        let req = open_lark::service::contact::v3::department::GetDepartmentRequest {
            user_id_type: Some("open_id".to_string()),
            department_id_type: Some(department_id_type),
        };

        match client.contact.v3.department.get(&department_id, &req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    async fn list_sub_departments(
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

        let req: open_lark::service::contact::v3::department::GetChildrenDepartmentsRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };

        match client.contact.v3.department.children(&req).await {
            Ok(resp) => match serde_json::to_string(&resp) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }
}
