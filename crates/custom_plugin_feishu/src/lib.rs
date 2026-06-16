//! # Feishu (Lark) Host Plugin (Resource-based)
//!
//! All 10 sender interfaces collapsed into one `feishu-client` resource with ~35 methods.
//! Config sources (priority):
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::ResolvedWorkload;
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker, find_interface};
use wash_runtime::wit::{WitInterface, WitWorld};
use wasmtime::component::Resource;

mod bindings {
    wasmtime::component::bindgen!({
        world: "feishu",
        imports: {
            default: async | trappable | tracing,
        },
        exports: { default: async | tracing },
        with: {
            "custom:feishu/sender.feishu-client": super::FeishuClientHandle,
        },
    });
}

use bindings::custom::feishu::types::{FeishuConfig, FeishuError, ImMessage};

pub(crate) const PLUGIN_ID: &str = "feishu";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a feishu-client resource instance.
/// The WebSocket loop has its own cancel token stored in ComponentData.
pub struct FeishuClientHandle {
    pub client: Arc<open_lark::prelude::LarkClient>,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
    /// Resolved workload
    workload: Option<ResolvedWorkload>,
    /// Shared LarkClient (created in on_workload_resolved)
    client: Option<Arc<open_lark::prelude::LarkClient>>,
    /// Cancellation token for the WebSocket listener
    ws_cancel_token: Option<tokio_util::sync::CancellationToken>,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Feishu {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Feishu {
    fn default() -> Self {
        Self::new()
    }
}

impl Feishu {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::feishu::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — empty (resource lives in HostFeishuClient)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::feishu::sender::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

mod http;

async fn get_tenant_token(client: &open_lark::prelude::LarkClient) -> Result<String, FeishuError> {
    let token_manager = client.config.token_manager.lock().await;
    token_manager
        .get_tenant_access_token(&client.config, "", "", &client.config.app_ticket_manager)
        .await
        .map_err(|e| FeishuError::AuthFailed(e.to_string()))
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

/// Helper: get tenant token from a resource handle.
async fn token_from_handle(handle: &FeishuClientHandle) -> Result<String, FeishuError> {
    get_tenant_token(&handle.client).await
}

// ---------------------------------------------------------------------------
// Spawn the Feishu WebSocket client
// ---------------------------------------------------------------------------

fn spawn_feishu_ws(
    workload: ResolvedWorkload,
    component_id: String,
    client: Arc<open_lark::prelude::LarkClient>,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    use open_lark::prelude::*;

    let shared_config = Arc::new(client.config.clone());
    let workload = Arc::new(workload);
    let cid = component_id.clone();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!(
                    component_id = %component_id,
                    error = %e,
                    "Failed to build tokio runtime for Feishu WS"
                );
                return;
            }
        };

        let handler = {
            let workload = workload.clone();
            let cid = cid.clone();
            move |event: open_lark::service::im::v1::p2_im_message_receive_v1::P2ImMessageReceiveV1| {
                // Record metrics via global meter
                {
                    let meter = opentelemetry::global::meter("feishu");
                    let counter = meter
                        .u64_counter("feishu_messages_total")
                        .with_description("Total number of Feishu messages processed")
                        .build();
                    counter.add(1, &[opentelemetry::KeyValue::new("direction", "inbound")]);
                }

                let msg = &event.event.message;
                let sender = &event.event.sender;

                let text_content = if msg.message_type == "text" {
                    serde_json::from_str::<serde_json::Value>(&msg.content)
                        .ok()
                        .and_then(|v| {
                            v.get("text")
                                .and_then(|t| t.as_str())
                                .map(String::from)
                        })
                } else {
                    None
                };

                let im_msg = ImMessage {
                    message_id: msg.message_id.clone(),
                    chat_id: msg.chat_id.clone(),
                    sender_id: sender.sender_id.open_id.clone(),
                    sender_type: sender.sender_type.clone(),
                    message_type: msg.message_type.clone(),
                    text_content,
                    create_time: msg.create_time.clone(),
                    raw_json: serde_json::json!({
                        "message_id": msg.message_id,
                        "chat_id": msg.chat_id,
                        "message_type": msg.message_type,
                        "content": msg.content,
                        "sender_open_id": sender.sender_id.open_id,
                    })
                    .to_string(),
                };

                let workload = workload.clone();
                let cid = cid.clone();
                let client = client.clone();

                tokio::spawn(async move {
                    let mut store = match workload.new_store(&cid).await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to create store for Feishu callback");
                            return;
                        }
                    };

                    // Temporary client handle for the callback — guest can call methods on it
                    let temp_handle = FeishuClientHandle {
                        client,
                    };
                    let client_resource = match store.data_mut().table.push(temp_handle) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to push client resource for callback");
                            return;
                        }
                    };

                    let instance_pre = match workload.instantiate_pre(&cid).await {
                        Ok(pre) => pre,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to instantiate_pre for Feishu callback");
                            return;
                        }
                    };

                    let pre = match bindings::FeishuPre::new(instance_pre) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to create FeishuPre");
                            return;
                        }
                    };

                    let proxy = match pre.instantiate_async(&mut store).await {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to instantiate for Feishu callback");
                            return;
                        }
                    };

                    match proxy.custom_feishu_handler().call_on_message(&mut store, client_resource, &im_msg).await {
                        Ok(Ok(())) => {
                            debug!(component_id = %cid, "Guest on-message handled successfully");
                        }
                        Ok(Err(e)) => {
                            warn!(component_id = %cid, error = %e, "Guest on-message returned error");
                        }
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Guest on-message call failed");
                        }
                    }
                });
            }
        };

        let event_handler = match EventDispatcherHandler::builder()
            .register_p2_im_message_receive_v1(handler)
        {
            Ok(b) => b.build(),
            Err(e) => {
                warn!(component_id = %component_id, error = %e, "Failed to register IM message handler");
                return;
            }
        };

        rt.block_on(async {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(component_id = %component_id, "Feishu WebSocket task cancelled");
                }
                result = open_lark::client::ws_client::LarkWsClient::open(
                    shared_config,
                    event_handler,
                ) => {
                    match result {
                        Ok(()) => {
                            warn!(component_id = %component_id, "Feishu WebSocket client exited");
                        }
                        Err(e) => {
                            warn!(component_id = %component_id, error = %e, "Feishu WebSocket client error");
                        }
                    }
                }
            }
        });
    });
}

// ---------------------------------------------------------------------------
// WIT sender::HostFeishuClient — resource constructor + all methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::feishu::sender::HostFeishuClient for ActiveCtx<'a> {
    async fn new(
        &mut self,
        _config: Option<FeishuConfig>,
    ) -> wasmtime::Result<Resource<FeishuClientHandle>> {
        let Some(plugin) = self.get_plugin::<Feishu>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("feishu plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        let Some(client) = data.client.clone() else {
            return Err(wasmtime::Error::msg(
                "feishu client not started — WebSocket should be running from on_workload_resolved",
            ));
        };

        drop(lock);

        let handle = FeishuClientHandle { client };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    // -- IM --

    #[instrument(skip_all)]
    async fn send_text(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request = open_lark::service::im::v1::message::CreateMessageRequest::with_msg(
            &chat_id, msg, "chat_id",
        );
        match h.client.im.v1.message.create(request, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn send_text_to_user(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        receive_id: String,
        receive_id_type: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request = open_lark::service::im::v1::message::CreateMessageRequest::with_msg(
            &receive_id,
            msg,
            &receive_id_type,
        );
        match h.client.im.v1.message.create(request, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn reply_message(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        message_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let msg = open_lark::service::im::v1::message::MessageText::new(&content);
        let request =
            open_lark::service::im::v1::message::CreateMessageRequest::with_msg("", msg, "");
        match h
            .client
            .im
            .v1
            .message
            .reply(&message_id, request, None)
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::SendFailed(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn get_access_token(
        &mut self,
        handle: Resource<FeishuClientHandle>,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        Ok(token_from_handle(h).await)
    }

    // -- Contact --

    #[instrument(skip_all)]
    async fn get_user(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        user_id: String,
        user_id_type: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req = open_lark::service::contact::v3::user::GetUserRequest {
            user_id_type: Some(user_id_type),
            ..Default::default()
        };
        match h.client.contact.v3.user.get(&user_id, &req).await {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn batch_get_users(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::contact::v3::user::BatchGetUsersRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.contact.v3.user.batch(&req).await {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn search_users(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::contact::v3::user::SearchUsersRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.contact.v3.user.search(&req).await {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn list_department_users(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::contact::v3::user::FindUsersByDepartmentRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.contact.v3.user.find_by_department(&req).await {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn get_department(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        department_id: String,
        department_id_type: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req = open_lark::service::contact::v3::department::GetDepartmentRequest {
            user_id_type: Some("open_id".to_string()),
            department_id_type: Some(department_id_type),
        };
        match h
            .client
            .contact
            .v3
            .department
            .get(&department_id, &req)
            .await
        {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn list_sub_departments(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::contact::v3::department::GetChildrenDepartmentsRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.contact.v3.department.children(&req).await {
            Ok(resp) => {
                Ok(serde_json::to_string(&resp).map_err(|e| FeishuError::Internal(e.to_string())))
            }
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    // -- Group --

    #[instrument(skip_all)]
    async fn create_group(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/im/v1/chats",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn get_group(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}"),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn update_group(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_put(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}"),
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn add_group_members(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members"),
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn remove_group_members(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_delete(
            &token,
            &format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members"),
            Some(request_json),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn list_group_members(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        chat_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        let params: serde_json::Value =
            serde_json::from_str(&request_json).map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        let query = build_query_string(&params);
        let url = if query.is_empty() {
            format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members")
        } else {
            format!("https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members?{query}")
        };
        Ok(http::feishu_get(&token, &url).await)
    }

    // -- AI --

    #[instrument(skip_all)]
    async fn recognize_text(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/optical_char_recognition/v1/image/basic_recognize",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn translate(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/translation/v1/text/translate",
            request_json,
        )
        .await)
    }

    // -- Calendar --

    #[instrument(skip_all)]
    async fn list_calendars(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        let params: serde_json::Value =
            serde_json::from_str(&request_json).map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        let query = build_query_string(&params);
        let url = format!(
            "https://open.feishu.cn/open-apis/calendar/v4/calendars{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        Ok(http::feishu_get(&token, &url).await)
    }

    #[instrument(skip_all)]
    async fn create_calendar_event(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        calendar_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            &format!("https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events"),
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn list_calendar_events(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        calendar_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        let params: serde_json::Value =
            serde_json::from_str(&request_json).map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        let query = build_query_string(&params);
        let url = format!(
            "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        Ok(http::feishu_get(&token, &url).await)
    }

    #[instrument(skip_all)]
    async fn get_calendar_event(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        calendar_id: String,
        event_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}"
            ),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn delete_calendar_event(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        calendar_id: String,
        event_id: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_delete(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/calendar/v4/calendars/{calendar_id}/events/{event_id}"
            ),
            None,
        )
        .await)
    }

    // -- CardKit --

    #[instrument(skip_all)]
    async fn create_card(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/cardkit/v1/cards",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn update_card(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        card_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_patch(
            &token,
            &format!("https://open.feishu.cn/open-apis/cardkit/v1/cards/{card_id}"),
            request_json,
        )
        .await
        .map(|_| ()))
    }

    // -- Mail --

    #[instrument(skip_all)]
    async fn send_mail(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        user_mailbox_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages"
            ),
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn list_mails(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        user_mailbox_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        let params: serde_json::Value =
            serde_json::from_str(&request_json).map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        let query = build_query_string(&params);
        let url = format!(
            "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages{}",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            }
        );
        Ok(http::feishu_get(&token, &url).await)
    }

    #[instrument(skip_all)]
    async fn get_mail(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        user_mailbox_id: String,
        message_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/mail/v1/user_mailboxes/{user_mailbox_id}/messages/{message_id}"
            ),
        )
        .await)
    }

    // -- Task --

    #[instrument(skip_all)]
    async fn create_task(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::task::v2::task::CreateTaskRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.task.task.create(req, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn get_task(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        task_guid: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        match h.client.task.task.get(&task_guid, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn update_task(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        task_guid: String,
        request_json: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::task::v2::task::UpdateTaskRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.task.task.patch(&task_guid, req, None, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn delete_task(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        task_guid: String,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        let h = self.table.get(&handle)?;
        match h.client.task.task.delete(&task_guid, None, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn list_tasks(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
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
        match h
            .client
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

    #[instrument(skip_all)]
    async fn create_tasklist(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let req: open_lark::service::task::v2::tasklist::CreateTasklistRequest =
            match serde_json::from_str(&request_json) {
                Ok(r) => r,
                Err(e) => return Ok(Err(FeishuError::Internal(e.to_string()))),
            };
        match h.client.task.tasklist.create(req, None, None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    #[instrument(skip_all)]
    async fn list_tasklists(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
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
        match h
            .client
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

    // -- Bot --

    #[instrument(skip_all)]
    async fn get_bot_info(
        &mut self,
        handle: Resource<FeishuClientHandle>,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        match h.client.bot.v3.info.get(None).await {
            Ok(resp) => match serde_json::to_string(&resp.data) {
                Ok(json) => Ok(Ok(json)),
                Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
            },
            Err(e) => Ok(Err(FeishuError::Internal(e.to_string()))),
        }
    }

    // -- Docs --

    #[instrument(skip_all)]
    async fn create_document(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/docx/v1/documents",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn get_document(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        document_id: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}"),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn create_spreadsheet(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn get_spreadsheet(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        spreadsheet_token: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}"),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn create_bitable(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            "https://open.feishu.cn/open-apis/bitable/v1/apps",
            request_json,
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn list_bitable_tables(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        app_token: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_get(
            &token,
            &format!("https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"),
        )
        .await)
    }

    #[instrument(skip_all)]
    async fn list_bitable_records(
        &mut self,
        handle: Resource<FeishuClientHandle>,
        app_token: String,
        table_id: String,
        request_json: String,
    ) -> wasmtime::Result<Result<String, FeishuError>> {
        let h = self.table.get(&handle)?;
        let token = match token_from_handle(h).await {
            Ok(t) => t,
            Err(e) => return Ok(Err(e)),
        };
        Ok(http::feishu_post(
            &token,
            &format!(
                "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
            ),
            request_json,
        )
        .await)
    }

    // -- Lifecycle --

    async fn stop(
        &mut self,
        _handle: Resource<FeishuClientHandle>,
    ) -> wasmtime::Result<Result<(), FeishuError>> {
        debug!("Feishu client resource stop() — WebSocket listener unaffected");
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<FeishuClientHandle>) -> wasmtime::Result<()> {
        let _ = self.table.delete(rep);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for Feishu {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:feishu/handler@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:feishu/sender@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = find_interface(&interfaces, "custom", "feishu") else {
            return Ok(());
        };

        let interface_config = interface.config.clone();

        bindings::custom::feishu::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::feishu::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let wash_runtime::engine::workload::WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "Feishu plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                interface_config,
                workload: None,
                client: None,
                ws_cancel_token: None,
            },
        );

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let mut lock = self.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(component_id) {
            data.workload = Some(workload.clone());

            if data.client.is_none() {
                let app_id = match resolve_field(None, &data.interface_config, "app-id") {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            component_id,
                            error = %e,
                            "feishu: no app-id configured, skipping WebSocket start"
                        );
                        return Ok(());
                    }
                };
                let app_secret = match resolve_field(None, &data.interface_config, "app-secret") {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            component_id,
                            error = %e,
                            "feishu: no app-secret configured, skipping WebSocket start"
                        );
                        return Ok(());
                    }
                };

                use open_lark::prelude::*;

                let cancel_token = tokio_util::sync::CancellationToken::new();
                let client = Arc::new(
                    LarkClient::builder(&app_id, &app_secret)
                        .with_app_type(AppType::SelfBuild)
                        .with_enable_token_cache(true)
                        .build(),
                );

                spawn_feishu_ws(
                    workload.clone(),
                    component_id.to_string(),
                    client.clone(),
                    cancel_token.clone(),
                );

                debug!(
                    component_id,
                    "Feishu WebSocket started from on_workload_resolved"
                );
                data.client = Some(client);
                data.ws_cancel_token = Some(cancel_token);
            }
        }
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                |data| async move {
                    if let Some(token) = data.ws_cancel_token.as_ref() {
                        token.cancel();
                        debug!(workload_id, "Feishu WebSocket cancelled on unbind");
                    }
                },
            )
            .await;
        debug!(workload_id = %workload_id, "Feishu plugin unbound");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = Feishu::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = Feishu::new();
        let world = plugin.world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "feishu")
        );
    }

    #[test]
    fn test_world_exports() {
        let plugin = Feishu::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "feishu")
        );
    }

    #[test]
    fn test_build_query_string() {
        let params: serde_json::Value = serde_json::json!({"page_size": "10", "page_token": "abc"});
        let query = build_query_string(&params);
        assert!(query.contains("page_size=10"));
        assert!(query.contains("page_token=abc"));
    }

    #[test]
    fn test_build_query_string_empty() {
        let params: serde_json::Value = serde_json::json!({});
        assert!(build_query_string(&params).is_empty());
    }
}
