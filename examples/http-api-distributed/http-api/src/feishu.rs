use crate::bindings::custom::feishu::sender::FeishuClient;
use crate::bindings::custom::feishu::types::FeishuError;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::{LOG_CTX, helpers, templates};
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const FEISHU_HTML: &str = include_str!("../resources/feishu.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(FEISHU_HTML))
}

// ============ Error helper ============

fn feishu_error(status: StatusCode, e: FeishuError) -> anyhow::Result<Response<Body>> {
    let msg = match e {
        FeishuError::Internal(s) => format!("Internal: {s}"),
        FeishuError::AuthFailed(s) => format!("Auth failed: {s}"),
        FeishuError::SendFailed(s) => format!("Send failed: {s}"),
    };
    log(Level::Error, LOG_CTX, &format!("FEISHU ERROR: {}", msg));
    helpers::json_error(status, &msg)
}

// ============ IM ============

#[derive(Deserialize)]
struct ImSendTextRequest {
    chat_id: String,
    content: String,
}

pub async fn im_send_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ImSendTextRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU IM SEND TEXT: chat={}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.send_text(&body.chat_id, &body.content) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "FEISHU IM SEND TEXT OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct ImSendTextToUserRequest {
    receive_id: String,
    receive_id_type: String,
    content: String,
}

pub async fn im_send_text_to_user(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ImSendTextToUserRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU IM SEND TEXT TO USER: recv={}", body.receive_id),
    );
    let client = FeishuClient::new(None);
    match client.send_text_to_user(&body.receive_id, &body.receive_id_type, &body.content) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "FEISHU IM SEND TEXT TO USER OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct ImReplyMessageRequest {
    message_id: String,
    content: String,
}

pub async fn im_reply_message(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ImReplyMessageRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU IM REPLY: msg={}", body.message_id),
    );
    let client = FeishuClient::new(None);
    match client.reply_message(&body.message_id, &body.content) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "FEISHU IM REPLY OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn im_get_access_token(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Info, LOG_CTX, "FEISHU IM GET ACCESS TOKEN");
    let client = FeishuClient::new(None);
    match client.get_access_token() {
        Ok(token) => {
            log(Level::Info, LOG_CTX, "FEISHU IM GET ACCESS TOKEN OK");
            helpers::json_response(serde_json::json!({ "token": token }).to_string())
        }
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Contact ============

#[derive(Deserialize)]
struct ContactGetUserRequest {
    user_id: String,
    user_id_type: String,
}

pub async fn contact_get_user(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ContactGetUserRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU CONTACT GET USER: {}", body.user_id),
    );
    let client = FeishuClient::new(None);
    match client.get_user(&body.user_id, &body.user_id_type) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct JsonRequest {
    request_json: String,
}

pub async fn contact_batch_get_users(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CONTACT BATCH GET USERS");
    let client = FeishuClient::new(None);
    match client.batch_get_users(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn contact_search_users(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CONTACT SEARCH USERS");
    let client = FeishuClient::new(None);
    match client.search_users(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn contact_list_department_users(
    mut req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CONTACT LIST DEPARTMENT USERS");
    let client = FeishuClient::new(None);
    match client.list_department_users(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct ContactGetDeptRequest {
    department_id: String,
    department_id_type: String,
}

pub async fn contact_get_department(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ContactGetDeptRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU CONTACT GET DEPT: {}", body.department_id),
    );
    let client = FeishuClient::new(None);
    match client.get_department(&body.department_id, &body.department_id_type) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn contact_list_sub_departments(
    mut req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CONTACT LIST SUB DEPARTMENTS");
    let client = FeishuClient::new(None);
    match client.list_sub_departments(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Group ============

pub async fn group_create_group(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU GROUP CREATE");
    let client = FeishuClient::new(None);
    match client.create_group(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct ChatIdRequest {
    chat_id: String,
}

pub async fn group_get_group(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ChatIdRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU GROUP GET: {}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.get_group(&body.chat_id) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct GroupWithChatId {
    chat_id: String,
    request_json: String,
}

pub async fn group_update_group(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: GroupWithChatId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU GROUP UPDATE: {}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.update_group(&body.chat_id, &body.request_json) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn group_add_group_members(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: GroupWithChatId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU GROUP ADD MEMBERS: {}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.add_group_members(&body.chat_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn group_remove_group_members(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: GroupWithChatId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU GROUP REMOVE MEMBERS: {}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.remove_group_members(&body.chat_id, &body.request_json) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn group_list_group_members(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: GroupWithChatId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU GROUP LIST MEMBERS: {}", body.chat_id),
    );
    let client = FeishuClient::new(None);
    match client.list_group_members(&body.chat_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ AI ============

pub async fn ai_recognize_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU AI RECOGNIZE TEXT");
    let client = FeishuClient::new(None);
    match client.recognize_text(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn ai_translate(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU AI TRANSLATE");
    let client = FeishuClient::new(None);
    match client.translate(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Calendar ============

pub async fn calendar_list_calendars(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CALENDAR LIST");
    let client = FeishuClient::new(None);
    match client.list_calendars(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct CalendarWithId {
    calendar_id: String,
    request_json: String,
}

pub async fn calendar_create_calendar_event(
    mut req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body: CalendarWithId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU CALENDAR CREATE EVENT: {}", body.calendar_id),
    );
    let client = FeishuClient::new(None);
    match client.create_calendar_event(&body.calendar_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn calendar_list_calendar_events(
    mut req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body: CalendarWithId = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU CALENDAR LIST EVENTS: {}", body.calendar_id),
    );
    let client = FeishuClient::new(None);
    match client.list_calendar_events(&body.calendar_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct CalendarEventRequest {
    calendar_id: String,
    event_id: String,
}

pub async fn calendar_get_calendar_event(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: CalendarEventRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "FEISHU CALENDAR GET EVENT: {}/{}",
            body.calendar_id, body.event_id
        ),
    );
    let client = FeishuClient::new(None);
    match client.get_calendar_event(&body.calendar_id, &body.event_id) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn calendar_delete_calendar_event(
    mut req: Request<Body>,
) -> anyhow::Result<Response<Body>> {
    let body: CalendarEventRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "FEISHU CALENDAR DELETE EVENT: {}/{}",
            body.calendar_id, body.event_id
        ),
    );
    let client = FeishuClient::new(None);
    match client.delete_calendar_event(&body.calendar_id, &body.event_id) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ CardKit ============

pub async fn cardkit_create_card(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU CARDKIT CREATE CARD");
    let client = FeishuClient::new(None);
    match client.create_card(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct CardIdRequest {
    card_id: String,
    request_json: String,
}

pub async fn cardkit_update_card(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: CardIdRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU CARDKIT UPDATE: {}", body.card_id),
    );
    let client = FeishuClient::new(None);
    match client.update_card(&body.card_id, &body.request_json) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Mail ============

#[derive(Deserialize)]
struct MailboxRequest {
    user_mailbox_id: String,
    request_json: String,
}

pub async fn mail_send_mail(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: MailboxRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU MAIL SEND: mailbox={}", body.user_mailbox_id),
    );
    let client = FeishuClient::new(None);
    match client.send_mail(&body.user_mailbox_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn mail_list_mails(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: MailboxRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU MAIL LIST: mailbox={}", body.user_mailbox_id),
    );
    let client = FeishuClient::new(None);
    match client.list_mails(&body.user_mailbox_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct MailGetRequest {
    user_mailbox_id: String,
    message_id: String,
}

pub async fn mail_get_mail(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: MailGetRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU MAIL GET: mailbox={}", body.user_mailbox_id),
    );
    let client = FeishuClient::new(None);
    match client.get_mail(&body.user_mailbox_id, &body.message_id) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Task ============

pub async fn task_create_task(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU TASK CREATE");
    let client = FeishuClient::new(None);
    match client.create_task(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct TaskGuidRequest {
    task_guid: String,
}

pub async fn task_get_task(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: TaskGuidRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU TASK GET: {}", body.task_guid),
    );
    let client = FeishuClient::new(None);
    match client.get_task(&body.task_guid) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct TaskUpdateRequest {
    task_guid: String,
    request_json: String,
}

pub async fn task_update_task(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: TaskUpdateRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU TASK UPDATE: {}", body.task_guid),
    );
    let client = FeishuClient::new(None);
    match client.update_task(&body.task_guid, &body.request_json) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn task_delete_task(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: TaskGuidRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU TASK DELETE: {}", body.task_guid),
    );
    let client = FeishuClient::new(None);
    match client.delete_task(&body.task_guid) {
        Ok(()) => helpers::json_response("{\"ok\":true}"),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn task_list_tasks(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU TASK LIST");
    let client = FeishuClient::new(None);
    match client.list_tasks(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn task_create_tasklist(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU TASK CREATE TASKLIST");
    let client = FeishuClient::new(None);
    match client.create_tasklist(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn task_list_tasklists(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU TASK LIST TASKLISTS");
    let client = FeishuClient::new(None);
    match client.list_tasklists(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Bot ============

pub async fn bot_get_bot_info(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Info, LOG_CTX, "FEISHU BOT GET INFO");
    let client = FeishuClient::new(None);
    match client.get_bot_info() {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ Docs ============

pub async fn docs_create_document(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU DOCS CREATE DOCUMENT");
    let client = FeishuClient::new(None);
    match client.create_document(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct DocumentIdRequest {
    document_id: String,
}

pub async fn docs_get_document(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: DocumentIdRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU DOCS GET DOCUMENT: {}", body.document_id),
    );
    let client = FeishuClient::new(None);
    match client.get_document(&body.document_id) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn docs_create_spreadsheet(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU DOCS CREATE SPREADSHEET");
    let client = FeishuClient::new(None);
    match client.create_spreadsheet(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct SpreadsheetTokenRequest {
    spreadsheet_token: String,
}

pub async fn docs_get_spreadsheet(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SpreadsheetTokenRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU DOCS GET SPREADSHEET: {}", body.spreadsheet_token),
    );
    let client = FeishuClient::new(None);
    match client.get_spreadsheet(&body.spreadsheet_token) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn docs_create_bitable(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: JsonRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "FEISHU DOCS CREATE BITABLE");
    let client = FeishuClient::new(None);
    match client.create_bitable(&body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct AppTokenRequest {
    app_token: String,
}

pub async fn docs_list_bitable_tables(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: AppTokenRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("FEISHU DOCS LIST BITABLE TABLES: {}", body.app_token),
    );
    let client = FeishuClient::new(None);
    match client.list_bitable_tables(&body.app_token) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct BitableRecordsRequest {
    app_token: String,
    table_id: String,
    request_json: String,
}

pub async fn docs_list_bitable_records(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: BitableRecordsRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "FEISHU DOCS LIST BITABLE RECORDS: {}/{}",
            body.app_token, body.table_id
        ),
    );
    let client = FeishuClient::new(None);
    match client.list_bitable_records(&body.app_token, &body.table_id, &body.request_json) {
        Ok(json) => helpers::json_response(json),
        Err(e) => feishu_error(StatusCode::BAD_GATEWAY, e),
    }
}
