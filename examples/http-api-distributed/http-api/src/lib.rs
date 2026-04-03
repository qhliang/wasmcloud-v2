mod bindings {
    wit_bindgen::generate!({
        path: "../wit",
        world: "http-api",
        generate_all,
    });

    use super::CustomHandler;

    export!(CustomHandler);
}

mod crontab;
mod d1;
mod dingtalk;
mod feishu;
mod helpers;
mod kv;
mod llm;
mod r2;
mod task;
mod mail;
mod templates;

use bindings::wasi::logging::logging::{Level, log};
use wstd::http::{Body, Request, Response, StatusCode};

const LOG_CTX: &str = "http-api";

static HOME_HTML: &str = include_str!("../resources/home.html");

struct CustomHandler;

impl bindings::exports::custom::crontab::handler::Guest for CustomHandler {
    fn handle_tick(name: String) -> Result<(), String> {
        let message = format!(
            "CRONTAB TICK: schedule '{}' fired via handle-tick export",
            name
        );
        log(Level::Info, LOG_CTX, &message);
        crontab::push_callback(message);
        Ok(())
    }
}

impl bindings::exports::custom::feishu::handler::Guest for CustomHandler {
    fn on_message(
        msg: bindings::exports::custom::feishu::handler::ImMessage,
    ) -> Result<(), String> {
        log(
            Level::Info,
            LOG_CTX,
            &format!("Received Feishu message: {:?}", msg),
        );
        Ok(())
    }
}

impl bindings::exports::custom::dingtalk_stream::handler::Guest for CustomHandler {
    fn on_message(
        msg: bindings::exports::custom::dingtalk_stream::handler::ChatbotMessage,
    ) -> Result<(), String> {
        log(
            Level::Info,
            LOG_CTX,
            &format!("Received DingTalk message: {:?}", msg),
        );
        Ok(())
    }
}

#[wstd::http_server]
async fn main(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let path = req.uri().path();
    log(
        Level::Debug,
        LOG_CTX,
        &format!("Request: {} {}", req.method(), path),
    );

    let result = match path {
        "/" => helpers::html_response(templates::render(HOME_HTML)),
        "/task" => task::create(req).await,
        "/kv" | "/kv/" => kv::home(req).await,
        "/kv/get" => kv::get(req).await,
        "/kv/set" => kv::set(req).await,
        "/kv/delete" => kv::delete(req).await,
        "/kv/keys" => kv::keys(req).await,
        "/d1" | "/d1/" => d1::home(req).await,
        "/d1/query" => d1::query(req).await,
        "/d1/batch" => d1::batch(req).await,
        "/r2" | "/r2/" => r2::home(req).await,
        "/r2/containers" => r2::containers(req).await,
        "/r2/container/create" => r2::container_create(req).await,
        "/r2/container/delete" => r2::container_delete(req).await,
        "/r2/objects" => r2::objects(req).await,
        "/r2/object/get" => r2::object_get(req).await,
        "/r2/object/put" => r2::object_put(req).await,
        "/r2/object/delete" => r2::object_delete(req).await,
        "/llm" | "/llm/" => llm::home(req).await,
        "/llm/chat" => llm::chat(req).await,
        "/crontab" | "/crontab/" => crontab::home(req).await,
        "/crontab/schedule" => crontab::schedule(req).await,
        "/crontab/schedule-delay" => crontab::schedule_delay(req).await,
        "/crontab/remove" => crontab::remove(req).await,
        "/crontab/list" => crontab::list(req).await,
        "/crontab/callback" => crontab::callback(req).await,
        "/crontab/callbacks" => crontab::callbacks(req).await,
        "/dingtalk" | "/dingtalk/" => dingtalk::home(req).await,
        "/dingtalk/send-text" => dingtalk::send_text(req).await,
        "/dingtalk/send-markdown" => dingtalk::send_markdown(req).await,
        "/dingtalk/send-oto-text" => dingtalk::send_oto_text(req).await,
        "/dingtalk/get-access-token" => dingtalk::get_access_token(req).await,
        "/feishu" | "/feishu/" => feishu::home(req).await,
        "/feishu/im/send-text" => feishu::im_send_text(req).await,
        "/feishu/im/send-text-to-user" => feishu::im_send_text_to_user(req).await,
        "/feishu/im/reply-message" => feishu::im_reply_message(req).await,
        "/feishu/im/get-access-token" => feishu::im_get_access_token(req).await,
        "/feishu/contact/get-user" => feishu::contact_get_user(req).await,
        "/feishu/contact/batch-get-users" => feishu::contact_batch_get_users(req).await,
        "/feishu/contact/search-users" => feishu::contact_search_users(req).await,
        "/feishu/contact/list-department-users" => feishu::contact_list_department_users(req).await,
        "/feishu/contact/get-department" => feishu::contact_get_department(req).await,
        "/feishu/contact/list-sub-departments" => feishu::contact_list_sub_departments(req).await,
        "/feishu/group/create-group" => feishu::group_create_group(req).await,
        "/feishu/group/get-group" => feishu::group_get_group(req).await,
        "/feishu/group/update-group" => feishu::group_update_group(req).await,
        "/feishu/group/add-group-members" => feishu::group_add_group_members(req).await,
        "/feishu/group/remove-group-members" => feishu::group_remove_group_members(req).await,
        "/feishu/group/list-group-members" => feishu::group_list_group_members(req).await,
        "/feishu/ai/recognize-text" => feishu::ai_recognize_text(req).await,
        "/feishu/ai/translate" => feishu::ai_translate(req).await,
        "/feishu/calendar/list-calendars" => feishu::calendar_list_calendars(req).await,
        "/feishu/calendar/create-calendar-event" => {
            feishu::calendar_create_calendar_event(req).await
        }
        "/feishu/calendar/list-calendar-events" => feishu::calendar_list_calendar_events(req).await,
        "/feishu/calendar/get-calendar-event" => feishu::calendar_get_calendar_event(req).await,
        "/feishu/calendar/delete-calendar-event" => {
            feishu::calendar_delete_calendar_event(req).await
        }
        "/feishu/cardkit/create-card" => feishu::cardkit_create_card(req).await,
        "/feishu/cardkit/update-card" => feishu::cardkit_update_card(req).await,
        "/feishu/mail/send-mail" => feishu::mail_send_mail(req).await,
        "/feishu/mail/list-mails" => feishu::mail_list_mails(req).await,
        "/feishu/mail/get-mail" => feishu::mail_get_mail(req).await,
        "/feishu/task/create-task" => feishu::task_create_task(req).await,
        "/feishu/task/get-task" => feishu::task_get_task(req).await,
        "/feishu/task/update-task" => feishu::task_update_task(req).await,
        "/feishu/task/delete-task" => feishu::task_delete_task(req).await,
        "/feishu/task/list-tasks" => feishu::task_list_tasks(req).await,
        "/feishu/task/create-tasklist" => feishu::task_create_tasklist(req).await,
        "/feishu/task/list-tasklists" => feishu::task_list_tasklists(req).await,
        "/feishu/bot/get-bot-info" => feishu::bot_get_bot_info(req).await,
        "/feishu/docs/create-document" => feishu::docs_create_document(req).await,
        "/feishu/docs/get-document" => feishu::docs_get_document(req).await,
        "/feishu/docs/create-spreadsheet" => feishu::docs_create_spreadsheet(req).await,
        "/feishu/docs/get-spreadsheet" => feishu::docs_get_spreadsheet(req).await,
        "/feishu/docs/create-bitable" => feishu::docs_create_bitable(req).await,
        "/feishu/docs/list-bitable-tables" => feishu::docs_list_bitable_tables(req).await,
        "/feishu/docs/list-bitable-records" => feishu::docs_list_bitable_records(req).await,
        "/mail" | "/mail/" => mail::home(req).await,
        "/mail/send" => mail::send_mail(req).await,
        "/mail/list" => mail::list_mails(req).await,
        "/mail/get" => mail::get_mail(req).await,
        _ => {
            log(Level::Debug, LOG_CTX, &format!("Not found: {}", path));
            helpers::text_response(StatusCode::NOT_FOUND, "Not found\n")
        }
    };

    match &result {
        Ok(resp) => log(
            Level::Debug,
            LOG_CTX,
            &format!("Response: {}", resp.status()),
        ),
        Err(e) => log(Level::Error, LOG_CTX, &format!("Error: {}", e)),
    }
    result
}
