use crate::bindings::custom::feishu::types::FeishuError;

pub(crate) async fn feishu_post(
    token: &str,
    url: &str,
    body: String,
) -> Result<String, FeishuError> {
    let resp = reqwest::Client::new()
        .post(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))?;
    resp.text()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))
}

pub(crate) async fn feishu_get(token: &str, url: &str) -> Result<String, FeishuError> {
    let resp = reqwest::Client::new()
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))?;
    resp.text()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))
}

pub(crate) async fn feishu_patch(
    token: &str,
    url: &str,
    body: String,
) -> Result<String, FeishuError> {
    let resp = reqwest::Client::new()
        .patch(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))?;
    resp.text()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))
}

pub(crate) async fn feishu_delete(
    token: &str,
    url: &str,
    body: Option<String>,
) -> Result<(), FeishuError> {
    let mut req = reqwest::Client::new()
        .delete(url)
        .header("Authorization", format!("Bearer {token}"));
    if let Some(b) = body {
        req = req.header("Content-Type", "application/json").body(b);
    }
    req.send()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))?;
    Ok(())
}

pub(crate) async fn feishu_put(
    token: &str,
    url: &str,
    body: String,
) -> Result<(), FeishuError> {
    reqwest::Client::new()
        .put(url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| FeishuError::Internal(e.to_string()))?;
    Ok(())
}
