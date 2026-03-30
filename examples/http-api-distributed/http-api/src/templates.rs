pub static BASE_CSS: &str = include_str!("../resources/base.css");
pub static BASE_JS: &str = include_str!("../resources/base.js");
pub static NAV_HTML: &str = include_str!("../resources/nav.html");

/// Load an HTML page from resources and replace `{BASE_CSS}`, `{BASE_JS}`, and `{NAV}` placeholders.
pub fn render(html: &str) -> String {
    html
        .replace("{BASE_CSS}", BASE_CSS)
        .replace("{BASE_JS}", BASE_JS)
        .replace("{NAV}", NAV_HTML)
}
