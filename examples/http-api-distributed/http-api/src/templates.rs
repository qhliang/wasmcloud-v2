pub static BASE_CSS: &str = include_str!("../resources/base.css");
pub static BASE_JS: &str = include_str!("../resources/base.js");
pub static NAV_HTML: &str = include_str!("../resources/nav.html");

/// Load an HTML page from resources and replace `{BASE_CSS}`, `{BASE_JS}`, and `{NAV}` placeholders.
///
/// Placeholders may appear as:
/// - `{BASE_CSS}` — single token
/// - `{ BASE_CSS ; }` — multiline with optional semicolon and whitespace
pub fn render(html: &str) -> String {
    let mut out = html.to_string();
    let replacements: &[(&str, &str)] = &[
        ("BASE_CSS", BASE_CSS),
        ("BASE_JS", BASE_JS),
        ("NAV", NAV_HTML),
    ];
    for (needle, replacement) in replacements {
        // Match `{NEEDLE}`, `{ NEEDLE }`, `{ NEEDLE ; }` including multiline
        let pattern = format!(r"\{{\s*{needle}\s*;?\s*\}}");
        if let Ok(re) = regex::Regex::new(&pattern) {
            out = re.replace_all(&out, *replacement).to_string();
        }
    }
    out
}
