use std::sync::LazyLock;

use regex::Regex;

pub static BASE_CSS: &str = include_str!("../resources/base.css");
pub static BASE_JS: &str = include_str!("../resources/base.js");
pub static NAV_HTML: &str = include_str!("../resources/nav.html");

static CSS_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\s*BASE_CSS\s*;?\s*\}").unwrap());
static JS_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\s*BASE_JS\s*;?\s*\}").unwrap());
static NAV_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\s*NAV\s*;?\s*\}").unwrap());

/// Load an HTML page from resources and replace `{BASE_CSS}`, `{BASE_JS}`, and `{NAV}` placeholders.
///
/// Placeholders may appear as:
/// - `{BASE_CSS}` — single token
/// - `{ BASE_CSS ; }` — multiline with optional semicolon and whitespace
pub fn render(html: &str) -> String {
    let css: &str = BASE_CSS;
    let js: &str = BASE_JS;
    let nav: &str = NAV_HTML;
    let out = CSS_RE.replace_all(html, css);
    let out = JS_RE.replace_all(&out, js);
    NAV_RE.replace_all(&out, nav).to_string()
}
