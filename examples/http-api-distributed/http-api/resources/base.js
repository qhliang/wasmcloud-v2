function showResult(id, msg, isError) {
    var el = document.getElementById(id);
    el.textContent = msg;
    el.className = 'result ' + (isError ? 'error' : 'success');
    el.style.display = 'block';
}
function escapeHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
