/* 公共工具：API 封装、Toast、复制、格式化、主题、SSE */
const App = (() => {

  function token() { return localStorage.getItem('admin_token') || ''; }

  async function api(path, opts = {}) {
    const headers = Object.assign({}, opts.headers || {});
    if (!headers['Content-Type'] && !(opts.body instanceof FormData)) headers['Content-Type'] = 'application/json';
    const t = token();
    if (t) headers['Authorization'] = 'Bearer ' + t;
    const resp = await fetch(path, Object.assign({}, opts, { headers }));
    if (resp.status === 401) {
      const p = prompt('管理后台已启用密码保护，请输入管理密码：');
      if (p !== null) {
        localStorage.setItem('admin_token', p);
        return api(path, opts);
      }
      throw new Error('未授权');
    }
    const ct = resp.headers.get('content-type') || '';
    const data = ct.includes('json') ? await resp.json() : await resp.text();
    if (!resp.ok) {
      let msg = '请求失败（HTTP ' + resp.status + '）';
      if (data && data.error && data.error.message) msg = data.error.message;
      else if (data && typeof data.detail === 'string') msg = data.detail;
      throw new Error(msg);
    }
    return data;
  }

  function toast(msg, type = 'success') {
    let box = document.getElementById('toast-box');
    if (!box) {
      box = document.createElement('div');
      box.id = 'toast-box';
      document.body.appendChild(box);
    }
    const el = document.createElement('div');
    el.className = 'toast ' + (type === 'success' ? '' : type);
    el.textContent = msg;
    box.appendChild(el);
    setTimeout(() => el.remove(), 3200);
  }

  async function copy(text) {
    try {
      await navigator.clipboard.writeText(text);
      toast('已复制到剪贴板');
    } catch (_) {
      const ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
      toast('已复制到剪贴板');
    }
  }

  function maskEmail(email) {
    if (!email) return '—';
    const at = email.indexOf('@');
    if (at < 0) return email.slice(0, 2) + '***';
    const local = email.slice(0, at);
    return (local.length >= 2 ? local.slice(0, 2) : local) + '***@' + email.slice(at + 1);
  }

  function fmtTokens(n) {
    n = Number(n) || 0;
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function fmtDuration(sec) {
    sec = Math.max(0, Math.floor(sec));
    const d = Math.floor(sec / 86400);
    const h = String(Math.floor((sec % 86400) / 3600)).padStart(2, '0');
    const m = String(Math.floor((sec % 3600) / 60)).padStart(2, '0');
    const s = String(sec % 60).padStart(2, '0');
    return (d > 0 ? d + '天 ' : '') + h + ':' + m + ':' + s;
  }

  function fmtTime(ts) {
    if (!ts) return '—';
    return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false });
  }

  function timeAgo(ts) {
    if (!ts) return '—';
    const diff = Math.floor(Date.now() / 1000 - ts);
    if (diff < 5) return '刚刚';
    if (diff < 60) return diff + ' 秒前';
    if (diff < 3600) return Math.floor(diff / 60) + ' 分钟前';
    if (diff < 86400) return Math.floor(diff / 3600) + ' 小时前';
    return Math.floor(diff / 86400) + ' 天前';
  }

  function applyTheme(theme) {
    document.body.classList.toggle('light', theme === 'light');
    localStorage.setItem('theme', theme);
  }

  function initTheme() {
    applyTheme(localStorage.getItem('theme') || 'dark');
  }

  function toggleTheme() {
    const next = document.body.classList.contains('light') ? 'dark' : 'light';
    applyTheme(next);
    return next;
  }

  function connectSSE(onData) {
    const t = token();
    const url = '/api/events' + (t ? ('?token=' + encodeURIComponent(t)) : '');
    const es = new EventSource(url);
    es.onmessage = (e) => {
      try { onData(JSON.parse(e.data)); } catch (_) { /* 忽略坏帧 */ }
    };
    return es;
  }

  function escapeHtml(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g,
      (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function downloadText(filename, text) {
    const blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  return {
    api, toast, copy, maskEmail, fmtTokens, fmtDuration, fmtTime, timeAgo,
    applyTheme, initTheme, toggleTheme, connectSSE, escapeHtml, downloadText, token,
  };
})();

document.addEventListener('DOMContentLoaded', () => App.initTheme());