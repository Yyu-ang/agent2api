//! TeleAgent 人工确认管理 API（protected router，与账号配置相同鉴权边界）。
//!
//! ── 额外的同源校验 ─────────────────────────────────────────
//! 「没配 API Key、也没开面板认证」时 protected 组全部放行，而面板响应带
//! `Access-Control-Allow-Origin: *`：用户浏览器里任意网页都能读待确认列表、
//! 替用户批准本机 agent 执行命令。这里的后果比普通配置接口严重，所以列表与
//! 回复两个接口一律拒绝跨源浏览器请求（`Origin` 与 `Host` 不一致，或
//! `Sec-Fetch-Site: cross-site`）。确认页本身同源加载，不受影响；脚本 / curl
//! 不带 `Origin`，照常按原有鉴权放行。
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{Html, Response};
use serde_json::Value;

use crate::server::core::providers::teleagent::confirmations;
use crate::server::errors;
use crate::server::http::{ok_json, parse_body};
use crate::server::ServerState;

/// 浏览器跨源请求（含 `Origin: null`）→ true。
pub(crate) fn is_cross_origin(headers: &HeaderMap) -> bool {
    let header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok()).unwrap_or("");
    if header("sec-fetch-site").eq_ignore_ascii_case("cross-site") {
        return true;
    }
    let origin = header("origin").trim();
    if origin.is_empty() {
        return false;
    }
    let host = header("host").trim();
    let origin_host = origin.split_once("://").map(|(_, rest)| rest).unwrap_or("");
    host.is_empty() || origin_host.is_empty() || !origin_host.eq_ignore_ascii_case(host)
}

fn reject_cross_origin() -> Response {
    errors::management_error(403, "确认接口只接受同源请求（请在网关自身的确认页或脚本里操作）")
}

/// GET /api/teleagent/confirmations
pub async fn list(State(_state): State<ServerState>, headers: HeaderMap) -> Response {
    if is_cross_origin(&headers) {
        return reject_cross_origin();
    }
    ok_json(confirmations::list())
}

/// POST /api/teleagent/confirmations/reply
/// {"session_id":"...", "id":"...", "reply":"once|always|reject"}
/// 或 {"session_id":"...", "id":"...", "answers":["..."]}
pub async fn reply(State(_state): State<ServerState>, headers: HeaderMap, body: Bytes) -> Response {
    if is_cross_origin(&headers) {
        return reject_cross_origin();
    }
    let payload = match parse_body(&body) {
        Ok(value) => value,
        Err(error) => return errors::management_error(400, error.message),
    };
    let field = |name: &str| payload.get(name).and_then(Value::as_str).unwrap_or("").trim();
    let (session_id, id) = (field("session_id"), field("id"));
    if session_id.is_empty() || id.is_empty() {
        return errors::management_error(400, "缺少 session_id 或 id");
    }
    match confirmations::reply(session_id, id, &payload).await {
        Ok(()) => ok_json(serde_json::json!({"success": true})),
        Err(error) => errors::management_error(error.status_code, error.message),
    }
}

/// 一个独立可访问的管理页：只在受管理鉴权保护的路由上提供，不公开待批准内容。
pub async fn page(State(_state): State<ServerState>) -> Html<&'static str> {
    Html(PAGE)
}

const PAGE: &str = r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>TeleAgent 人工确认</title>
<style>
body{font:15px system-ui,sans-serif;background:#111827;color:#f3f4f6;margin:auto;max-width:900px;padding:24px}
h1{font-size:24px}button,input{font:inherit;border-radius:7px;padding:8px}
input{background:#1f2937;color:#fff;border:1px solid #4b5563;width:250px}
button{cursor:pointer;background:#374151;color:white;border:1px solid #6b7280}
button:focus{outline:2px solid #93c5fd}
article{background:#1f2937;padding:16px;border-radius:10px;margin:12px 0}
small{color:#9ca3af}p{white-space:pre-wrap;overflow-wrap:anywhere}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}label{display:block;margin-top:8px}
</style></head><body>
<h1>TeleAgent 人工确认</h1>
<p>仅显示上游明确要求授权或回答的问题。权限不会自动批准。</p>
<label>管理 API Key（如面板会话已登录则可留空） <input id="key" type="password" autocomplete="off"></label>
<div class="controls"><button id="refresh">刷新待处理事项</button></div>
<p id="status" role="status"></p><main id="pending"></main>
<script>
const root=document.getElementById('pending'), status=document.getElementById('status');
const key=document.getElementById('key');
const headers=()=>key.value?{'Authorization':'Bearer '+key.value}:{};
async function call(url,method='GET',body) {
  const h=headers();if(body)h['Content-Type']='application/json';
  const r=await fetch(url,{method,headers:h,body:body?JSON.stringify(body):undefined,credentials:'same-origin'});
  const raw=await r.text();let data;try{data=JSON.parse(raw)}catch{throw Error('HTTP '+r.status+': '+raw.slice(0,200))}
  if(!r.ok)throw Error('HTTP '+r.status+': '+JSON.stringify(data).slice(0,350));
  return data;
}
function add(parent,tag,value){const node=document.createElement(tag);node.textContent=value;parent.appendChild(node);return node;}
function button(parent,title,handler){const b=add(parent,'button',title);b.onclick=handler;return b;}
async function reply(item,payload){try{
  status.textContent='正在回复上游…';
  await call('/api/teleagent/confirmations/reply','POST',{session_id:item.session_id,id:item.id,...payload});
  status.textContent='上游已接受回复；原会话正在继续。';await refresh();
}catch(e){status.textContent=String(e)}}
async function refresh(){
  try {
    const data=await call('/api/teleagent/confirmations');
    root.replaceChildren();const items=data.pending||data.data?.pending||[];
    if(!items.length)add(root,'p','当前没有待处理的请求');
    for(const item of items){
      const box=add(root,'article','');
      add(box,'small',item.type+' · '+item.session_id+' · '+item.id);
      add(box,'p',item.description||'上游未提供说明');
      if(item.tool)add(box,'small','工具：'+item.tool);
      const controls=add(box,'div','');controls.className='controls';
      if(item.type==='permission'){
        button(controls,'允许一次',()=>reply(item,{reply:'once'}));
        button(controls,'始终允许',()=>{if(confirm('确定对该类型权限始终允许？'))reply(item,{reply:'always'})});
        button(controls,'拒绝',()=>reply(item,{reply:'reject'}));
      }else{
        if(Array.isArray(item.questions)){for(const q of item.questions){
          add(box,'p',q.question||q.header||'选择答案');
          if(Array.isArray(q.options)){for(const opt of q.options){
            if(opt.label)button(controls,opt.label,()=>reply(item,{answers:[opt.label]}));
          }}
        }}
        const answer=add(box,'input','');answer.placeholder='填写回复';
        button(controls,'提交回答',()=>reply(item,{answers:[answer.value]}));
      }
    }
    status.textContent='待处理 '+items.length+' 项';
  }catch(e){status.textContent=String(e)}
}
document.getElementById('refresh').onclick=refresh;
refresh();
</script></body></html>"#;

#[cfg(test)]
mod parity_tests {
    use super::is_cross_origin;
    use axum::http::HeaderMap;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            if let Ok(value) = value.parse() {
                map.insert(*name, value);
            }
        }
        map
    }

    #[test]
    fn confirmation_endpoints_reject_cross_origin_browsers() {
        assert!(!is_cross_origin(&headers(&[("host", "127.0.0.1:3000")])), "脚本/curl 不带 Origin");
        assert!(!is_cross_origin(&headers(&[("host", "127.0.0.1:3000"), ("origin", "http://127.0.0.1:3000")])));
        assert!(is_cross_origin(&headers(&[("host", "127.0.0.1:3000"), ("origin", "https://evil.example")])));
        assert!(is_cross_origin(&headers(&[("host", "127.0.0.1:3000"), ("origin", "null")])));
        assert!(is_cross_origin(&headers(&[("host", "127.0.0.1:3000"), ("sec-fetch-site", "cross-site")])));
    }
}
