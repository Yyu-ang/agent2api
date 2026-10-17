//! TeleAgent 人工确认管理 API（protected router，与账号配置相同鉴权边界）。
use axum::body::Bytes;
use axum::extract::State;
use axum::response::{Html, Response};
use serde_json::Value;

use crate::server::core::providers::teleagent::confirmations;
use crate::server::errors;
use crate::server::http::{ok_json, parse_body};
use crate::server::ServerState;

/// GET /api/teleagent/confirmations
pub async fn list(State(_state): State<ServerState>) -> Response {
    ok_json(confirmations::list())
}

/// POST /api/teleagent/confirmations/reply
/// {"session_id":"...", "id":"...", "reply":"once|always|reject"}
/// 或 {"session_id":"...", "id":"...", "answers":["..."]}
pub async fn reply(State(_state): State<ServerState>, body: Bytes) -> Response {
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
