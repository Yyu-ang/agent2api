//! 一轮 Zed 网页登录的编排：生成 RSA-2048 密钥对 → 绑本机随机端口 →
//! 拼 `native_app_signin` 授权地址 → 等回调 → 解密 access token →
//! 读 users/me 组装凭证（插件 `signIn` / `signedInWith` 的对应物）。
//!
//! ── 为什么监听器与密钥只活在一轮里 ────────────────────────
//! 公钥进了授权地址、私钥只用来解这一次回调：两者与端口、systemId 是
//! 「同生共死」的一组状态，分开传必漏收尾 —— 合在 [`Session`] 里，
//! 编排（超时/取消/落账号）在 `core::login::zed`，与 Trae 的分工相同。
//!
//! ── 回调的响应由监听器自己给 ──────────────────────────────
//! Zed 的页面把浏览器送到本机端口后要看到结果：带齐 `user_id` 与
//! `access_token` 的请求回 **302 到官方成功页**，缺任一参数回 **404**
//! 且**不投递**（插件逐字）—— 浏览器噪音（favicon / preconnect）因此
//! 不会误杀这一轮登录，监听器继续等真回调。任意路径都认（Zed 的页面
//! 「sends the browser to the port on whatever path; the query is what
//! counts」），所以这里不设路径白名单，只看 query。
//!
//! ── 解密为什么不用手动去填充 ──────────────────────────────
//! 插件在 Node/Bun 上手动剥 PKCS#1 v1.5 填充，是因为那两个运行时拒绝
//! v1.5 解密；RustCrypto 的 `rsa` 没有这个限制，`Pkcs1v15Encrypt` 直接
//! 解。顺序照插件：先 OAEP-SHA256，失败再 v1.5，两条都失败才报错。

use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Mutex as StdMutex;

use axum::extract::OriginalUri;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use base64::Engine;
use rsa::pkcs1::EncodeRsaPublicKey;
use rsa::{Oaep, Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

use crate::server::core::proxies::ResolvedProxy;
use crate::server::errors::GatewayError;
use crate::server::logging;

use super::credentials::{credential_from_me, new_system_id, Credential};
use super::{SITE, SUCCEEDED_URL};

/// 回调密文解出的明文上限之外的防护在 RSA 本身（2048 位密钥的密文恒
/// 256 字节），这里不需要额外的长度判定。

/// 进行中的一轮登录（活对象：私钥 + 监听器 + systemId）。
pub struct Session {
    pub auth_url: String,
    system_id: String,
    private_key: RsaPrivateKey,
    shutdown: watch::Sender<bool>,
    inbox: tokio::sync::Mutex<mpsc::UnboundedReceiver<(String, String)>>,
    sender_slot: StdMutex<Option<mpsc::UnboundedSender<(String, String)>>>,
}

impl Session {
    /// 起一轮登录：先生成密钥与 systemId、绑端口，最后拼授权地址。
    ///
    /// 顺序与插件一致：端口必须先落地，授权地址里的 `native_app_port`
    /// 才是真在听的那个端口。RSA-2048 密钥生成是百毫秒级的 CPU 活，
    /// 调用方在后台任务里调本函数（见 `core::login::zed`），不占 HTTP
    /// 处理线程。
    pub async fn begin() -> Result<Self, String> {
        let private_key = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
            .map_err(|error| format!("生成 Zed 登录密钥失败：{error}"))?;
        let public_key = RsaPublicKey::from(&private_key);
        let der = public_key
            .to_pkcs1_der()
            .map_err(|error| format!("导出 Zed 登录公钥失败：{error}"))?;
        // url-safe base64 **带 padding**（插件 padded() 逐字：Rust 的
        // URL_SAFE 写法）；base64 的 URL_SAFE 引擎正好保留 padding
        let public_b64 = base64::engine::general_purpose::URL_SAFE.encode(der.as_bytes());
        let system_id = new_system_id()
            .ok_or_else(|| "无法生成 Zed 登录的 systemId（系统随机源不可用），请重试".to_string())?;

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|error| format!("绑定本机回调端口失败：{error}"))?;
        let port = listener
            .local_addr()
            .map_err(|error| format!("读不到回调端口：{error}"))?
            .port();
        let (shutdown, receiver) = watch::channel(false);
        let (sender, inbox) = mpsc::unbounded_channel::<(String, String)>();
        let app = router(sender.clone());
        spawn_server(listener, app.clone(), receiver.clone());
        if let Ok(v6) = TcpListener::bind((Ipv6Addr::LOCALHOST, port)).await {
            spawn_server(v6, app, receiver);
        }
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("native_app_port", &port.to_string())
            .append_pair("native_app_public_key", &public_b64)
            .append_pair("system_id", &system_id)
            .finish();
        let auth_url = format!("{SITE}/native_app_signin?{query}");
        logging::log(
            "[Login]",
            &format!("Zed 登录回调已监听 127.0.0.1:{port}（native_app_signin）"),
        );
        Ok(Self {
            auth_url,
            system_id,
            private_key,
            shutdown,
            inbox: tokio::sync::Mutex::new(inbox),
            sender_slot: StdMutex::new(Some(sender)),
        })
    }

    /// 主动结束这一轮：让监听任务退出、等待方以 None 收尾（取消路径
    /// 必须显式调，与 trae 的 CallbackListener::close 同一理由：Session
    /// 被 Arc 共享，只靠 Drop 端口要等到超时才还）。
    pub fn close(&self) {
        if let Some(mut slot) = self
            .sender_slot
            .lock()
            .map_err(|poisoned| poisoned.into_inner())
            .ok()
        {
            slot.take();
        }
        let _ = self.shutdown.send(true);
    }

    /// 等回调并把它换成凭证（不落盘，落盘由调用方做）。
    ///
    /// 解密 → users/me → 选组织（组织关了 hosted models 在
    /// `credential_from_me` 里拒绝）。超时不在这里管（编排层用网关共用
    /// 的 `LOGIN_TIMEOUT_MS` 包住整个 complete，与 Trae 同一口径）。
    pub async fn complete(
        &self,
        proxy: Option<&ResolvedProxy>,
    ) -> Result<Credential, GatewayError> {
        let (user_id, ciphertext) = {
            let mut guard = self.inbox.lock().await;
            guard.recv().await
        }
        .ok_or_else(|| {
            GatewayError::with_status(408, "Zed 登录回调已丢失（本轮监听已结束），请重新发起登录")
        })?;
        let access = decrypt_access_token(&self.private_key, &ciphertext)
            .map_err(|message| GatewayError::with_status(502, message))?;
        // 先按「还没有组织/套餐」的半成品凭证去读 users/me（那条调用只要
        // userId + access + systemId），读完再由 credential_from_me 补齐
        let probe = Credential {
            access: access.clone(),
            user_id: user_id.clone(),
            system_id: self.system_id.clone(),
            org: String::new(),
            plan: String::new(),
            login: String::new(),
            name: String::new(),
            display: String::new(),
        };
        let me = super::http::users_me(&probe, proxy, std::time::Duration::from_secs(20))
            .await
            .map_err(|error| {
                let status = error.status().unwrap_or(502);
                GatewayError::with_status(
                    i32::from(status),
                    format!("Zed：读取账号信息失败（{}）", error.message()),
                )
            })?;
        credential_from_me(&access, &user_id, &self.system_id, &me)
            .map_err(|message| GatewayError::with_status(400, message))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

/// 解开回调里的 access token 密文（插件 `decrypt`）：先 OAEP-SHA256，
/// 失败再 PKCS#1 v1.5。密文是 url-safe base64（解码前先剥掉尾部 `=`，
/// 与插件 `replace(/=+$/, "")` 同一条 —— 本仓的解码器用 NO_PAD 引擎，
/// 剥不剥都行，写明是与插件对齐）。
pub fn decrypt_access_token(key: &RsaPrivateKey, ciphertext: &str) -> Result<String, String> {
    let stripped = ciphertext.trim_end_matches('=');
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(stripped)
        .map_err(|_| "Zed sign-in: the access token couldn't be decrypted".to_string())?;
    if let Ok(plain) = key.decrypt(Oaep::new::<sha2::Sha256>(), &bytes) {
        return String::from_utf8(plain)
            .map_err(|_| "Zed sign-in: the access token couldn't be decrypted".to_string());
    }
    if let Ok(plain) = key.decrypt(Pkcs1v15Encrypt, &bytes) {
        return String::from_utf8(plain)
            .map_err(|_| "Zed sign-in: the access token couldn't be decrypted".to_string());
    }
    Err("Zed sign-in: the access token couldn't be decrypted".to_string())
}

/// 只有一条兜底路由：任意路径都看 query（见模块头）。
fn router(sender: mpsc::UnboundedSender<(String, String)>) -> Router {
    Router::new().fallback(move |uri: OriginalUri| {
        let sender = sender.clone();
        async move { handle_callback(&sender, &uri) }
    })
}

fn handle_callback(
    sender: &mpsc::UnboundedSender<(String, String)>,
    uri: &OriginalUri,
) -> Response {
    let query = uri.0.query().unwrap_or_default();
    let mut user_id = String::new();
    let mut access_token = String::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "user_id" => user_id = value.into_owned(),
            "access_token" => access_token = value.into_owned(),
            _ => {}
        }
    }
    if user_id.is_empty() || access_token.is_empty() {
        // 缺材料：404 且不投递（浏览器噪音不许误杀这一轮）
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    }
    if sender
        .send((user_id.to_string(), access_token.to_string()))
        .is_err()
    {
        return (StatusCode::GONE, "Login expired").into_response();
    }
    // 302 去官方成功页（插件逐字）
    let mut response = StatusCode::FOUND.into_response();
    if let Ok(location) = axum::http::HeaderValue::from_str(SUCCEEDED_URL) {
        response
            .headers_mut()
            .insert(axum::http::header::LOCATION, location);
    }
    response
}

fn spawn_server(listener: TcpListener, app: Router, mut shutdown: watch::Receiver<bool>) {
    crate::spawn_task(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
            if *shutdown.borrow() {
                return;
            }
            let _ = shutdown.changed().await;
        });
        if let Err(error) = serve.await {
            logging::log("[Login]", &format!("⚠️ Zed 回调监听器异常退出: {error}"));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::traits::PublicKeyParts;
    use rsa::{Oaep, Pkcs1v15Encrypt, RsaPublicKey};
    use std::time::Duration;

    fn test_key() -> RsaPrivateKey {
        RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).expect("测试密钥应可生成")
    }

    #[test]
    fn decrypt_round_trips_oaep_and_pkcs1v15() {
        let key = test_key();
        let public = RsaPublicKey::from(&key);
        let secret = b"zed-access-token-123";
        // OAEP-SHA256
        let encrypted = public
            .encrypt(&mut rsa::rand_core::OsRng, Oaep::new::<sha2::Sha256>(), secret)
            .expect("OAEP 加密应成功");
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(&encrypted);
        assert_eq!(
            decrypt_access_token(&key, &encoded).expect("OAEP 应可解"),
            "zed-access-token-123"
        );
        // PKCS#1 v1.5（带 padding 的 url-safe 写法也要认）
        let encrypted = public
            .encrypt(&mut rsa::rand_core::OsRng, Pkcs1v15Encrypt, secret)
            .expect("v1.5 加密应成功");
        let encoded = base64::engine::general_purpose::URL_SAFE.encode(&encrypted);
        assert_eq!(
            decrypt_access_token(&key, &encoded).expect("v1.5 应可解"),
            "zed-access-token-123"
        );
        // 密钥位数与插件一致（RSA-2048）
        assert_eq!(key.n().bits(), 2048);
    }

    #[tokio::test]
    async fn callback_needs_both_params_and_redirects() {
        let session = Session::begin().await.expect("应能起一轮登录");
        // 从 auth_url 里拆出端口（地址形态本身也是契约的一部分）
        let url = url::Url::parse(&session.auth_url).expect("auth_url 应合法");
        assert_eq!(url.host_str(), Some("zed.dev"));
        assert_eq!(url.path(), "/native_app_signin");
        let port: u16 = url
            .query_pairs()
            .find(|(key, _)| key == "native_app_port")
            .and_then(|(_, value)| value.parse().ok())
            .expect("应带端口");
        assert!(
            url.query_pairs().any(|(key, _)| key == "native_app_public_key"),
            "应带公钥"
        );
        // 缺参数 → 404、不投递
        let status = reqwest::get(format!("http://127.0.0.1:{port}/anything?user_id=1"))
            .await
            .expect("监听应可达")
            .status()
            .as_u16();
        assert_eq!(status, 404);
        // 带齐 → 302 去成功页，且回调被投递
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("client");
        let response = client
            .get(format!(
                "http://127.0.0.1:{port}/cb?user_id=u9&access_token=xyz"
            ))
            .send()
            .await
            .expect("监听应可达");
        assert_eq!(response.status().as_u16(), 302);
        assert_eq!(
            response
                .headers()
                .get("location")
                .and_then(|value| value.to_str().ok()),
            Some(SUCCEEDED_URL)
        );
        let got = tokio::time::timeout(Duration::from_secs(2), async {
            let mut guard = session.inbox.lock().await;
            guard.recv().await
        })
        .await
        .expect("回调应投递")
        .expect("通道不应关闭");
        assert_eq!(got, ("u9".to_string(), "xyz".to_string()));
        session.close();
    }
}
