//! Zed 网页登录与通用 start / wait / cancel 状态表的衔接。
//!
//! 形态与 Trae 那一家逐字同构（理由也相同，见 `login/trae.rs` 模块头）：
//! 授权地址只能在进程内现造（先生成 RSA 密钥对、绑 loopback 随机端口，
//! 才拼得出 `native_app_signin` 地址），「这一轮登录」是一份带着私钥与
//! 监听器的活对象，不能塞进共用的 `LoginTaskState`，单独一张待办表；
//! 同一时刻只允许一轮（新的一轮先关掉旧的监听）；取消/超时/完成三条
//! 收尾路径都显式 `close()` 还端口。
//!
//! 超时取网关共用的 `LOGIN_TIMEOUT_MS`（5 分钟）而不是插件的 10 分钟：
//! 与 Trae 同一取舍（共用 `/wait` 协议与界面计时口径），超时重新发起
//! 即可。

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::server::core::providers::zed::login::Session;
use crate::server::core::providers::{kind_id, ProviderKind};
use crate::server::logging;

use super::{finish_task_error, LoginService, LoginTaskHandle, LOGIN_TIMEOUT_MS};

impl LoginService {
    /// 发起一次 Zed 网页登录（同步部分只建任务 + 登记 + 起后台；密钥
    /// 生成与拼地址在后台任务里做，调用方用 `wait_for_auth_url` 等）。
    pub fn start_zed_login(&self) -> Result<LoginTaskHandle, String> {
        let info = crate::server::core::endpoints::resolve_edition(Some(
            crate::server::core::endpoints::DEFAULT_EDITION,
        ));
        let handle = self.new_handle_for_provider(info, kind_id(ProviderKind::Zed));
        let state = format!("zed-{}", logging::now_ms());
        handle.update(|task| {
            task.state = Some(state.clone());
        });
        // 先收掉上一轮（释放它的回调端口），再登记新的一轮
        self.close_zed_login();
        self.tasks.register(&state, handle.clone());
        let service = self.clone();
        let task = handle.clone();
        crate::spawn_task(async move {
            service.run_zed_login(task).await;
        });
        logging::log("[Login]", "发起 Zed 网页登录（等待浏览器回调…）");
        Ok(handle)
    }

    /// 后台那半个登录：造授权地址 → 等回调 → 解密换凭证 → 落账号。
    async fn run_zed_login(&self, handle: LoginTaskHandle) {
        let state = handle.snapshot().state.unwrap_or_default();
        let session = match Session::begin().await {
            Ok(session) => Arc::new(session),
            Err(error) => {
                self.close_zed_login();
                finish_task_error(&handle, &error);
                return;
            }
        };
        if handle.snapshot().canceled {
            session.close();
            self.close_zed_login();
            return;
        }
        let auth_url = session.auth_url.clone();
        self.zed_login()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(state.clone(), session.clone());
        handle.update(|task| {
            task.auth_url = Some(auth_url.clone());
        });

        let outcome =
            tokio::time::timeout(Duration::from_millis(LOGIN_TIMEOUT_MS), session.complete(None))
                .await;
        let credential = match outcome {
            Ok(Ok(credential)) => credential,
            Ok(Err(error)) => {
                self.close_zed_login();
                if handle.snapshot().canceled {
                    // 取消由 /cancel 收尾，这里不再补一条错误
                    return;
                }
                finish_task_error(&handle, &error.message);
                return;
            }
            Err(_) => {
                self.close_zed_login();
                finish_task_error(&handle, "Zed 网页登录超时（5 分钟），请重新发起");
                return;
            }
        };
        // 顺手把模型清单预热一次（插件 signedInWith 同一条：登录成功先
        // 读一次目录，模型页打开就有真清单而不是兜底六个）。失败不影响
        // 登录结果（目录刷新链路会再试），结果直接丢弃。
        let store = self.store.clone();
        let warm_credential = credential.clone();
        crate::spawn_task(async move {
            let _ = crate::server::core::providers::zed::models::refresh(
                &store,
                "",
                &warm_credential,
                true,
            )
            .await;
        });

        // 落账号与取消共用同一把任务锁（与 trae / zcode 逐字相同的处置）
        let mut task = handle.lock();
        if task.done || task.canceled {
            drop(task);
            self.close_zed_login();
            return;
        }
        match self.store.add_zed_account(&credential, None, "web") {
            Ok(account) => {
                task.session = Some(json!({
                    "accountUid": account.get("id"),
                    "nickname": account.get("name"),
                    "edition": task.edition,
                    "provider": kind_id(ProviderKind::Zed),
                }));
                logging::log(
                    "[Login]",
                    &format!(
                        "✅ Zed 网页登录完成，账号已加入列表（{}）",
                        account.get("name").and_then(Value::as_str).unwrap_or("")
                    ),
                );
            }
            Err(error) => task.error = Some(error.message),
        }
        task.done = true;
        task.finished_at = Some(logging::now_ms());
        drop(task);
        self.close_zed_login();
    }

    /// 关掉进行中的 Zed 登录（本家同一时刻只有一轮，整体收摊）。
    pub(crate) fn close_zed_login(&self) {
        let orphaned: Vec<Arc<Session>> = self
            .zed_login()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .drain()
            .map(|(_, session)| session)
            .collect();
        for session in orphaned {
            session.close();
        }
    }
}
