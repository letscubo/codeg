//! fork(letscubo)专属:`/api/myclaw/file` 的**下载票**——让浏览器拿一条真 URL 直连容器。
//!
//! ## 为什么需要票
//!
//! 浏览器给不了 `Authorization` 头的场合恰好全是下载/预览要用的:`<a href download>`、
//! `<img src>`、`<video src>`、`<iframe src>`、`window.open`。所以只靠 Bearer 的
//! `/api/myclaw/file` 只能被平台服务端调用;要让用户点一下就下载,必须把凭证放进 URL。
//!
//! 放进 URL 的**不能**是 codeg 的那条 token —— 它等价于容器内全权限。于是:平台签一张
//! **限定单个文件 + 短时效**的票,浏览器拿着票直连容器。字节不过平台。
//!
//! ## 为什么不复用 `workspace_download` 那套票
//!
//! 上游 `workspace_transfer` 的票是**一次性消费**的(`consume_download_ticket` 拿到就
//! `remove`),而且存在进程内存里。两条都不行:
//!
//!   · 一次性 → Range 续传、`<video>` 拖拽进度条都是**多次请求同一个 URL**,第二次就 404;
//!   · 进程内存 → codeg 一重启,签出去的链接全废;而且平台要多一次「先 POST 拿票」的往返。
//!
//! 所以这里是**无状态自验**:票自带签名,codeg 用自己的 token 当 HMAC 密钥本地验。平台
//! 侧已经持有解密后的 token(`readCodegRow`),本地算完即可,零往返、重启不失效、可多次用。
//!
//! ## 票的格式(平台侧必须逐字节一致)
//!
//! ```text
//! ticket  = <payload_b64url> "." <sig_b64url>
//! payload = {"p":"<绝对路径>","e":<到期 unix 秒>}     // 紧凑 JSON
//! sig     = HMAC-SHA256(key = codeg token 的字节, msg = payload_b64url 的**文本字节**)
//! ```
//!
//! 签在 base64 文本上而不是 JSON 字节上:两侧的 JSON 序列化(键序、空格、转义)不必逐字
//! 对齐,只要 base64 文本一致就行 —— 少一整类「两边都觉得自己对」的排查。
//! 对应实现:平台侧 `web/src/lib/codeg/file-ticket.ts`,改动必须两边一起改。
//!
//! ## 三条硬约束
//!
//! 1. **票绑死单个文件**:`p` 进签名,并与 query 里的 `path` **逐字符**比对。不绑就等于
//!    一张全盘读取票。
//! 2. **票只开下载这一条路由**:验票只在 `download` handler 内做,**绝不**接进
//!    `web::auth::require_token`。接进去的话,一张下载票就能调 `/api/myclaw/exec`,
//!    等于把整台实例交出去。
//! 3. **codeg 侧再封一次时效上限**([`MAX_TICKET_TTL_SECS`]):`e` 超出「现在 + 上限」一律
//!    拒。平台持有密钥、本来可以签一张永久票 —— 这道闸把「链接泄漏」的窗口压回可控范围。
//!
//! 残留暴露面:票在 URL 里,会进边缘 access log、浏览器 history、`Referer`。但它只能读
//! 那一个文件、只活几分钟,爆炸半径远小于目前前端内存里那条全权 token
//! (`agent/ws-access` 直接把它发给浏览器,那两个 route 的顶注也承认是已知缺口)。

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// 票的最长有效期:签发时间未知,所以按「到期时刻不得超过现在 + 这个值」来封。
/// 1 小时足够一个 200MB 文件在慢网络上下完,又不至于让泄漏的链接长期可用。
pub const MAX_TICKET_TTL_SECS: i64 = 3600;

/// 容忍的时钟偏差:容器与平台各自对时,15 分钟的票不该因为几十秒的漂移而随机失败。
/// 只放宽**到期**判断,不放宽上限判断。
const CLOCK_SKEW_SECS: i64 = 60;

#[derive(Debug, PartialEq, Eq)]
pub enum TicketError {
    /// 不是 `<payload>.<sig>`、base64 解不开、或 JSON 不合格式。
    Malformed,
    /// 签名对不上 —— 密钥不同或被改过。
    BadSignature,
    /// 已过期。
    Expired,
    /// 到期时刻超出 [`MAX_TICKET_TTL_SECS`] 允许的窗口。
    TtlTooLong,
    /// 票里的路径与请求里的 `path` 不一致。
    PathMismatch,
}

impl TicketError {
    /// 回给客户端的一句话。**刻意不区分**签名错 / 过期 / 路径不符 —— 对端拿不到
    /// 「这张票本来是对的、只是过期了」这类信息,少一条试探信道。日志里另有细分。
    pub fn public_message(&self) -> &'static str {
        "Invalid or expired download ticket"
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TicketError::Malformed => "malformed",
            TicketError::BadSignature => "bad-signature",
            TicketError::Expired => "expired",
            TicketError::TtlTooLong => "ttl-too-long",
            TicketError::PathMismatch => "path-mismatch",
        }
    }
}

/// 现在的 unix 秒。
pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 验一张票:签名、时效、以及它是否就是为 `path` 这个文件签的。
///
/// `path` 传 **query 里的原始字符串**(展开 `~` 之前、canonicalize 之前)—— 签的是什么
/// 就比什么,中间任何一次规范化都可能让两侧对不上。
pub fn verify_ticket(secret: &str, ticket: &str, path: &str, now: i64) -> Result<(), TicketError> {
    let (payload_b64, sig_b64) = ticket.split_once('.').ok_or(TicketError::Malformed)?;
    if payload_b64.is_empty() || sig_b64.is_empty() {
        return Err(TicketError::Malformed);
    }

    // 先验签再解析:没过签名的内容一律不当数据看。
    let sig = URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|_| TicketError::Malformed)?;
    let mut mac =
        <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).map_err(|_| TicketError::BadSignature)?;
    mac.update(payload_b64.as_bytes());
    // verify_slice 内部是恒定时间比较,别换成 `==`。
    mac.verify_slice(&sig)
        .map_err(|_| TicketError::BadSignature)?;

    let payload = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|_| TicketError::Malformed)?;
    let claims: serde_json::Value =
        serde_json::from_slice(&payload).map_err(|_| TicketError::Malformed)?;
    let signed_path = claims
        .get("p")
        .and_then(|v| v.as_str())
        .ok_or(TicketError::Malformed)?;
    let exp = claims
        .get("e")
        .and_then(|v| v.as_i64())
        .ok_or(TicketError::Malformed)?;

    if exp > now + MAX_TICKET_TTL_SECS {
        return Err(TicketError::TtlTooLong);
    }
    if exp + CLOCK_SKEW_SECS < now {
        return Err(TicketError::Expired);
    }
    if signed_path != path {
        return Err(TicketError::PathMismatch);
    }
    Ok(())
}

/// 签一张票。**只在测试里用** —— 生产是平台签(`file-ticket.ts`),codeg 只验。
/// 留在这里是为了让两侧格式有一份可执行的规格:TS 改了格式,这里的测试会跟着红。
#[cfg(test)]
pub fn sign_ticket(secret: &str, path: &str, exp: i64) -> String {
    // 键序显式写成 e,p:平台侧是 `JSON.stringify({ e, p })`,靠的是插入序。
    // 不要改成 `json!({...}).to_string()` —— 那取决于 serde_json 是否开了
    // preserve_order,换个 feature 就和 TS 对不上了。
    let payload = format!(
        "{{\"e\":{exp},\"p\":{}}}",
        serde_json::Value::String(path.to_string())
    );
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(payload_b64.as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{payload_b64}.{sig}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "codeg-token-9a5123e3";
    const PATH: &str = "/home/ubuntu/.myclaw/sessions/s1/deck.pptx";

    #[test]
    fn a_fresh_ticket_for_the_right_path_passes() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now + 900);
        assert_eq!(verify_ticket(SECRET, &t, PATH, now), Ok(()));
    }

    /// Range 续传 / `<video>` 拖进度条 = 同一条 URL 被请求多次。票必须**可重复使用**
    /// (这正是不能复用上游一次性 ticket 的原因)。
    #[test]
    fn the_same_ticket_works_again_and_again() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now + 900);
        for _ in 0..5 {
            assert_eq!(verify_ticket(SECRET, &t, PATH, now + 60), Ok(()));
        }
    }

    #[test]
    fn a_ticket_for_another_file_is_refused() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now + 900);
        assert_eq!(
            verify_ticket(SECRET, &t, "/etc/shadow", now),
            Err(TicketError::PathMismatch)
        );
        // 连同目录前缀也不行 —— 绑的是那一个文件,不是它所在的树。
        assert_eq!(
            verify_ticket(
                SECRET,
                &t,
                "/home/ubuntu/.myclaw/sessions/s1/other.pptx",
                now
            ),
            Err(TicketError::PathMismatch)
        );
    }

    #[test]
    fn another_instances_token_cannot_sign_a_valid_ticket() {
        let now = 1_700_000_000;
        let t = sign_ticket("some-other-instance-token", PATH, now + 900);
        assert_eq!(
            verify_ticket(SECRET, &t, PATH, now),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn tampering_with_the_payload_breaks_the_signature() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now + 900);
        let (payload, sig) = t.split_once('.').unwrap();
        // 把路径换成 /etc/shadow,签名照抄
        let forged_payload =
            URL_SAFE_NO_PAD.encode(format!("{{\"e\":{},\"p\":\"/etc/shadow\"}}", now + 900));
        assert_eq!(
            verify_ticket(
                SECRET,
                &format!("{forged_payload}.{sig}"),
                "/etc/shadow",
                now
            ),
            Err(TicketError::BadSignature)
        );
        // 反过来:签名动一个字符
        let mut bad_sig: Vec<char> = sig.chars().collect();
        bad_sig[0] = if bad_sig[0] == 'A' { 'B' } else { 'A' };
        let bad: String = bad_sig.into_iter().collect();
        assert_eq!(
            verify_ticket(SECRET, &format!("{payload}.{bad}"), PATH, now),
            Err(TicketError::BadSignature)
        );
    }

    #[test]
    fn an_expired_ticket_is_refused_but_small_clock_drift_is_tolerated() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now);
        // 刚过期 + 容器时钟快了 30 秒 → 还认
        assert_eq!(verify_ticket(SECRET, &t, PATH, now + 30), Ok(()));
        // 真过了头 → 拒
        assert_eq!(
            verify_ticket(SECRET, &t, PATH, now + CLOCK_SKEW_SECS + 1),
            Err(TicketError::Expired)
        );
    }

    /// 平台持有密钥,本来能签一张十年有效的票。codeg 侧这道闸把窗口封回来。
    #[test]
    fn an_over_long_ticket_is_refused_even_though_it_is_correctly_signed() {
        let now = 1_700_000_000;
        let t = sign_ticket(SECRET, PATH, now + MAX_TICKET_TTL_SECS + 1);
        assert_eq!(
            verify_ticket(SECRET, &t, PATH, now),
            Err(TicketError::TtlTooLong)
        );
        // 正好贴着上限 → 认
        let ok = sign_ticket(SECRET, PATH, now + MAX_TICKET_TTL_SECS);
        assert_eq!(verify_ticket(SECRET, &ok, PATH, now), Ok(()));
    }

    #[test]
    fn junk_is_refused_without_panicking() {
        let now = 1_700_000_000;
        for bad in [
            "",
            ".",
            "a.",
            ".b",
            "no-dot",
            "not-base64!!.also-not",
            // 合法 base64,但内容不是 JSON
            "aGVsbG8.aGVsbG8",
        ] {
            assert!(verify_ticket(SECRET, bad, PATH, now).is_err(), "{bad}");
        }
        // 合法签名但 payload 缺字段
        let payload = URL_SAFE_NO_PAD.encode(r#"{"p":"/a"}"#);
        let mut mac = <Hmac<Sha256>>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(payload.as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        assert_eq!(
            verify_ticket(SECRET, &format!("{payload}.{sig}"), "/a", now),
            Err(TicketError::Malformed)
        );
    }

    /// 格式规格:平台侧 TS 用同样的 secret/path/exp 必须算出同一串。
    /// 这个常量是从 `file-ticket.ts` 的测试里对拷过来的 —— 两边任何一侧改了格式,
    /// 这条会红。
    #[test]
    fn wire_format_is_pinned() {
        let t = sign_ticket("secret", "/home/ubuntu/a.txt", 1_700_000_900);
        assert_eq!(
            t,
            "eyJlIjoxNzAwMDAwOTAwLCJwIjoiL2hvbWUvdWJ1bnR1L2EudHh0In0.\
             ql0psDHm8eFo24mfJgJkD9NGOy3tLMLPgXDhZQ_bZ40"
        );
    }
}
