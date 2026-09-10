//! Re-register this instance's public route with the MyClaw platform on startup.
//!
//! ## What this fixes
//!
//! The public entrypoint for a codeg instance is one Cloudflare KV record,
//! `{origin, target}`, whose `target` is the **container's bridge IP**:
//!
//! ```text
//! <slug>.moltly.ai → CF → preview-worker → KV → host nginx → <containerIP>:3080
//! ```
//!
//! A container's IP changes on every restart, and the platform registered that
//! record exactly once, at provisioning. Every restart since has therefore taken
//! the instance offline with no self-healing and no alert — the row still reads
//! RUNNING while every request 502s.
//!
//! Measured 2026-09-09: A1's container restarted at 10:18Z and moved from
//! 172.17.0.55 to .19; the host's nginx logged `connect() failed (113: No route
//! to host)` continuously. Three sibling instances (D6, D10, D11) were found in
//! the same state. A host reboot takes down every instance on that host at once,
//! and shuffles IPs between containers besides — us1 packs 34 containers into a
//! contiguous 172.17.0.2-.35.
//!
//! ## Why the container is the right caller
//!
//! The call goes **outbound**. It does not depend on the very inbound route that
//! is broken, which is what lets an instance repair itself. And because codeg
//! runs under supervisor (`autorestart=true`), every path that can change the IP
//! — host reboot, container restart, a codeg crash, a self-update re-exec — ends
//! in this same startup, so every one of them re-registers.
//!
//! The alternative, a sweep plus docker-event watch in the host agent, covers the
//! same ground but carries a race this design does not have: on host boot, pm2
//! starts the agent while Docker starts the containers, so a sweep can run before
//! a container exists and find no IP to publish.
//!
//! ## No credentials are pushed into the container
//!
//! This container never holds a Cloudflare token. It reports; the platform, which
//! already holds the credential, writes. Authentication reuses the outbound
//! webhook's `vmId + s`, and the endpoint is derived from that same configured
//! webhook URL — the identical trick `myclaw_skills` uses for skill sync, and for
//! the identical reason: the origin, the vm id and the secret are all already in
//! that one URL, so nothing new has to be configured and existing instances work
//! the moment they are upgraded.
//!
//! The platform treats the address we report as untrusted input and constrains it
//! (docker-bridge range, and the port must equal the one it has on file), so a
//! compromised container can at worst point its own slug at another container —
//! where the bearer token will not match.

use std::time::Duration;

use crate::chat_channel::webhook::{redact_url, WebhookConfig};
use crate::db::AppDatabase;

/// Path of the configured outbound webhook — the anchor everything is derived
/// from. Kept byte-identical to `myclaw_skills::EVENTS_PATH`; both read the same
/// stored config.
const EVENTS_PATH: &str = "/api/codeg/events";
/// Path this module posts to. The platform route lives at
/// `web/src/app/api/codeg/route/route.ts`.
const ROUTE_PATH: &str = "/api/codeg/route";

/// Fast attempts made before backing off to the slow loop.
///
/// Retrying matters more here than for skill sync: a stale skill is an
/// inconvenience, a missed registration leaves the instance unreachable from the
/// internet. On a host reboot the platform may briefly be unreachable or the
/// host agent may still be coming up, so a first failure is expected rather than
/// exceptional — hence the burst of quick attempts (2s, 4s, 8s, 16s, 32s).
const FAST_ATTEMPTS: u32 = 6;
/// First backoff step; doubles each attempt.
const BACKOFF_BASE_SECS: u64 = 2;

/// Interval of the slow loop the fast attempts fall back to.
///
/// **Never giving up is the point.** An earlier draft stopped after the fast
/// burst (~1 minute) — which fails in precisely the situation this exists for:
/// on a host reboot the platform can easily be unreachable for longer than a
/// minute, and an instance that gives up then stays 502 until something restarts
/// it, i.e. exactly the outage this module was written to end.
///
/// Five minutes is chosen against the cost of being wrong in each direction: a
/// pointless request every five minutes is nothing, while five minutes is the
/// longest an already-broken instance should wait for a retry.
const SLOW_RETRY_SECS: u64 = 300;

/// Derive the registration endpoint from the configured outbound webhook.
///
/// `None` = this instance has no enabled webhook pointing at the platform, i.e.
/// it is not platform-managed and has no route to register. Silence is correct.
fn route_endpoint(hooks: &[WebhookConfig]) -> Option<String> {
    hooks
        .iter()
        .filter(|w| w.enabled)
        .find(|w| w.url.contains(EVENTS_PATH))
        .map(|w| w.url.replacen(EVENTS_PATH, ROUTE_PATH, 1))
}

/// This container's bridge IPv4.
///
/// Reuses the web module's advertisability filter, which already rejects
/// loopback, link-local and the unspecified address — the same three we must not
/// publish. The first survivor is taken: a codeg container sits on a single
/// docker bridge, so the list is a singleton in practice, and the platform
/// rejects anything outside the bridge range anyway.
fn container_ipv4() -> Option<String> {
    use std::net::IpAddr;
    let interfaces = if_addrs::get_if_addrs().ok()?;
    interfaces.into_iter().find_map(|iface| match iface.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_link_local() && !ip.is_unspecified() => {
            Some(ip.to_string())
        }
        _ => None,
    })
}

/// Describe a reqwest failure **without the URL**.
///
/// `reqwest::Error`'s `Display` embeds the request URL, and ours carries the
/// webhook secret in its query (`?vmId=…&s=…`). Logging the error verbatim would
/// therefore write that secret into `codeg.log` — a file that is read over SSH
/// during triage and swept up by backups and diagnostic bundles. `without_url`
/// exists for exactly this ("if, for example, it contains sensitive
/// information"); the caller logs a redacted endpoint alongside, so nothing is
/// lost but the secret.
fn redacted_err(e: reqwest::Error) -> String {
    e.without_url().to_string()
}

/// One POST. `Ok(true)` = registered, `Ok(false)` = the platform refused in a way
/// retrying cannot fix, `Err` = worth another attempt.
async fn post_once(endpoint: &str, ip: &str, port: u16) -> Result<bool, String> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(redacted_err)?;

    let res = client
        .post(endpoint)
        .json(&serde_json::json!({ "ip": ip, "port": port }))
        .send()
        .await
        .map_err(redacted_err)?;

    let status = res.status();
    if status.is_success() {
        return Ok(true);
    }
    // 4xx is a verdict, not a hiccup: a bad secret or a rejected address will be
    // just as bad on the next try. 5xx (the platform's KV write failing, say) is
    // exactly what the backoff exists for.
    if status.is_client_error() {
        let body = res.text().await.unwrap_or_default();
        tracing::error!(
            "[MyclawRoute] platform refused registration at {}: {status} {}",
            redact_url(endpoint),
            body.chars().take(200).collect::<String>()
        );
        return Ok(false);
    }
    Err(format!("HTTP {status}"))
}

/// Register once, retrying with exponential backoff. Returns whether the route
/// was accepted.
pub async fn register_once(db: &AppDatabase) -> bool {
    let Ok(hooks) = crate::commands::chat_channel::get_chat_event_webhooks_core(db).await else {
        tracing::warn!("[MyclawRoute] could not read webhook config — skipping registration");
        return false;
    };
    let Some(endpoint) = route_endpoint(&hooks) else {
        // Not platform-managed, or provisioning has not configured the webhook
        // yet. The platform registers the route itself at provisioning time, so
        // there is nothing to repair on a first boot.
        tracing::info!(
            "[MyclawRoute] no enabled webhook pointing at {EVENTS_PATH} — route registration idle"
        );
        return false;
    };

    let Some(ip) = container_ipv4() else {
        tracing::error!("[MyclawRoute] no non-loopback IPv4 found — cannot register route");
        return false;
    };
    let port: u16 = std::env::var("CODEG_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(3080);

    let mut attempt: u32 = 1;
    loop {
        match post_once(&endpoint, &ip, port).await {
            Ok(true) => {
                tracing::info!("[MyclawRoute] route registered as {ip}:{port}");
                return true;
            }
            Ok(false) => return false, // refused; retrying cannot help
            Err(e) => {
                if attempt > FAST_ATTEMPTS {
                    tracing::warn!(
                        "[MyclawRoute] registration to {} still failing ({e}); retrying in {SLOW_RETRY_SECS}s",
                        redact_url(&endpoint)
                    );
                    tokio::time::sleep(Duration::from_secs(SLOW_RETRY_SECS)).await;
                    continue;
                }
                let secs = BACKOFF_BASE_SECS * 2u64.pow(attempt - 1);
                tracing::warn!(
                    "[MyclawRoute] registration attempt {attempt} to {} failed ({e}); retrying in {secs}s",
                    redact_url(&endpoint)
                );
                tokio::time::sleep(Duration::from_secs(secs)).await;
                attempt += 1;
            }
        }
    }
}

/// Fire the startup registration on a detached task.
///
/// Detached deliberately: a slow or unreachable platform must never hold up the
/// server coming up. The instance is perfectly usable from inside the host while
/// this is still retrying.
pub fn spawn_startup_registration(db: AppDatabase) {
    tokio::spawn(async move {
        register_once(&db).await;
    });
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hook(url: &str, enabled: bool) -> WebhookConfig {
        WebhookConfig {
            url: url.to_string(),
            enabled,
        }
    }

    #[test]
    fn derives_the_route_endpoint_from_the_events_webhook() {
        // origin / vmId / secret all ride along untouched — that is the whole
        // point of deriving rather than configuring a second URL.
        let hooks = vec![hook(
            "https://myclaw.ai/api/codeg/events?vmId=abc-123&s=sekret",
            true,
        )];
        assert_eq!(
            route_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/route?vmId=abc-123&s=sekret")
        );
    }

    #[test]
    fn ignores_disabled_hooks() {
        let hooks = vec![hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", false)];
        assert_eq!(route_endpoint(&hooks), None);
    }

    #[test]
    fn picks_the_enabled_one_when_both_are_present() {
        let hooks = vec![
            hook("https://stale.example/api/codeg/events?vmId=a&s=b", false),
            hook("https://myclaw.ai/api/codeg/events?vmId=a&s=b", true),
        ];
        assert_eq!(
            route_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/route?vmId=a&s=b")
        );
    }

    #[test]
    fn no_platform_webhook_means_no_endpoint() {
        // A self-hosted instance with an unrelated webhook must stay silent
        // rather than posting its address somewhere arbitrary.
        let hooks = vec![hook("https://example.com/hooks/other", true)];
        assert_eq!(route_endpoint(&hooks), None);
    }

    /// The endpoint carries the webhook secret in its query, and this log line is
    /// written on every failed attempt — a retry storm during a host reboot would
    /// otherwise print it repeatedly into a file that triage reads over SSH and
    /// backups sweep up.
    #[test]
    fn redaction_keeps_the_secret_out_of_logs() {
        let endpoint = "https://myclaw.ai/api/codeg/route?vmId=abc-123&s=sup3rsekret";
        let logged = redact_url(endpoint);
        assert_eq!(logged, "https://myclaw.ai");
        assert!(!logged.contains("sup3rsekret"), "{logged}");
        assert!(!logged.contains("abc-123"), "{logged}");
    }

    #[test]
    fn replaces_only_the_first_occurrence() {
        // A query string that happens to echo the path must not be rewritten —
        // that would corrupt the secret or the redirect it carries.
        let hooks = vec![hook(
            "https://myclaw.ai/api/codeg/events?vmId=a&s=b&next=/api/codeg/events",
            true,
        )];
        assert_eq!(
            route_endpoint(&hooks).as_deref(),
            Some("https://myclaw.ai/api/codeg/route?vmId=a&s=b&next=/api/codeg/events")
        );
    }
}
