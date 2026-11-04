//! Native companion tool: prepare locally, call MyClaw registration, verify public URL.
use crate::{
    db::AppDatabase,
    web::handlers::port_preview::{self, PortPreviews, Registration},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    port: u32,
}
pub(crate) fn parse_arguments(args: Value) -> Result<u16, String> {
    let args: Arguments =
        serde_json::from_value(args).map_err(|_| "expose_preview accepts only an integer port")?;
    port_preview::validate_port(args.port, 0).map_err(str::to_owned)
}
#[async_trait::async_trait]
pub(crate) trait PreviewPublisher: Send + Sync {
    async fn register(&self, prepared: &Registration) -> Result<String, String>;
    async fn verify(&self, url: &str) -> Result<(), String>;
}
pub(crate) async fn expose_with(
    state: &PortPreviews,
    vm_id: &str,
    args: Value,
    publisher: &dyn PreviewPublisher,
) -> Result<Value, String> {
    let port = parse_arguments(args)?;
    let id = uuid::Uuid::parse_str(vm_id).map_err(|_| "Invalid instance identity")?;
    if id.to_string() != vm_id {
        return Err("Invalid instance identity".into());
    }
    let host = format!("p-{port}-{vm_id}.myclaw.build");
    let expected = format!("https://{host}");
    let prepared = port_preview::prepare(state, port.into(), host).await?;
    let url = publisher.register(&prepared).await?;
    if url != expected {
        return Err("Platform returned an unexpected preview URL".into());
    }
    publisher.verify(&url).await?;
    Ok(json!({"content":[{"type":"text","text":json!({"url":url}).to_string()}]}))
}
struct PlatformPublisher<'a>(&'a AppDatabase);
#[async_trait::async_trait]
impl PreviewPublisher for PlatformPublisher<'_> {
    async fn register(&self, prepared: &Registration) -> Result<String, String> {
        let result = crate::commands::myclaw_platform::post_signed(
            self.0,
            "/api/codeg/preview/register",
            json!(prepared),
            Duration::from_secs(30),
        )
        .await
        .map_err(|e| {
            if e.starts_with("Legacy host port previews require") {
                e
            } else {
                "MyClaw preview registration failed".to_owned()
            }
        })?;
        result
            .get("url")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| "Invalid registration response".into())
    }
    async fn verify(&self, url: &str) -> Result<(), String> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| "Cannot prepare preview verification")?;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if let Ok(response) = client.head(url).send().await {
                if response
                    .headers()
                    .get("x-myclaw-port-preview")
                    .is_some_and(|v| v == "v1")
                {
                    return Ok(());
                }
            }
        }
        Err("Preview registered but not reachable yet; retry expose_preview after checking the service and ingress".into())
    }
}
pub async fn expose(db: &AppDatabase, args: Value) -> Result<Value, String> {
    parse_arguments(args.clone())?; // fail before credential reads or any listener/network side effects
    let ep = crate::commands::myclaw_platform::endpoint(db).await?;
    expose_with(
        &port_preview::shared(),
        &ep.vm_id,
        args,
        &PlatformPublisher(db),
    )
    .await
}
