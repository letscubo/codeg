//! Real SACP reactor + the production native send path over in-memory JSON-RPC.
//! No ConnectionManager, AppState, filesystem, database, runtime or model.
//! This verifies transport concurrency/correlation, not the full connection loop.
use super::*;
use futures::{SinkExt as _, StreamExt as _};
use sacp::schema::{CancelNotification, SessionId};
use sacp::Client;
use std::time::Duration;
use tokio::sync::oneshot;

fn request(submission: &str, native: &str) -> Value {
    json!({"version":1,"sessionId":"session-A","hostTurnId":"host-A",
        "expectedNativeTurnId":native,"submissionId":submission,
        "prompt":[{"type":"text","text":"same B"}]})
}

fn receipt(request: &Value, native: &str) -> Value {
    json!({"version":1,"hostTurnId":request["hostTurnId"],
        "nativeTurnId":native,"submissionId":request["submissionId"],"outcome":"injected"})
}

#[tokio::test]
async fn myclaw_prompt_native_acp_transport_pending_steer_allows_cancel_and_late_exact_ack() {
    let (client_end, mut agent_end) = sacp::Channel::duplex();
    let (seen_tx, seen_rx) = oneshot::channel();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let (ack_tx, ack_rx) = oneshot::channel();
    let agent = tokio::spawn(async move {
        let mut tx = agent_end.tx.clone();
        let send = |value: Value| Ok(serde_json::from_value(value).unwrap());
        let steer = serde_json::to_value(agent_end.rx.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(steer["method"], STEER_METHOD);
        seen_tx.send(()).unwrap();
        // Withhold the consumption response. The SAME connection must still
        // deliver cancellation, without turning B into another ordinary prompt.
        let cancel = serde_json::to_value(agent_end.rx.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(cancel["method"], "session/cancel");
        assert_eq!(cancel["params"]["sessionId"], "session-A");
        cancel_tx.send(()).unwrap();
        ack_rx.await.unwrap();
        tx.send(send(json!({"jsonrpc":"2.0","id":steer["id"],
            "result":receipt(&steer["params"], "native-A")})))
            .await
            .unwrap();
        // Keep the transport alive until the reactor finishes the callback.
        let _ = agent_end.rx.next().await;
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        Client
            .builder()
            .connect_with(client_end, async move |cx| -> Result<(), sacp::Error> {
                let (result_tx, mut result_rx) = oneshot::channel();
                let sending = cx.clone();
                let _ = cx.spawn(async move {
                    let result = super::send(sending, request("submission-A", "native-A")).await;
                    let _ = result_tx.send(result);
                    Ok(())
                });
                seen_rx.await.unwrap();
                assert!(matches!(
                    result_rx.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                cx.send_notification_to(
                    Agent,
                    CancelNotification::new(SessionId::new("session-A")),
                )?;
                cancel_rx.await.unwrap();
                // A may have ended by this point; its delayed result is correlated
                // against its immutable captured request, never a current B scope.
                ack_tx.send(()).unwrap();
                assert_eq!(result_rx.await.unwrap().unwrap(), SteerOutcome::Injected);
                Ok(())
            }),
    )
    .await
    .expect("native steer blocked the cancellation reactor")
    .unwrap();
    agent.await.unwrap();
}

#[tokio::test]
async fn myclaw_prompt_native_acp_transport_equal_text_and_wrong_native_ack_keep_identity() {
    let (client_end, mut agent_end) = sacp::Channel::duplex();
    let (seen_tx, seen_rx) = oneshot::channel();
    let agent = tokio::spawn(async move {
        let mut tx = agent_end.tx.clone();
        let send = |value: Value| Ok(serde_json::from_value(value).unwrap());
        let first = serde_json::to_value(agent_end.rx.next().await.unwrap().unwrap()).unwrap();
        let second = serde_json::to_value(agent_end.rx.next().await.unwrap().unwrap()).unwrap();
        assert_eq!(first["method"], STEER_METHOD);
        assert_eq!(second["method"], STEER_METHOD);
        assert_ne!(first["id"], second["id"]);
        assert_ne!(
            first["params"]["submissionId"],
            second["params"]["submissionId"]
        );
        assert_eq!(first["params"]["prompt"], second["params"]["prompt"]);
        seen_tx.send(()).unwrap();
        // Deliver responses out of order. Equal text does not merge custody,
        // and a successor native turn cannot confirm the first submission.
        tx.send(send(json!({"jsonrpc":"2.0","id":second["id"],
            "result":receipt(&second["params"], "native-A")})))
            .await
            .unwrap();
        tx.send(send(json!({"jsonrpc":"2.0","id":first["id"],
            "result":receipt(&first["params"], "native-successor")})))
            .await
            .unwrap();
        let _ = agent_end.rx.next().await;
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        Client
            .builder()
            .connect_with(client_end, async move |cx| -> Result<(), sacp::Error> {
                let (first_tx, first_rx) = oneshot::channel();
                let first_cx = cx.clone();
                let _ = cx.spawn(async move {
                    let _ = first_tx.send(super::send(first_cx, request("one", "native-A")).await);
                    Ok(())
                });
                let (second_tx, second_rx) = oneshot::channel();
                let second_cx = cx.clone();
                let _ = cx.spawn(async move {
                    let _ =
                        second_tx.send(super::send(second_cx, request("two", "native-A")).await);
                    Ok(())
                });
                seen_rx.await.unwrap();
                // Scheduling may swap the two requests on the wire, so inspect
                // each result by its own id rather than assuming spawn order.
                let first = first_rx.await.unwrap();
                let second = second_rx.await.unwrap();
                assert_eq!(
                    [&first, &second]
                        .iter()
                        .filter(|result| matches!(result, Ok(SteerOutcome::Injected)))
                        .count(),
                    1
                );
                assert_eq!(
                    [&first, &second]
                        .iter()
                        .filter(|result| matches!(result, Err(AcpError::Protocol(_))))
                        .count(),
                    1
                );
                Ok(())
            }),
    )
    .await
    .expect("out-of-order native receipts did not settle")
    .unwrap();
    agent.await.unwrap();
}
