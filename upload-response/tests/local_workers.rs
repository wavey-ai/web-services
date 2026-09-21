use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Response;
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use tokio::time::{advance, timeout};
use upload_response::{UploadResponseConfig, UploadResponseService, UploadResponseTimeouts};

fn service() -> Arc<UploadResponseService> {
    Arc::new(UploadResponseService::new_with_timeouts(
        UploadResponseConfig {
            num_streams: 1,
            slot_size_kb: 1,
            slots_per_stream: 16,
            response_timeout_ms: 10,
        },
        UploadResponseTimeouts {
            response_claim_lease_ms: 100,
            response_deadline_ms: 10,
            ..Default::default()
        },
    ))
}

fn head() -> Response<()> {
    Response::builder().status(200).body(()).unwrap()
}

fn request_head(id: u64) -> StreamHeaders {
    StreamHeaders::Request(StreamRequestHeaders {
        stream_id: id,
        version: http_pack::HttpVersion::Http11,
        method: b"POST".to_vec(),
        scheme: None,
        authority: None,
        path: b"/job".to_vec(),
        headers: vec![],
    })
}

#[tokio::test(start_paused = true)]
async fn local_writes_require_the_claim_and_productive_writers_keep_it() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let mut writer = service
        .claim_response_writer(id, "a")
        .await
        .unwrap()
        .unwrap();
    assert!(service
        .write_response_headers(id, StreamHeaders::from_response(id, &head()).unwrap())
        .await
        .is_err());
    writer.ensure_started(head()).await.unwrap();
    for _ in 0..5 {
        advance(Duration::from_millis(60)).await;
        writer
            .send_body(Bytes::from_static(b"progress"))
            .await
            .unwrap();
        assert_eq!(service.response_owner(id).await.as_deref(), Some("a"));
        assert!(service
            .claim_response_writer(id, "b")
            .await
            .unwrap()
            .is_none());
    }
    assert!(service
        .append_response_body(id, Bytes::from_static(b"bypass"))
        .await
        .is_err());
    assert!(service.end_response(id).await.is_err());
    assert!(service
        .write_handler_response(id, web_service::HandlerResponse::default())
        .await
        .is_err());
    writer.finish().await.unwrap();
    assert!(writer
        .send_body(Bytes::from_static(b"after-end"))
        .await
        .is_err());
    assert!(service
        .append_response_body_unchecked(id, Bytes::from_static(b"after-end"))
        .await
        .is_err());
    assert!(writer.release().await);
    assert!(service
        .claim_response_writer(id, "b")
        .await
        .unwrap()
        .is_none());
    assert!(service.end_response(id).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn renewal_protects_work_before_the_response_starts() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let mut writer = service
        .claim_response_writer(id, "a")
        .await
        .unwrap()
        .unwrap();
    for _ in 0..5 {
        advance(Duration::from_millis(60)).await;
        writer.renew().await.unwrap();
        assert!(service
            .claim_response_writer(id, "b")
            .await
            .unwrap()
            .is_none());
    }
    writer.ensure_started(head()).await.unwrap();
    writer.finish().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_writer_cannot_write_renew_or_release_a_replacement_with_the_same_name() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let mut stale = service
        .claim_response_writer(id, "same-worker")
        .await
        .unwrap()
        .unwrap();
    advance(Duration::from_millis(101)).await;
    assert!(stale.renew().await.unwrap_err().contains("expired"));
    let mut replacement = service
        .claim_response_writer(id, "same-worker")
        .await
        .unwrap()
        .unwrap();
    assert!(stale.ensure_started(head()).await.is_err());
    assert!(stale.renew().await.is_err());
    drop(stale);
    tokio::task::yield_now().await;
    replacement.ensure_started(head()).await.unwrap();
    replacement.finish().await.unwrap();
    assert_eq!(
        service.response_owner(id).await.as_deref(),
        Some("same-worker")
    );
}

#[tokio::test(start_paused = true)]
async fn partial_response_cannot_be_reclaimed_after_expiry() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let mut writer = service
        .claim_response_writer(id, "a")
        .await
        .unwrap()
        .unwrap();
    writer.ensure_started(head()).await.unwrap();
    advance(Duration::from_millis(101)).await;
    assert!(service
        .claim_response_writer(id, "b")
        .await
        .unwrap()
        .is_none());
    assert!(writer
        .send_body(Bytes::from_static(b"stale"))
        .await
        .is_err());
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn dropping_writer_releases_unstarted_work_and_cannot_cross_slot_reuse() {
    let service = service();
    let first = service.open_stream().await.unwrap();
    let id = first.stream_id();
    let stale = service
        .claim_response_writer(id, "a")
        .await
        .unwrap()
        .unwrap();
    drop(stale);
    tokio::task::yield_now().await;
    let mut stale = service
        .claim_response_writer(id, "b")
        .await
        .unwrap()
        .unwrap();
    first.close().await;
    let second = service.open_stream().await.unwrap();
    let replacement = service
        .claim_response_writer(second.stream_id(), "b")
        .await
        .unwrap()
        .unwrap();
    assert!(stale.ensure_started(head()).await.is_err());
    assert!(stale.renew().await.is_err());
    drop(stale);
    tokio::task::yield_now().await;
    replacement.renew().await.unwrap();
    assert_eq!(service.response_last(second.stream_id()), Some(0));
}

#[tokio::test(start_paused = true)]
async fn activity_watchers_observe_publication_release_expiry_and_close_without_polling() {
    let service = service();
    let mut first = service.watch_active_streams();
    let mut second = service.watch_active_streams();
    assert!(first.next().await.is_empty());
    assert!(second.next().await.is_empty());
    assert!(timeout(Duration::from_millis(5), first.next())
        .await
        .is_err());
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    assert_eq!(first.next().await[0].stream_id, id);
    assert_eq!(second.next().await[0].stream_id, id);
    service
        .write_request_headers(id, request_head(id))
        .await
        .unwrap();
    assert_eq!(first.next().await[0].request_last, 1);
    service
        .append_request_body(id, Bytes::from_static(b"body"))
        .await
        .unwrap();
    assert_eq!(first.next().await[0].request_last, 2);
    service.end_request(id).await.unwrap();
    assert_eq!(first.next().await[0].request_last, 3);
    let writer = service
        .claim_response_writer(id, "a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.next().await[0].response_owner.as_deref(), Some("a"));
    advance(Duration::from_millis(60)).await;
    writer.renew().await.unwrap();
    assert_eq!(first.next().await[0].response_owner.as_deref(), Some("a"));
    assert!(timeout(Duration::from_millis(110), first.next())
        .await
        .unwrap()[0]
        .response_owner
        .is_none());
    let writer = service
        .claim_response_writer(id, "b")
        .await
        .unwrap()
        .unwrap();
    first.next().await;
    writer.release().await;
    assert!(first.next().await[0].response_owner.is_none());
    service
        .write_stage_head(id, "decode", Bytes::from_static(b"stage"))
        .await
        .unwrap();
    assert_eq!(first.next().await[0].stage_last("decode"), 1);
    assert!(service.try_claim_stage(id, "decode", "c").await);
    assert_eq!(first.next().await[0].stage_owner("decode"), Some("c"));
    service.release_stage(id, "decode", "c").await;
    assert!(first.next().await[0].stage_owner("decode").is_none());
    stream.close().await;
    assert!(first.next().await.is_empty());
    assert!(second.next().await.is_empty());
}

#[tokio::test]
async fn lane_waits_observe_headers_body_end_and_reject_reused_slots() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let request = service.request_lane_handle(id).unwrap();
    let stage = service
        .stage_lane_handle(id, "decode")
        .await
        .unwrap()
        .unwrap();
    let response = service.response_lane_handle(id).unwrap();
    let producer = async {
        tokio::task::yield_now().await;
        service
            .write_request_headers(id, request_head(id))
            .await
            .unwrap();
        service
            .append_request_body(id, Bytes::from_static(b"body"))
            .await
            .unwrap();
        service.end_request(id).await.unwrap();
        service
            .write_stage_head(id, "decode", Bytes::from_static(b"stage"))
            .await
            .unwrap();
        service.end_stage(id, "decode").await.unwrap();
        service
            .write_response_headers(id, StreamHeaders::from_response(id, &head()).unwrap())
            .await
            .unwrap();
        service.end_response(id).await.unwrap();
    };
    let consumer = async {
        request.wait_for_slot(1).await.unwrap();
        assert_eq!(
            request.wait_for_slot(2).await.unwrap(),
            Bytes::from_static(b"body")
        );
        assert!(request.wait_for_slot(3).await.unwrap().is_empty());
        assert!(request.wait_for_slot(4).await.is_err());
        assert_eq!(
            stage.wait_for_slot(1).await.unwrap(),
            Bytes::from_static(b"stage")
        );
        assert!(stage.wait_for_slot(2).await.unwrap().is_empty());
        response.wait_for_slot(1).await.unwrap();
        assert!(response.wait_for_slot(2).await.unwrap().is_empty());
    };
    timeout(Duration::from_secs(1), async {
        tokio::join!(producer, consumer);
    })
    .await
    .unwrap();
    stream.close().await;
    let _replacement = service.open_stream().await.unwrap();
    assert!(request.wait_for_slot(1).await.is_err());
    assert!(stage.wait_for_slot(1).await.is_err());
    assert!(response.wait_for_slot(1).await.is_err());
}

#[tokio::test]
async fn lane_waiters_wake_on_close_before_any_data() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let request = service.request_lane_handle(stream.stream_id()).unwrap();
    let stage = service
        .stage_lane_handle(stream.stream_id(), "decode")
        .await
        .unwrap()
        .unwrap();
    let close = async {
        tokio::task::yield_now().await;
        stream.close().await;
    };
    timeout(Duration::from_secs(1), async {
        let (request, stage, ()) =
            tokio::join!(request.wait_for_slot(1), stage.wait_for_slot(1), close);
        assert!(request.is_err());
        assert!(stage.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn claimed_handler_response_crosses_the_ring_without_losing_bytes() {
    let service = service();
    let stream = service.open_stream().await.unwrap();
    let id = stream.stream_id();
    let mut writer = service
        .claim_response_writer(id, "producer")
        .await
        .unwrap()
        .unwrap();
    assert!(service.register_response_reader(id, "consumer").await);
    let lane = service.response_lane_handle(id).unwrap();
    let expected = Bytes::from(vec![0x5a; 40 * 1024]);
    let produce = writer.write_handler_response(web_service::HandlerResponse {
        status: http::StatusCode::CREATED,
        body: Some(expected.clone()),
        content_type: Some("application/octet-stream".into()),
        ..Default::default()
    });
    let consume = async {
        lane.wait_for_slot(1).await.unwrap();
        assert_eq!(service.get_response_headers(id).await.unwrap().status, 201);
        service
            .mark_response_reader_position(id, "consumer", 1)
            .await;
        let mut body = Vec::new();
        for slot in 2.. {
            let bytes = lane.wait_for_slot(slot).await.unwrap();
            service
                .mark_response_reader_position(id, "consumer", slot)
                .await;
            if bytes.is_empty() {
                break;
            }
            body.extend_from_slice(&bytes);
        }
        assert_eq!(body.as_slice(), expected.as_ref());
        assert!(lane.wait_for_slot(1).await.is_err());
    };
    timeout(Duration::from_secs(1), async {
        let (result, ()) = tokio::join!(produce, consume);
        result.unwrap();
    })
    .await
    .unwrap();
}
