//! Streams are handed to workers oldest first.
//!
//! Slots are allocated from a stack and reused newest-first, so slot order and
//! arrival order disagree as soon as any stream closes. Workers claim the
//! first stream they can serve, which makes this ordering the ring's queue
//! discipline rather than an implementation detail.

use std::sync::Arc;

use upload_response::{UploadResponseConfig, UploadResponseService};

fn service(num_streams: usize) -> Arc<UploadResponseService> {
    Arc::new(UploadResponseService::new(UploadResponseConfig {
        num_streams,
        slot_size_kb: 4,
        slots_per_stream: 8,
        response_timeout_ms: 5_000,
    }))
}

#[tokio::test]
async fn active_streams_are_ordered_by_arrival() {
    let service = service(4);

    let first = service.open_stream().await.expect("a stream");
    let second = service.open_stream().await.expect("a stream");
    let third = service.open_stream().await.expect("a stream");
    let ids = [first.stream_id(), second.stream_id(), third.stream_id()];

    let seen: Vec<u64> = service
        .active_streams()
        .await
        .iter()
        .map(|stream| stream.stream_id)
        .collect();
    assert_eq!(seen, ids, "streams should come back in the order they opened");

    // Close the middle one and open another. Its slot is the freshest on the
    // free stack, so the newcomer lands in the middle of the ring while being
    // the youngest stream there.
    let recycled = second.stream_idx();
    second.close().await;
    let fourth = service.open_stream().await.expect("a stream");
    assert_eq!(
        fourth.stream_idx(),
        recycled,
        "this test is only meaningful if the slot was reused"
    );

    let streams = service.active_streams().await;
    let seen: Vec<u64> = streams.iter().map(|stream| stream.stream_id).collect();
    assert_eq!(
        seen,
        [ids[0], ids[2], fourth.stream_id()],
        "the youngest stream must not jump the queue by taking a low slot"
    );
    assert!(
        streams[1].stream_idx > streams[2].stream_idx,
        "slot order should disagree with arrival order here, or the test proves nothing"
    );
}
