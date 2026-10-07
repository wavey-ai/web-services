//! Streaming responses on the HTTP/1.1+HTTP/2 listener.
//!
//! A streaming handler runs inside the response it produces, on the task that serves the
//! stream: hyper's per-stream task on HTTP/2, the connection task on HTTP/1.1. The service
//! future polls the handler until it sends the response head. From then on the response body
//! owns the handler and polls it each time hyper asks for the next frame.
//!
//! The writer holds at most one chunk. `send_data` stays pending until the body has taken the
//! previous chunk, so the handler is never more than one chunk ahead of what hyper accepted.
//!
//! A handler that computes its chunks without waiting would run its whole response in one poll
//! of the task: hyper keeps polling a body that is always ready, and on HTTP/1.1 flushes only
//! when its write buffer is full. The body therefore yields the task after each
//! [`HANDLER_SLICE`] of handler time.

use crate::{
    error::ServerError,
    request_limit::RequestPermit,
    traits::{HandlerResult, StreamWriter},
};
use bytes::Bytes;
use futures_util::future::poll_fn;
use http::Response;
use hyper::body::{Body, Frame};
use std::{
    any::Any,
    future::Future,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tokio_util::{
    sync::CancellationToken,
    task::{task_tracker::TaskTrackerToken, TaskTracker},
};
use tracing::{error, Span};

/// Handler time after which the body yields its task, so that a handler that never waits shares
/// its worker with the other connections and its first chunk is flushed early.
const HANDLER_SLICE: Duration = Duration::from_micros(500);

/// Wakes the task after the poll in which it yielded, and records that it did.
struct YieldWaker {
    resumed: AtomicBool,
    task: Waker,
}

impl Wake for YieldWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.resumed.store(true, Ordering::Release);
        self.task.wake_by_ref();
    }
}

/// Make the current task yield. The runtime defers the wake-up until the current poll of the
/// task has returned. hyper may poll the body again within that poll (HTTP/1.1 loops back after
/// a flush), so the body stays `Pending` until the returned waker has fired.
fn yield_task(cx: &Context<'_>) -> Arc<YieldWaker> {
    let yielded = Arc::new(YieldWaker {
        resumed: AtomicBool::new(false),
        task: cx.waker().clone(),
    });
    let waker = Waker::from(Arc::clone(&yielded));
    let mut yield_cx = Context::from_waker(&waker);
    let _ = std::pin::pin!(tokio::task::yield_now())
        .as_mut()
        .poll(&mut yield_cx);
    yielded
}

/// A handler call that owns everything it borrows.
pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = HandlerResult<()>> + Send>>;

/// Work that outlives the request that started it: WebSocket sessions, and streaming handlers
/// that still run after their response ended.
pub(crate) struct Detached {
    /// Also counts every streaming response in flight, so a stopping server waits until each
    /// handler is gone.
    pub(crate) tasks: TaskTracker,
    /// Cancelled when the server stops: detached tasks end.
    pub(crate) shutdown: CancellationToken,
}

impl Detached {
    pub(crate) fn new() -> Self {
        Self {
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
        }
    }
}

/// What the writer has handed over and the body has not yet taken.
#[derive(Default)]
struct Slot {
    head: Option<Response<()>>,
    head_sent: bool,
    chunk: Option<Bytes>,
    /// Set only by `StreamWriter::finish`. A writer dropped with this unset stopped early, so
    /// the stream is reset instead of ended cleanly.
    finished: bool,
    writer_dropped: bool,
    body_dropped: bool,
    /// Present only while the body waits with no handler poll in progress, so a write made
    /// during the body's own poll of the handler wakes nothing.
    body_waker: Option<Waker>,
    /// The writer waiting for the slot to empty.
    writer_waker: Option<Waker>,
}

type Shared = Arc<Mutex<Slot>>;

fn lock(shared: &Shared) -> MutexGuard<'_, Slot> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// Keep `previous` when it already wakes the current task, to avoid a clone per poll.
fn current_waker(previous: Option<Waker>, cx: &Context<'_>) -> Waker {
    match previous {
        Some(waker) if waker.will_wake(cx.waker()) => waker,
        _ => cx.waker().clone(),
    }
}

struct InlineStreamWriter {
    shared: Shared,
}

impl InlineStreamWriter {
    fn poll_send(
        &self,
        cx: &mut Context<'_>,
        data: &mut Option<Bytes>,
    ) -> Poll<Result<(), ServerError>> {
        let mut slot = lock(&self.shared);
        if slot.finished {
            return Poll::Ready(Err(ServerError::Config("stream already finished".into())));
        }
        if slot.body_dropped {
            return Poll::Ready(Err(ServerError::Config(
                "failed to send stream body chunk".into(),
            )));
        }
        if slot.chunk.is_some() {
            let previous = slot.writer_waker.take();
            slot.writer_waker = Some(current_waker(previous, cx));
            return Poll::Pending;
        }
        slot.chunk = data.take();
        let body_waker = slot.body_waker.take();
        drop(slot);
        wake(body_waker);
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl StreamWriter for InlineStreamWriter {
    async fn send_response(&mut self, response: Response<()>) -> Result<(), ServerError> {
        let body_waker = {
            let mut slot = lock(&self.shared);
            if slot.head_sent {
                return Err(ServerError::Config("stream response already sent".into()));
            }
            if slot.body_dropped {
                return Err(ServerError::Config(
                    "failed to send stream response head".into(),
                ));
            }
            slot.head = Some(response);
            slot.head_sent = true;
            slot.body_waker.take()
        };
        wake(body_waker);
        Ok(())
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        let mut data = Some(data);
        poll_fn(|cx| self.poll_send(cx, &mut data)).await
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        let body_waker = {
            let mut slot = lock(&self.shared);
            slot.finished = true;
            slot.body_waker.take()
        };
        wake(body_waker);
        Ok(())
    }
}

impl Drop for InlineStreamWriter {
    fn drop(&mut self) {
        let body_waker = {
            let mut slot = lock(&self.shared);
            slot.writer_dropped = true;
            slot.body_waker.take()
        };
        wake(body_waker);
    }
}

/// The handler future with what lives exactly as long as it: the request permit and the
/// request span.
struct Handler {
    future: HandlerFuture,
    span: Span,
    label: &'static str,
    _permit: RequestPermit,
}

impl Future for Handler {
    type Output = ();

    /// Ready once the handler has returned or panicked. Both are logged here. A panic is
    /// contained so that it does not unwind through the task that serves the connection.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        let _entered = this.span.enter();
        match catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(Ok(()))) => Poll::Ready(()),
            Ok(Poll::Ready(Err(err))) => {
                error!("{} error: {}", this.label, err);
                Poll::Ready(())
            }
            Err(panic) => {
                error!(panic = panic_message(&*panic), "{} panicked", this.label);
                Poll::Ready(())
            }
        }
    }
}

/// Drop a handler without letting a panic in its destructors unwind into hyper.
fn drop_handler(handler: Option<Handler>) {
    let Some(handler) = handler else {
        return;
    };
    let label = handler.label;
    if let Err(panic) = catch_unwind(AssertUnwindSafe(move || drop(handler))) {
        error!(
            panic = panic_message(&*panic),
            "{} panicked when dropped", label
        );
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// The body of a streaming response. It owns the handler: dropping the body, which hyper does
/// when the client goes away or the connection closes, cancels the handler.
pub(crate) struct StreamResponseBody {
    shared: Shared,
    handler: Option<Handler>,
    detached: Arc<Detached>,
    _tracked: TaskTrackerToken,
    ended: bool,
    /// Handler time since the body last returned `Pending`.
    busy: Duration,
    /// Set while the task yields; the handler runs again once it has resumed.
    yielding: Option<Arc<YieldWaker>>,
}

impl StreamResponseBody {
    /// `Pending` when the handler has used its slice: the task yields before the handler runs
    /// again.
    fn poll_handler(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(handler) = self.handler.as_mut() else {
            return Poll::Ready(());
        };
        if let Some(yielding) = &self.yielding {
            if !yielding.resumed.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            self.yielding = None;
        }
        if self.busy >= HANDLER_SLICE {
            self.busy = Duration::ZERO;
            self.yielding = Some(yield_task(cx));
            return Poll::Pending;
        }
        let started = Instant::now();
        let done = Pin::new(handler).poll(cx).is_ready();
        self.busy += started.elapsed();
        if done {
            // Drops the writer the handler owned, and the request permit.
            drop_handler(self.handler.take());
        }
        Poll::Ready(())
    }

    fn poll_head(&mut self, cx: &mut Context<'_>) -> Poll<Result<Response<()>, ServerError>> {
        let previous = {
            let mut slot = lock(&self.shared);
            if let Some(head) = slot.head.take() {
                return Poll::Ready(Ok(head));
            }
            slot.body_waker.take()
        };
        let yielded = self.poll_handler(cx).is_pending();
        let mut slot = lock(&self.shared);
        if let Some(head) = slot.head.take() {
            return Poll::Ready(Ok(head));
        }
        if slot.writer_dropped {
            return Poll::Ready(Err(ServerError::Config(
                "stream handler finished before sending response".into(),
            )));
        }
        slot.body_waker = Some(current_waker(previous, cx));
        if !yielded {
            self.busy = Duration::ZERO;
        }
        Poll::Pending
    }

    /// Hand a chunk taken from the slot to hyper, and wake a writer that waits for the slot on
    /// another task. A writer on this task is polled again through the handler.
    fn yield_chunk(
        chunk: Bytes,
        mut slot: MutexGuard<'_, Slot>,
        cx: &Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ServerError>>> {
        let writer_waker = slot
            .writer_waker
            .take()
            .filter(|waker| !waker.will_wake(cx.waker()));
        drop(slot);
        wake(writer_waker);
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    /// The response is complete but the handler has work left after `finish`. It keeps the
    /// request permit and runs as detached work until it returns or the server stops.
    fn detach_handler(&mut self) {
        let Some(handler) = self.handler.take() else {
            return;
        };
        let shutdown = self.detached.shutdown.clone();
        drop(self.detached.tasks.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = handler => {}
            }
        }));
    }
}

impl Body for StreamResponseBody {
    type Data = Bytes;
    type Error = ServerError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, ServerError>>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        let previous = {
            let mut slot = lock(&this.shared);
            if let Some(chunk) = slot.chunk.take() {
                return Self::yield_chunk(chunk, slot, cx);
            }
            slot.body_waker.take()
        };
        let yielded = this.poll_handler(cx).is_pending();
        let mut slot = lock(&this.shared);
        if let Some(chunk) = slot.chunk.take() {
            return Self::yield_chunk(chunk, slot, cx);
        }
        if slot.finished {
            drop(slot);
            this.ended = true;
            this.detach_handler();
            return Poll::Ready(None);
        }
        if slot.writer_dropped {
            drop(slot);
            this.ended = true;
            // The handler dropped its writer without calling `finish`, so the body is short.
            // Fail the body to reset the stream instead of letting the peer read a truncated
            // response as a complete one.
            return Poll::Ready(Some(Err(ServerError::Config(
                "streaming response ended before the handler finished".into(),
            ))));
        }
        slot.body_waker = Some(current_waker(previous, cx));
        if !yielded {
            this.busy = Duration::ZERO;
        }
        Poll::Pending
    }
}

impl Drop for StreamResponseBody {
    fn drop(&mut self) {
        let writer_waker = {
            let mut slot = lock(&self.shared);
            slot.body_dropped = true;
            // The handler's writer must not wake this task while the handler is dropped below.
            slot.body_waker = None;
            slot.writer_waker.take()
        };
        wake(writer_waker);
        drop_handler(self.handler.take());
    }
}

/// Start a streaming handler and poll it until it sends the response head. The returned body
/// runs the rest of the handler. An `Err` means the handler stopped before it sent a head.
pub(crate) async fn respond(
    start: impl FnOnce(Box<dyn StreamWriter>) -> HandlerFuture,
    label: &'static str,
    permit: RequestPermit,
    detached: &Arc<Detached>,
) -> Result<Response<StreamResponseBody>, ServerError> {
    let shared = Arc::new(Mutex::new(Slot::default()));
    let writer = InlineStreamWriter {
        shared: Arc::clone(&shared),
    };
    let mut body = StreamResponseBody {
        shared,
        handler: Some(Handler {
            future: start(Box::new(writer)),
            span: Span::current(),
            label,
            _permit: permit,
        }),
        detached: Arc::clone(detached),
        _tracked: detached.tasks.token(),
        ended: false,
        busy: Duration::ZERO,
        yielding: None,
    };
    let head = poll_fn(|cx| body.poll_head(cx)).await?;
    let (parts, ()) = head.into_parts();
    Ok(Response::from_parts(parts, body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_limit::RequestLimiter;
    use futures_util::task::{waker, ArcWake};
    use http_body_util::BodyExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl ArcWake for CountingWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn chunk(index: usize) -> Bytes {
        Bytes::from(format!("chunk-{index}"))
    }

    fn start_with<F, Fut>(handler: F) -> impl FnOnce(Box<dyn StreamWriter>) -> HandlerFuture
    where
        F: FnOnce(Box<dyn StreamWriter>) -> Fut,
        Fut: Future<Output = HandlerResult<()>> + Send + 'static,
    {
        move |writer| Box::pin(handler(writer))
    }

    /// Poll `respond` to its head on a waker that counts wake-ups.
    fn head_now(
        start: impl FnOnce(Box<dyn StreamWriter>) -> HandlerFuture,
        permit: RequestPermit,
        detached: &Arc<Detached>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Response<StreamResponseBody>, ServerError>> {
        let mut responding = Box::pin(respond(start, "test handler", permit, detached));
        responding.as_mut().poll(cx)
    }

    /// Poll the body once, or again after a yield: these tests poll outside the runtime, where
    /// a yield's wake-up fires at once. A handler that never waits yields after its slice.
    fn poll_body(
        body: &mut StreamResponseBody,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, ServerError>>> {
        poll_body_counting_yields(body, cx, &mut 0)
    }

    fn poll_body_counting_yields(
        body: &mut StreamResponseBody,
        cx: &mut Context<'_>,
        yielded: &mut usize,
    ) -> Poll<Option<Result<Bytes, ServerError>>> {
        loop {
            let polled = Pin::new(&mut *body).poll_frame(cx);
            if polled.is_pending() && body.yielding.is_some() {
                *yielded += 1;
                continue;
            }
            return polled
                .map(|frame| frame.map(|frame| frame.map(|frame| frame.into_data().unwrap())));
        }
    }

    #[test]
    fn the_handler_runs_one_chunk_ahead_and_wakes_nothing_on_its_own_task() {
        let produced = Arc::new(AtomicUsize::new(0));
        let start = start_with({
            let produced = Arc::clone(&produced);
            move |mut writer| async move {
                writer.send_response(Response::new(())).await?;
                for index in 0.. {
                    writer.send_data(chunk(index)).await?;
                    produced.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
        });
        let wakes = Arc::new(CountingWaker::default());
        let waker = waker(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);
        let detached = Arc::new(Detached::new());
        let Poll::Ready(Ok(response)) =
            head_now(start, RequestPermit::unlimited(), &detached, &mut cx)
        else {
            panic!("the head was not ready");
        };
        let mut body = response.into_body();
        assert_eq!(produced.load(Ordering::SeqCst), 1);

        let mut yielded = 0;
        for taken in 0..64 {
            match poll_body_counting_yields(&mut body, &mut cx, &mut yielded) {
                Poll::Ready(Some(Ok(data))) => assert_eq!(data, chunk(taken)),
                other => panic!("chunk {taken}: {other:?}"),
            }
            let produced = produced.load(Ordering::SeqCst);
            assert!(
                produced <= taken + 2,
                "the handler produced {produced} chunks with {} taken",
                taken + 1
            );
        }
        // The only wake-ups are the yields' own.
        assert_eq!(wakes.0.load(Ordering::SeqCst), yielded);
    }

    #[test]
    fn the_body_ends_cleanly_only_after_finish() {
        for finish in [true, false] {
            let start = start_with(move |mut writer| async move {
                writer.send_response(Response::new(())).await?;
                writer.send_data(chunk(0)).await?;
                if finish {
                    writer.finish().await?;
                }
                Ok(())
            });
            let waker = futures_util::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let detached = Arc::new(Detached::new());
            let Poll::Ready(Ok(response)) =
                head_now(start, RequestPermit::unlimited(), &detached, &mut cx)
            else {
                panic!("the head was not ready");
            };
            let mut body = response.into_body();
            assert!(matches!(
                poll_body(&mut body, &mut cx),
                Poll::Ready(Some(Ok(data))) if data == chunk(0)
            ));
            let end = poll_body(&mut body, &mut cx);
            if finish {
                assert!(matches!(end, Poll::Ready(None)), "{end:?}");
            } else {
                assert!(matches!(end, Poll::Ready(Some(Err(_)))), "{end:?}");
                assert!(matches!(poll_body(&mut body, &mut cx), Poll::Ready(None)));
            }
        }
    }

    #[test]
    fn a_handler_that_stops_before_its_head_is_an_error() {
        let failing = start_with(|_writer| async move {
            Err::<(), _>(ServerError::Config("failed before the head".into()))
        });
        let silent = start_with(|mut writer| async move { writer.finish().await });
        let panicking = start_with(|_writer| async move {
            panic!("handler panic before the head");
        });
        for start in [
            Box::new(failing) as Box<dyn FnOnce(Box<dyn StreamWriter>) -> HandlerFuture>,
            Box::new(silent),
            Box::new(panicking),
        ] {
            let waker = futures_util::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let detached = Arc::new(Detached::new());
            assert!(matches!(
                head_now(start, RequestPermit::unlimited(), &detached, &mut cx),
                Poll::Ready(Err(_))
            ));
        }
    }

    #[test]
    fn a_panic_after_the_head_fails_the_body_and_drops_the_handler() {
        let dropped = Arc::new(AtomicBool::new(false));
        let start = start_with({
            let dropped = Arc::clone(&dropped);
            move |mut writer| async move {
                let _flag = DropFlag(dropped);
                writer.send_response(Response::new(())).await?;
                writer.send_data(chunk(0)).await?;
                writer.send_data(chunk(1)).await?;
                panic!("handler panic mid-body");
            }
        });
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let detached = Arc::new(Detached::new());
        let Poll::Ready(Ok(response)) =
            head_now(start, RequestPermit::unlimited(), &detached, &mut cx)
        else {
            panic!("the head was not ready");
        };
        let mut body = response.into_body();
        assert!(matches!(
            poll_body(&mut body, &mut cx),
            Poll::Ready(Some(Ok(_)))
        ));
        assert!(!dropped.load(Ordering::SeqCst));
        // This poll resumes the handler: it hands over the second chunk, then panics.
        assert!(matches!(
            poll_body(&mut body, &mut cx),
            Poll::Ready(Some(Ok(_)))
        ));
        assert!(matches!(
            poll_body(&mut body, &mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_the_body_cancels_the_handler_and_releases_its_permit() {
        let limiter = Arc::new(RequestLimiter::new(1));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let start = start_with({
            let dropped = Arc::clone(&dropped);
            move |mut writer| async move {
                let _flag = DropFlag(dropped);
                writer.send_response(Response::new(())).await?;
                std::future::pending::<()>().await;
                writer.finish().await
            }
        });
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let detached = Arc::new(Detached::new());
        let Poll::Ready(Ok(response)) = head_now(start, permit, &detached, &mut cx) else {
            panic!("the head was not ready");
        };
        let mut body = response.into_body();
        assert!(poll_body(&mut body, &mut cx).is_pending());
        assert_eq!(detached.tasks.len(), 1);
        assert!(Arc::clone(&limiter).try_acquire_owned().is_err());

        drop(body);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(detached.tasks.len(), 0);
        assert!(Arc::clone(&limiter).try_acquire_owned().is_ok());
    }

    #[tokio::test]
    async fn a_handler_that_never_waits_yields_the_task_after_its_slice() {
        let start = start_with(|mut writer| async move {
            writer.send_response(Response::new(())).await?;
            for index in 0..8 {
                let until = Instant::now() + HANDLER_SLICE * 2;
                while Instant::now() < until {
                    std::hint::spin_loop();
                }
                writer.send_data(chunk(index)).await?;
            }
            writer.finish().await
        });
        let detached = Arc::new(Detached::new());
        let response = respond(start, "test handler", RequestPermit::unlimited(), &detached)
            .await
            .unwrap();
        let mut body = response.into_body();

        // Within one poll of the task the body hands over what is ready, then stays pending
        // however often it is polled again, as hyper does after a flush.
        let ready_in_one_poll = poll_fn(|cx| {
            let mut frames = 0;
            loop {
                match Pin::new(&mut body).poll_frame(cx) {
                    Poll::Ready(Some(Ok(_))) => frames += 1,
                    Poll::Pending => break,
                    other => panic!("{other:?}"),
                }
            }
            for _ in 0..16 {
                assert!(Pin::new(&mut body).poll_frame(cx).is_pending());
            }
            Poll::Ready(frames)
        })
        .await;
        assert!(
            ready_in_one_poll <= 2,
            "{ready_in_one_poll} frames in one poll"
        );

        let rest = body.collect().await.unwrap().to_bytes();
        let expected: Vec<u8> = (ready_in_one_poll..8)
            .flat_map(|index| chunk(index).to_vec())
            .collect();
        assert_eq!(rest.as_ref(), expected.as_slice());
    }

    #[tokio::test]
    async fn a_writer_moved_to_another_task_drives_the_body() {
        let start = start_with(|mut writer| async move {
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                writer.send_response(Response::new(())).await?;
                for index in 0..16 {
                    writer.send_data(chunk(index)).await?;
                }
                writer.finish().await
            });
            Ok(())
        });
        let detached = Arc::new(Detached::new());
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            respond(start, "test handler", RequestPermit::unlimited(), &detached),
        )
        .await
        .expect("the head did not arrive")
        .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
            .await
            .expect("the body did not end")
            .unwrap()
            .to_bytes();
        let expected: Vec<u8> = (0..16).flat_map(|index| chunk(index).to_vec()).collect();
        assert_eq!(body.as_ref(), expected.as_slice());
    }

    #[tokio::test]
    async fn a_writer_on_another_task_dropped_without_finish_fails_the_body() {
        let start = start_with(|mut writer| async move {
            writer.send_response(Response::new(())).await?;
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                writer.send_data(chunk(0)).await
            });
            Ok(())
        });
        let detached = Arc::new(Detached::new());
        let response = respond(start, "test handler", RequestPermit::unlimited(), &detached)
            .await
            .unwrap();
        let mut body = response.into_body();
        let first = body.frame().await.unwrap().unwrap();
        assert_eq!(first.into_data().unwrap(), chunk(0));
        let end = tokio::time::timeout(Duration::from_secs(5), body.frame())
            .await
            .expect("the body did not fail");
        assert!(matches!(end, Some(Err(_))), "{end:?}");
    }

    #[tokio::test]
    async fn work_after_finish_runs_detached_and_keeps_its_permit() {
        for stop_server in [false, true] {
            let limiter = Arc::new(RequestLimiter::new(1));
            let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
            let release = Arc::new(Notify::new());
            let completed = Arc::new(AtomicBool::new(false));
            let dropped = Arc::new(AtomicBool::new(false));
            let start = start_with({
                let release = Arc::clone(&release);
                let completed = Arc::clone(&completed);
                let dropped = Arc::clone(&dropped);
                move |mut writer| async move {
                    let _flag = DropFlag(dropped);
                    writer.send_response(Response::new(())).await?;
                    writer.send_data(chunk(0)).await?;
                    writer.finish().await?;
                    release.notified().await;
                    completed.store(true, Ordering::SeqCst);
                    Ok(())
                }
            });
            let detached = Arc::new(Detached::new());
            let response = respond(start, "test handler", permit, &detached)
                .await
                .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, chunk(0));
            assert!(!dropped.load(Ordering::SeqCst));
            assert!(Arc::clone(&limiter).try_acquire_owned().is_err());

            if stop_server {
                detached.shutdown.cancel();
            } else {
                release.notify_one();
            }
            detached.tasks.close();
            tokio::time::timeout(Duration::from_secs(5), detached.tasks.wait())
                .await
                .expect("the detached handler did not end");
            assert_eq!(completed.load(Ordering::SeqCst), !stop_server);
            assert!(dropped.load(Ordering::SeqCst));
            assert!(Arc::clone(&limiter).try_acquire_owned().is_ok());
        }
    }
}
