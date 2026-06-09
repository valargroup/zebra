//! Test-only Zakura echo/status service used to exercise service-aware routing.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::zakura::{
    encode_test_echo_status_response_payload, Frame, InboundSink, InboundSinkReject, ZakuraPeerId,
    TEST_ECHO_STATUS_REQUEST, TEST_ECHO_STATUS_RESPONSE, ZAKURA_STREAM_TEST_ECHO_STATUS,
};

/// Test-only native service id advertised by the echo/status service.
pub const TEST_ECHO_STATUS_SERVICE_ID: &str = "zakura.test.echo_status.v1";

/// Test-only echo/status service.
#[derive(Clone, Debug)]
pub struct TestEchoStatusService {
    reject_requests: bool,
    calls: Arc<AtomicUsize>,
    payloads: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl TestEchoStatusService {
    /// Create a service that answers test echo/status requests.
    pub fn accepting() -> Self {
        Self::new(false)
    }

    /// Create a service that rejects live test echo/status requests.
    pub fn rejecting() -> Self {
        Self::new(true)
    }

    fn new(reject_requests: bool) -> Self {
        Self {
            reject_requests,
            calls: Arc::new(AtomicUsize::new(0)),
            payloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Number of live requests observed by this service.
    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl InboundSink for TestEchoStatusService {
    fn deliver(
        &self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), InboundSinkReject> {
        Ok(())
    }

    fn request<'a>(
        &'a self,
        _peer_id: ZakuraPeerId,
        stream_kind: u16,
        request_id: u64,
        _max_frame_bytes: u32,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Frame>, InboundSinkReject>> + Send + 'a>> {
        Box::pin(async move {
            if stream_kind != ZAKURA_STREAM_TEST_ECHO_STATUS {
                return Err(InboundSinkReject::local(
                    "test echo/status service only handles the test stream",
                ));
            }
            if frame.message_type != TEST_ECHO_STATUS_REQUEST || frame.flags != 0 {
                return Err(InboundSinkReject::protocol(
                    "test echo/status request used an invalid frame",
                ));
            }

            self.calls.fetch_add(1, Ordering::Relaxed);
            self.payloads
                .lock()
                .map_err(|_| InboundSinkReject::local("test service payload mutex poisoned"))?
                .push(frame.payload.clone());

            if self.reject_requests {
                return Err(InboundSinkReject::local(
                    "test echo/status service intentionally rejected the request",
                ));
            }

            Ok(vec![Frame {
                message_type: TEST_ECHO_STATUS_RESPONSE,
                flags: 0,
                payload: encode_test_echo_status_response_payload(request_id, &frame.payload),
            }])
        })
    }
}
