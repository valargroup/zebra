//! Raw peer harness for adversarial Zakura tests.

use byteorder::{LittleEndian, WriteBytesExt};
use iroh::endpoint::{Connection, Endpoint, SendStream, VarInt};

use super::{LocalEndpointFactory, ZakuraTestNode};
use crate::{
    zakura::{
        run_native_initiator_handshake, Frame, StreamPrelude, ZakuraHandshakeConfig,
        ZakuraLocalLimits, ZakuraPeerId, FRAME_HEADER_BYTES, P2P_V2_ALPN, STREAM_PRELUDE_MAGIC,
    },
    BoxError, Config,
};

/// Raw Iroh peer that can violate Zakura stream rules by hand.
#[derive(Debug)]
pub struct HostilePeer {
    endpoint: Endpoint,
    connection: Connection,
    limits: ZakuraLocalLimits,
    held_streams: Vec<SendStream>,
}

impl HostilePeer {
    /// Connect to `victim` with a valid native control handshake.
    pub async fn connect_native(victim: &ZakuraTestNode, seed: u64) -> Result<Self, BoxError> {
        let limits = victim.limits().clone();
        let endpoint = LocalEndpointFactory::with_transport_config(limits.transport_config())
            .endpoint(seed)
            .await?;
        let victim_addr = victim.node_addr().await;
        endpoint.add_node_addr(victim_addr.clone())?;
        let connection = endpoint.connect(victim_addr, P2P_V2_ALPN).await?;
        let config = ZakuraHandshakeConfig::for_network(&Config::default().network);
        let local_peer_id = ZakuraPeerId::new(endpoint.node_id().as_bytes().to_vec())?;
        run_native_initiator_handshake(&connection, &limits, &config, &local_peer_id).await?;

        Ok(Self {
            endpoint,
            connection,
            limits,
            held_streams: Vec::new(),
        })
    }

    /// Return this peer's authenticated Iroh id as Zakura sees it.
    pub fn id(&self) -> Result<ZakuraPeerId, BoxError> {
        Ok(ZakuraPeerId::new(
            self.endpoint.node_id().as_bytes().to_vec(),
        )?)
    }

    /// Open one stream and send a valid prelude and frame.
    pub async fn send_frame(&self, stream_kind: u16, payload: Vec<u8>) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        let frame = Frame {
            message_type: 1,
            flags: 0,
            payload,
        };
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open one stream whose prelude names the given kind and version, then send
    /// a frame. Used to exercise the unknown-kind/unsupported-version rejection
    /// path (FLUP-015): the victim must reset the stream before the frame is
    /// ever delivered to the inbound sink.
    pub async fn send_frame_with_version(
        &self,
        stream_kind: u16,
        stream_version: u16,
        payload: Vec<u8>,
    ) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version,
            request_id: None,
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        let frame = Frame {
            message_type: 1,
            flags: 0,
            payload,
        };
        send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open one stream of `stream_kind` and send `count` valid frames on it.
    ///
    /// Unlike [`send_frame`](Self::send_frame), every frame travels on the SAME
    /// stream, so two calls produce exactly two long-lived same-kind streams.
    /// Used to prove the per-connection, per-kind message-rate bucket is shared
    /// across streams (FLUP-014) rather than re-created per stream. Each payload
    /// is unique so the inbound recorder can be inspected by content.
    pub async fn flood_stream(
        &self,
        stream_kind: u16,
        label: char,
        count: usize,
    ) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        for index in 0..count {
            let frame = Frame {
                message_type: 1,
                flags: 0,
                payload: format!("{label}-{index}").into_bytes(),
            };
            send.write_all(&frame.encode(self.limits.max_frame_bytes)?)
                .await?;
        }
        let _ = send.finish();
        Ok(())
    }

    /// Send a frame header whose declared length exceeds the victim cap.
    pub async fn oversize_frame_declared_len(&self, stream_kind: u16) -> Result<(), BoxError> {
        let (mut send, _recv) = self.connection.open_bi().await?;
        self.write_prelude(&mut send, stream_kind).await?;
        let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 1)?;
        WriteBytesExt::write_u16::<LittleEndian>(&mut header, 0)?;
        WriteBytesExt::write_u32::<LittleEndian>(
            &mut header,
            self.limits.max_frame_bytes.saturating_add(1),
        )?;
        send.write_all(&header).await?;
        let _ = send.finish();
        Ok(())
    }

    /// Open a stream and hold it past the victim's prelude timeout.
    pub async fn open_and_never_send_prelude(&mut self) -> Result<(), BoxError> {
        let (send, _recv) = self.connection.open_bi().await?;
        self.held_streams.push(send);
        tokio::time::sleep(self.limits.prelude_timeout + std::time::Duration::from_millis(20))
            .await;
        Ok(())
    }

    /// Open at most `count` streams quickly, sending valid preludes.
    pub async fn churn_streams(&mut self, stream_kind: u16, count: usize) -> Result<(), BoxError> {
        for _ in 0..count.min(4096) {
            let (mut send, _recv) = self.connection.open_bi().await?;
            self.write_prelude(&mut send, stream_kind).await?;
            self.held_streams.push(send);
        }
        Ok(())
    }

    /// Close the raw endpoint.
    pub async fn shutdown(self) {
        self.connection.close(VarInt::from_u32(0), b"hostile done");
        self.endpoint.close().await;
    }

    async fn write_prelude(&self, send: &mut SendStream, stream_kind: u16) -> Result<(), BoxError> {
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version: 1,
            request_id: None,
            max_frame_bytes: self.limits.max_frame_bytes,
        };
        send.write_all(&prelude.encode()?).await?;
        Ok(())
    }
}
