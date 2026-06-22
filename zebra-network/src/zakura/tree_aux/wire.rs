//! `tree_aux` stream wire messages and codec (design §5.4).
//!
//! A roots-only request/response stream: a client sends [`TreeAuxMessage::GetRoots`] and
//! the server answers with [`TreeAuxMessage::Roots`] (or
//! [`TreeAuxMessage::RangeUnavailable`]). The checkpoint final frontier is embedded in
//! the binary, not carried here, so there is no frontier message.

use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;
use zebra_chain::{
    block,
    parallel::commitment_aux::BlockCommitmentRoots,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};

use super::Frame;

/// Zakura stream kind reserved for the verified-commitment-trees `tree_aux` stream.
pub const ZAKURA_STREAM_TREE_AUX: u16 = 7;
/// Capability bit advertised for `tree_aux` during the handshake.
pub const ZAKURA_CAP_TREE_AUX: u64 = 1 << 4;
/// Version of the `tree_aux` stream.
pub const ZAKURA_TREE_AUX_STREAM_VERSION: u16 = 1;

/// `Status` advertisement message type.
pub const MSG_TA_STATUS: u16 = 1;
/// `GetRoots` request message type.
pub const MSG_TA_GET_ROOTS: u16 = 2;
/// `Roots` response message type.
pub const MSG_TA_ROOTS: u16 = 3;
/// `RangeUnavailable` response message type.
pub const MSG_TA_RANGE_UNAVAILABLE: u16 = 4;

/// Maximum per-block roots a single `GetRoots`/`Roots` may request or return.
pub const MAX_TA_ROOTS_PER_REQUEST: u32 = 4000;
/// Maximum encoded `tree_aux` message bytes (each root is ~68 bytes; 4000 fit easily).
pub const MAX_TA_MESSAGE_BYTES: usize = 1024 * 1024;

/// A `tree_aux` wire codec error.
#[derive(Clone, Debug, Error)]
#[allow(missing_docs)]
pub enum TreeAuxWireError {
    #[error("unknown tree_aux message type: {0}")]
    UnknownMessageType(u16),
    #[error("unsupported tree_aux frame flags: {0}")]
    UnsupportedFlags(u16),
    #[error("tree_aux root count {actual} exceeds the limit {max}")]
    RootCountLimit { actual: u32, max: u32 },
    #[error("tree_aux payload too large: {actual} > {max}")]
    OversizedPayload { actual: usize, max: usize },
    #[error("tree_aux message has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("tree_aux serialization error: {0}")]
    Serialization(#[from] SerializationError),
    #[error("tree_aux io error: {0}")]
    Io(String),
}

impl From<std::io::Error> for TreeAuxWireError {
    fn from(error: std::io::Error) -> Self {
        TreeAuxWireError::Io(error.to_string())
    }
}

/// A typed `tree_aux` message (design §5.4). Roots-only — the final frontier is embedded
/// in the binary, not on the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeAuxMessage {
    /// Advertise the servable height range `[servable_low, servable_high]`.
    Status {
        /// Lowest servable height.
        servable_low: block::Height,
        /// Highest servable height.
        servable_high: block::Height,
    },
    /// Request `count` per-block roots starting at `start_height`.
    GetRoots {
        /// First requested height.
        start_height: block::Height,
        /// Requested root count.
        count: u32,
    },
    /// A contiguous run of per-block roots in ascending height order.
    Roots {
        /// The roots, ascending by height.
        roots: Vec<BlockCommitmentRoots>,
    },
    /// The peer cannot serve this range.
    RangeUnavailable {
        /// First unavailable height.
        start_height: block::Height,
        /// Unavailable count.
        count: u32,
    },
}

impl TreeAuxMessage {
    /// This message's wire discriminator.
    pub fn message_type(&self) -> u16 {
        match self {
            Self::Status { .. } => MSG_TA_STATUS,
            Self::GetRoots { .. } => MSG_TA_GET_ROOTS,
            Self::Roots { .. } => MSG_TA_ROOTS,
            Self::RangeUnavailable { .. } => MSG_TA_RANGE_UNAVAILABLE,
        }
    }

    /// Encode this message to a Zakura wire [`Frame`].
    pub fn encode_frame(&self) -> Result<Frame, TreeAuxWireError> {
        let mut payload = Vec::new();
        match self {
            Self::Status {
                servable_low,
                servable_high,
            } => {
                payload.write_u32::<LittleEndian>(servable_low.0)?;
                payload.write_u32::<LittleEndian>(servable_high.0)?;
            }
            Self::GetRoots {
                start_height,
                count,
            } => {
                if *count > MAX_TA_ROOTS_PER_REQUEST {
                    return Err(TreeAuxWireError::RootCountLimit {
                        actual: *count,
                        max: MAX_TA_ROOTS_PER_REQUEST,
                    });
                }
                payload.write_u32::<LittleEndian>(start_height.0)?;
                payload.write_u32::<LittleEndian>(*count)?;
            }
            Self::Roots { roots } => {
                let count = u32::try_from(roots.len()).unwrap_or(u32::MAX);
                if count > MAX_TA_ROOTS_PER_REQUEST {
                    return Err(TreeAuxWireError::RootCountLimit {
                        actual: count,
                        max: MAX_TA_ROOTS_PER_REQUEST,
                    });
                }
                payload.write_u32::<LittleEndian>(count)?;
                for root in roots {
                    root.zcash_serialize(&mut payload)?;
                }
            }
            Self::RangeUnavailable {
                start_height,
                count,
            } => {
                payload.write_u32::<LittleEndian>(start_height.0)?;
                payload.write_u32::<LittleEndian>(*count)?;
            }
        }

        if payload.len() > MAX_TA_MESSAGE_BYTES {
            return Err(TreeAuxWireError::OversizedPayload {
                actual: payload.len(),
                max: MAX_TA_MESSAGE_BYTES,
            });
        }

        Ok(Frame {
            message_type: self.message_type(),
            flags: 0,
            payload,
        })
    }

    /// Decode a Zakura wire [`Frame`] into a typed message, rejecting malformed input.
    pub fn decode_frame(frame: Frame) -> Result<Self, TreeAuxWireError> {
        if frame.flags != 0 {
            return Err(TreeAuxWireError::UnsupportedFlags(frame.flags));
        }
        if frame.payload.len() > MAX_TA_MESSAGE_BYTES {
            return Err(TreeAuxWireError::OversizedPayload {
                actual: frame.payload.len(),
                max: MAX_TA_MESSAGE_BYTES,
            });
        }

        let mut reader = Cursor::new(frame.payload.as_slice());
        let message = match frame.message_type {
            MSG_TA_STATUS => Self::Status {
                servable_low: block::Height(reader.read_u32::<LittleEndian>()?),
                servable_high: block::Height(reader.read_u32::<LittleEndian>()?),
            },
            MSG_TA_GET_ROOTS => {
                let start_height = block::Height(reader.read_u32::<LittleEndian>()?);
                let count = reader.read_u32::<LittleEndian>()?;
                if count > MAX_TA_ROOTS_PER_REQUEST {
                    return Err(TreeAuxWireError::RootCountLimit {
                        actual: count,
                        max: MAX_TA_ROOTS_PER_REQUEST,
                    });
                }
                Self::GetRoots {
                    start_height,
                    count,
                }
            }
            MSG_TA_ROOTS => {
                let count = reader.read_u32::<LittleEndian>()?;
                if count > MAX_TA_ROOTS_PER_REQUEST {
                    return Err(TreeAuxWireError::RootCountLimit {
                        actual: count,
                        max: MAX_TA_ROOTS_PER_REQUEST,
                    });
                }
                // Grow as roots are read; never preallocate from the untrusted count.
                let mut roots = Vec::new();
                for _ in 0..count {
                    roots.push(BlockCommitmentRoots::zcash_deserialize(&mut reader)?);
                }
                Self::Roots { roots }
            }
            MSG_TA_RANGE_UNAVAILABLE => Self::RangeUnavailable {
                start_height: block::Height(reader.read_u32::<LittleEndian>()?),
                count: reader.read_u32::<LittleEndian>()?,
            },
            other => return Err(TreeAuxWireError::UnknownMessageType(other)),
        };

        let consumed = reader.position() as usize;
        if consumed != frame.payload.len() {
            return Err(TreeAuxWireError::TrailingBytes(
                frame.payload.len() - consumed,
            ));
        }

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_roots(n: u32) -> Vec<BlockCommitmentRoots> {
        (0..n)
            .map(|i| BlockCommitmentRoots {
                height: block::Height(1_687_100 + i),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            })
            .collect()
    }

    use zebra_chain::{orchard, sapling};

    #[test]
    fn tree_aux_messages_round_trip() {
        let messages = vec![
            TreeAuxMessage::Status {
                servable_low: block::Height(1),
                servable_high: block::Height(1_700_000),
            },
            TreeAuxMessage::GetRoots {
                start_height: block::Height(1_687_104),
                count: 128,
            },
            TreeAuxMessage::Roots {
                roots: sample_roots(5),
            },
            TreeAuxMessage::RangeUnavailable {
                start_height: block::Height(99),
                count: 10,
            },
        ];

        for message in messages {
            let frame = message.encode_frame().expect("encodes");
            let decoded = TreeAuxMessage::decode_frame(frame).expect("decodes");
            assert_eq!(decoded, message, "{message:?} round-trips on the wire");
        }
    }

    #[test]
    fn get_roots_over_limit_is_rejected() {
        let over = TreeAuxMessage::GetRoots {
            start_height: block::Height(1),
            count: MAX_TA_ROOTS_PER_REQUEST + 1,
        };
        assert!(
            matches!(
                over.encode_frame(),
                Err(TreeAuxWireError::RootCountLimit { .. })
            ),
            "an over-limit GetRoots is rejected by the codec"
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut frame = TreeAuxMessage::GetRoots {
            start_height: block::Height(1),
            count: 1,
        }
        .encode_frame()
        .expect("encodes");
        frame.payload.push(0xff);
        assert!(
            matches!(
                TreeAuxMessage::decode_frame(frame),
                Err(TreeAuxWireError::TrailingBytes(1))
            ),
            "a frame with trailing bytes is rejected"
        );
    }
}
