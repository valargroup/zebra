//! Native Zakura block-sync stream messages and service scaffold.

use std::{
    collections::HashMap,
    io::{self, Cursor, Read, Write},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use zebra_chain::{
    block,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};

use super::{Frame, ServicePeerDirection, ServicePeerLimits, ZakuraPeerId};

mod config;
mod error;
mod events;
mod service;
#[cfg(test)]
mod tests;
mod wire;

pub use config::{BlockSyncStatus, ZakuraBlockSyncConfig};
pub use error::BlockSyncWireError;
pub use events::{BlockSyncAction, BlockSyncEvent, BlockSyncMisbehavior};
#[cfg(test)]
pub(crate) use service::block_sync_streams;
pub use service::BlockSyncPeerSession;
pub(crate) use service::{BlockSyncService, MAX_BS_FRAME_BYTES};
pub use wire::{
    BlockSyncMessage, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_MESSAGE_BYTES, MSG_BS_BLOCK,
    MSG_BS_BLOCKS_DONE, MSG_BS_GET_BLOCKS, MSG_BS_RANGE_UNAVAILABLE, MSG_BS_STATUS,
    ZAKURA_BLOCK_SYNC_STREAM_VERSION, ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC,
};
