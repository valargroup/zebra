//! Tests and test methods for low-level RocksDB access.

#![allow(clippy::unwrap_in_result)]
#![allow(dead_code)]

use std::ops::Deref;

use crate::service::finalized_state::disk_db::{DiskDb, DB};

// Enable older test code to automatically access the inner database via Deref coercion.
impl Deref for DiskDb {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl DiskDb {
    /// Returns a list of column family names in this database.
    pub fn list_cf(&self) -> Result<Vec<String>, rocksdb::Error> {
        let opts = DiskDb::options();
        let path = self.path();

        rocksdb::DB::list_cf(&opts, path)
    }
}

/// Checkpoint-style (WAL-less) writes are durable in the memtable and readable,
/// mark a flush as pending, and the next WAL-backed write flushes them first
/// (clearing the pending flag) while keeping all earlier data readable.
#[test]
fn write_finalized_block_skips_wal_and_flushes_at_boundary() {
    use std::sync::atomic::Ordering;

    use zebra_chain::{block::Height, parameters::Network};

    use crate::{
        constants::state_database_format_version_in_code,
        service::finalized_state::disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        Config,
    };

    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let column_families = ["default".to_string(), "wal_test".to_string()];

    let db = DiskDb::new(
        &Config::ephemeral(),
        "wal_test_db",
        &state_database_format_version_in_code(),
        &network,
        column_families,
        false,
    );

    let cf = db.cf_handle("wal_test").expect("test column family exists");

    // A WAL-less (checkpoint-verified) write marks a flush as pending and is readable.
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&cf, Height(1), Height(10));
    db.write_finalized_block(batch, true)
        .expect("wal-less write succeeds");

    assert!(
        db.wal_flush_pending.load(Ordering::Acquire),
        "a wal-less write must mark a flush as pending",
    );
    assert_eq!(db.zs_get(&cf, &Height(1)), Some(Height(10)));

    // A WAL-backed (semantically-verified) write flushes the earlier WAL-less
    // write to SST files first, clearing the pending flag, and is itself readable.
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&cf, Height(2), Height(20));
    db.write_finalized_block(batch, false)
        .expect("wal-backed write succeeds");

    assert!(
        !db.wal_flush_pending.load(Ordering::Acquire),
        "a wal-backed write must flush pending wal-less writes and clear the flag",
    );
    assert_eq!(db.zs_get(&cf, &Height(1)), Some(Height(10)));
    assert_eq!(db.zs_get(&cf, &Height(2)), Some(Height(20)));
}

/// Check that zs_iter_opts returns an upper bound one greater than provided inclusive end bounds.
#[test]
fn zs_iter_opts_increments_key_by_one() {
    let _init_guard = zebra_test::init();

    // TODO: add an empty key (`()` type or `[]` when serialized) test case
    let keys: [u32; 14] = [
        0,
        1,
        200,
        255,
        256,
        257,
        65535,
        65536,
        65537,
        16777215,
        16777216,
        16777217,
        16777218,
        u32::MAX,
    ];

    for key in keys {
        let (_, bytes) = DiskDb::zs_iter_bounds(&..=key.to_be_bytes().to_vec());
        let mut extra_bytes = bytes.expect("there should be an upper bound");
        let bytes = extra_bytes.split_off(extra_bytes.len() - 4);
        let upper_bound = u32::from_be_bytes(bytes.clone().try_into().expect("should be 4 bytes"));
        let expected_upper_bound = key.wrapping_add(1);

        assert_eq!(
            expected_upper_bound, upper_bound,
            "the upper bound should be 1 greater than the original key"
        );

        if expected_upper_bound == 0 {
            assert_eq!(
                extra_bytes,
                vec![1],
                "there should be an extra byte with a value of 1"
            );
        } else {
            assert_eq!(extra_bytes.len(), 0, "there should be no extra bytes");
        }
    }
}
