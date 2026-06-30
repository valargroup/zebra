# Parameter Changes

| Parameter | Location | Old | New | PR | Why |
| --- | --- | --- | --- | --- | --- |
| RocksDB `max_total_wal_size` | `zebra-state/src/service/finalized_state/disk_db.rs` | `0` (unbounded) | 4 GiB | [#345](https://github.com/valargroup/zebra/pull/345) | Bound WAL growth during heavy sync so restarts do not spend minutes replaying tens of GiB of logs. |
