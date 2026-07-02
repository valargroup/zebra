//! Finalized state tests.

#![allow(clippy::unwrap_in_result)]

use zebra_chain::{block::Height, parameters::Network};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE},
    Config,
};

mod prop;
mod rollback;
mod transparent;
mod vectors;

#[test]
fn checkpoint_prune_range_retains_current_height_when_range_ends_before_it() {
    let current_height = Height(9);

    assert!(
        super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), current_height)),
        ),
        "raw transactions are still needed when the prune range ends before the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(
            current_height,
            Some((Height(1), Height(10))),
        ),
        "raw transactions can be skipped when the prune range covers the current height"
    );

    assert!(
        !super::checkpoint_prune_range_retains_current_height(current_height, None),
        "no checkpoint prune range means there is no archive backlog to drain"
    );
}

#[test]
#[should_panic(expected = "cannot open read-only state: no database found")]
fn read_only_open_rejects_empty_cache_dir_without_creating_database() {
    let _init_guard = zebra_test::init();

    let tempdir = tempfile::tempdir().expect("temporary cache directory is created");
    let config = Config {
        cache_dir: tempdir.path().to_path_buf(),
        ..Config::default()
    };
    let network = Network::Mainnet;
    let expected_db_path = config.db_path(
        STATE_DATABASE_KIND,
        state_database_format_version_in_code().major,
        &network,
    );

    let result = std::panic::catch_unwind(|| {
        ZebraDb::new(
            &config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            true,
        );
    });

    assert!(result.is_err());
    assert!(
        !expected_db_path.exists(),
        "read-only open must not create a missing primary database path"
    );

    std::panic::resume_unwind(result.expect_err("read-only open should panic"));
}
