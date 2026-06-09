//! zcashd-compat mode configuration and `zcashd` child-process supervision.

mod config;
mod manifest;
mod managed;
mod supervisor;

pub use config::{Config, ZcashdBinarySource as ConfigZcashdBinarySource};
pub use manifest::{
    EMBEDDED_MANIFEST_SCHEMA_VERSION, EMBEDDED_ZCASHD_RELEASE_MANIFEST, ZcashdReleaseArtifact,
    ZcashdReleaseManifest,
};
pub use managed::{
    effective_zcashd_source, resolve_managed_zcashd_binary, resolve_zcashd_binary_path,
    zcashd_target_triple, ZcashdBinarySource,
};
pub use supervisor::{is_command_resolvable, run as run_supervisor, SupervisorConfig};
