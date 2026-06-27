use color_eyre::eyre::{eyre, Result, WrapErr};

use zebra_chain::{block, orchard, parallel::commitment_aux::BlockCommitmentRoots, sapling};

use crate::fetch::{roots_path, CachedRoots};

pub fn parse_cached_roots(
    cache_dir: &std::path::Path,
    height: block::Height,
) -> Result<BlockCommitmentRoots> {
    let path = roots_path(cache_dir, height.0);
    let raw = std::fs::read(&path).wrap_err_with(|| {
        format!(
            "missing cached roots {} — run `fetch --with-roots` first",
            path.display()
        )
    })?;
    let cached: CachedRoots = serde_json::from_slice(&raw)
        .wrap_err_with(|| format!("parsing cached roots {}", path.display()))?;
    let sapling_hex = cached
        .sapling
        .ok_or_else(|| eyre!("cached roots {} has no sapling finalRoot", path.display()))?;
    let sapling_root =
        parse_sapling_root(&sapling_hex).wrap_err_with(|| format!("sapling root {}", height.0))?;
    let orchard_root = match cached.orchard {
        Some(hex) => {
            parse_orchard_root(&hex).wrap_err_with(|| format!("orchard root {}", height.0))?
        }
        None => orchard::tree::NoteCommitmentTree::default().root(),
    };

    Ok(BlockCommitmentRoots {
        height,
        sapling_root,
        orchard_root,
    })
}

fn decode_root_bytes(hex_str: &str) -> Result<[u8; 32]> {
    let raw = hex::decode(hex_str.trim()).wrap_err("root hex decode")?;
    <[u8; 32]>::try_from(raw.as_slice()).map_err(|_| eyre!("root hex was not 32 bytes"))
}

// z_gettreestate returns Sapling roots in display order, the reverse of Zebra's
// internal root bytes.
pub fn parse_sapling_root(hex_str: &str) -> Result<sapling::tree::Root> {
    let mut bytes = decode_root_bytes(hex_str)?;
    bytes.reverse();
    sapling::tree::Root::try_from(bytes).map_err(|e| eyre!("invalid sapling root: {e:?}"))
}

// z_gettreestate returns Orchard roots in the same byte order Zebra stores.
pub fn parse_orchard_root(hex_str: &str) -> Result<orchard::tree::Root> {
    let bytes = decode_root_bytes(hex_str)?;
    orchard::tree::Root::try_from(bytes).map_err(|e| eyre!("invalid orchard root: {e:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sapling_rpc_display_order_is_reversed() {
        let root = sapling::tree::NoteCommitmentTree::default().root();
        let display_hex = hex::encode(root.bytes_in_display_order());

        assert_eq!(parse_sapling_root(&display_hex).unwrap(), root);
    }

    #[test]
    fn orchard_rpc_display_order_is_not_reversed() {
        let root = orchard::tree::NoteCommitmentTree::default().root();
        let display_hex = hex::encode(root.bytes_in_display_order());

        assert_eq!(parse_orchard_root(&display_hex).unwrap(), root);
    }
}
