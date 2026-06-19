// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::io::Cursor;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use zebra_chain::{
    block::{
        merkle::{AuthDataRoot, AUTH_DIGEST_PLACEHOLDER},
        tests::generate::{
            large_multi_transaction_block, large_single_transaction_block_many_inputs,
        },
        Block,
    },
    serialization::{ZcashDeserialize, ZcashSerialize},
    transparent,
};
use zebra_test::vectors::{
    BLOCK_MAINNET_1687107_BYTES, BLOCK_MAINNET_1687121_BYTES, BLOCK_TESTNET_141042_BYTES,
};

const MIN_PARALLEL_CHECKPOINT_PREPARE_TRANSACTIONS: usize = 16;

fn block_serialization(c: &mut Criterion) {
    // Biggest block from `zebra-test`.
    let block141042_bytes: &[u8] = BLOCK_TESTNET_141042_BYTES.as_ref();
    let block141042 = Block::zcash_deserialize(Cursor::new(block141042_bytes)).unwrap();

    let blocks = vec![
        ("BLOCK_TESTNET_141042", block141042),
        (
            "large_multi_transaction_block",
            large_multi_transaction_block(),
        ),
        (
            "large_single_transaction_block_many_inputs",
            large_single_transaction_block_many_inputs(),
        ),
    ];

    for (name, block) in blocks {
        c.bench_with_input(
            BenchmarkId::new("zcash_serialize_to_vec", name),
            &block,
            |b, block| b.iter(|| block.zcash_serialize_to_vec().unwrap()),
        );

        let block_bytes = block.zcash_serialize_to_vec().unwrap();
        c.bench_with_input(
            BenchmarkId::new("zcash_deserialize", name),
            &block_bytes,
            |b, bytes| b.iter(|| Block::zcash_deserialize(Cursor::new(bytes)).unwrap()),
        );
    }
}

fn checkpoint_prepare_substages(c: &mut Criterion) {
    let blocks = vec![
        (
            "BLOCK_TESTNET_141042",
            Block::zcash_deserialize(Cursor::new(BLOCK_TESTNET_141042_BYTES.as_slice())).unwrap(),
        ),
        (
            "BLOCK_MAINNET_1687107",
            Block::zcash_deserialize(Cursor::new(BLOCK_MAINNET_1687107_BYTES.as_slice())).unwrap(),
        ),
        (
            "BLOCK_MAINNET_1687121",
            Block::zcash_deserialize(Cursor::new(BLOCK_MAINNET_1687121_BYTES.as_slice())).unwrap(),
        ),
        (
            "large_multi_transaction_block",
            large_multi_transaction_block(),
        ),
    ];

    let mut group = c.benchmark_group("Checkpoint Prepare Substages");

    for (name, block) in blocks {
        let (transaction_hashes, auth_digests): (Vec<_>, Vec<_>) = {
            if block.transactions.len() < MIN_PARALLEL_CHECKPOINT_PREPARE_TRANSACTIONS {
                block
                    .transactions
                    .iter()
                    .map(|tx| tx.txid_and_auth_digest())
                    .unzip()
            } else {
                use rayon::prelude::*;
                block
                    .transactions
                    .par_iter()
                    .map(|tx| tx.txid_and_auth_digest())
                    .unzip()
            }
        };
        group.bench_with_input(
            BenchmarkId::new("txid_auth_digest", name),
            &block,
            |b, block| {
                b.iter(|| {
                    let digests: (Vec<_>, Vec<_>) = if block.transactions.len()
                        < MIN_PARALLEL_CHECKPOINT_PREPARE_TRANSACTIONS
                    {
                        block
                            .transactions
                            .iter()
                            .map(|tx| tx.txid_and_auth_digest())
                            .unzip()
                    } else {
                        use rayon::prelude::*;
                        block
                            .transactions
                            .par_iter()
                            .map(|tx| tx.txid_and_auth_digest())
                            .unzip()
                    };
                    digests
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("auth_data_root", name),
            &auth_digests,
            |b, auth_digests| {
                b.iter_batched(
                    || auth_digests.clone(),
                    |auth_digests| {
                        auth_digests
                            .into_iter()
                            .map(|auth_digest| auth_digest.unwrap_or(AUTH_DIGEST_PLACEHOLDER))
                            .collect::<AuthDataRoot>()
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        if block.coinbase_height().is_some() {
            group.bench_function(BenchmarkId::new("new_ordered_outputs", name), |b| {
                b.iter(|| transparent::new_ordered_outputs(&block, &transaction_hashes))
            });
        }
    }

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().noise_threshold(0.05).sample_size(50);
    targets = block_serialization, checkpoint_prepare_substages
);
criterion_main!(benches);
