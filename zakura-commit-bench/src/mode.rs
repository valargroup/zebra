use clap::ValueEnum;

/// Which replay path the benchmark should use.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum RunMode {
    /// Current benchmark path: cached blocks go straight into the real verifier/state stack.
    DirectVerifier,
    /// Full replay path: cached bodies feed block-sync peers, the sequencer, applyQ, and Committer.
    ApplyQueue,
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectVerifier => "direct-verifier",
            Self::ApplyQueue => "apply-queue",
        }
    }
}
