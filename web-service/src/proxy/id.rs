use gen_id::{ConfigPreset::ShortEpochMaxNodes, IdGenerator, DEFAULT_EPOCH};
use std::sync::OnceLock;

fn generator() -> &'static IdGenerator {
    static GENERATOR: OnceLock<IdGenerator> = OnceLock::new();
    GENERATOR.get_or_init(|| IdGenerator::new(ShortEpochMaxNodes, DEFAULT_EPOCH))
}

pub(crate) fn next_id() -> u64 {
    generator().next_id(1)
}

pub(crate) fn next_id_string() -> String {
    next_id().to_string()
}
