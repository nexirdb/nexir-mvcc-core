mod support;

#[cfg(feature = "conformance")]
mod tests {
    use nexir_mvcc_core::InMemoryBackend;

    // This macro generates #[test] functions internally, applying them to the factory provided.
    nexir_mvcc_core::test_backend_conformance!(InMemoryBackend::new);
}
