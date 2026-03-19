/// Defines `SharedContainer`, a static `OnceCell`, a `#[ctor::dtor]` cleanup
/// function, and `get_shared_container()` for the given testcontainers image
/// and port.
///
/// NOTE: The same macro is defined in `qbey-mysql/tests/sqlx_mysql/common.rs`.
/// Keep both in sync when making changes.
///
/// Usage:
/// ```ignore
/// define_shared_container!(Postgres, 5432);
/// ```
macro_rules! define_shared_container {
    ($image:ty, $port:expr) => {
        struct SharedContainer {
            container: std::sync::Mutex<Option<testcontainers::ContainerAsync<$image>>>,
            host_port: u16,
        }

        static SHARED_CONTAINER: tokio::sync::OnceCell<SharedContainer> =
            tokio::sync::OnceCell::const_new();
        static DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        // Avoid unwrap() in dtor — panicking in a destructor causes process abort.
        // Errors are intentionally ignored since cleanup is best-effort.
        #[ctor::dtor]
        fn cleanup() {
            if let Some(shared) = SHARED_CONTAINER.get() {
                if let Some(container) = shared.container.lock().ok().and_then(|mut g| g.take()) {
                    if let Ok(rt) = tokio::runtime::Runtime::new() {
                        rt.block_on(async {
                            let _ = container.rm().await;
                        });
                    }
                }
            }
        }

        async fn get_shared_container() -> &'static SharedContainer {
            SHARED_CONTAINER
                .get_or_init(|| async {
                    let container = <$image>::default().start().await.unwrap();
                    let host_port = container.get_host_port_ipv4($port).await.unwrap();
                    SharedContainer {
                        container: std::sync::Mutex::new(Some(container)),
                        host_port,
                    }
                })
                .await
        }
    };
}
