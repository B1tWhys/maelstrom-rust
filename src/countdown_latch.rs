use std::sync::Arc;
use tokio::sync::Semaphore;

/// Small rust/tokio implementation of a [CountDownLatch] from the Java stdlib, adapted
/// from [this github issue](https://github.com/tokio-rs/tokio/issues/6087)
///
/// [CountDownLatch]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CountDownLatch.html
pub struct CountDownLatch(Arc<Semaphore>);

impl CountDownLatch {
    pub fn new(n: u32) -> (Self, impl Future + Send) {
        let sem = Arc::new(Semaphore::new(0));
        let cdl = Self(Arc::clone(&sem));

        let wait = async move {
            let _ = sem.acquire_many(n).await;
        };

        (cdl, wait)
    }

    pub fn count_down(&mut self) {
        self.0.add_permits(1);
    }
}

// TODO: Prob should have unit tests. I trust that random tokio maintainer tho...
