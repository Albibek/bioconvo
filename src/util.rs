use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use futures::future::Either;
use futures::stream::futures_unordered;
use futures::sync::mpsc::{Sender, UnboundedSender};
use futures::sync::oneshot;
use futures::{Async, Future, IntoFuture, Poll, Sink, Stream};
use resolve::resolver;
use slog::{debug, info, o, warn, Drain, Logger};
use tokio::executor::current_thread::spawn;
use tokio::timer::{Delay, Interval};

use metric::{Metric, MetricType};
pub fn prepare_log(root: &'static str) -> Logger {
    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, slog::Level::Trace).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"test", "test"=>root));
    return rlog;
}

#[derive(Clone, Debug)]
/// Builder for `BackoffRetry`, delays are specified in milliseconds
pub struct BackoffRetryBuilder {
    pub delay: u64,
    pub delay_mul: f32,
    pub delay_max: u64,
    pub retries: usize,
}

impl Default for BackoffRetryBuilder {
    fn default() -> Self {
        Self {
            delay: 250,
            delay_mul: 2f32,
            delay_max: 5000,
            retries: 25,
        }
    }
}

impl BackoffRetryBuilder {
    pub fn spawn<F>(self, action: F) -> BackoffRetry<F>
    where
        F: IntoFuture + Clone,
    {
        let inner = Either::A(action.clone().into_future());
        BackoffRetry {
            action,
            inner: inner,
            options: self,
        }
    }
}

/// TCP client that is able to reconnect with customizable settings
pub struct BackoffRetry<F: IntoFuture> {
    action: F,
    inner: Either<F::Future, Delay>,
    options: BackoffRetryBuilder,
}

impl<F> Future for BackoffRetry<F>
where
    F: IntoFuture + Clone,
{
    type Item = F::Item;
    type Error = Option<F::Error>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let (rotate_f, rotate_t) = match self.inner {
                // we are polling a future currently
                Either::A(ref mut future) => match future.poll() {
                    Ok(Async::Ready(item)) => {
                        return Ok(Async::Ready(item));
                    }
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => {
                        if self.options.retries == 0 {
                            return Err(Some(e));
                        } else {
                            (true, false)
                        }
                    }
                },
                Either::B(ref mut timer) => {
                    match timer.poll() {
                        // we are waiting for the delay
                        Ok(Async::Ready(())) => (false, true),
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Err(_) => unreachable!(), // timer should not return error
                    }
                }
            };

            if rotate_f {
                self.options.retries -= 1;
                let delay = self.options.delay as f32 * self.options.delay_mul;
                let delay = if delay <= self.options.delay_max as f32 {
                    delay as u64
                } else {
                    self.options.delay_max as u64
                };
                let delay = Delay::new(Instant::now() + Duration::from_millis(delay));
                self.inner = Either::B(delay);
            } else if rotate_t {
                self.inner = Either::A(self.action.clone().into_future());
            }
        }
    }
}
