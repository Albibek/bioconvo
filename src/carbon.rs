use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{self, Duration, Instant, SystemTime};

use bytes::{BufMut, Bytes, BytesMut};
use failure::Error;
use ftoa;
use futures::stream;
use futures::sync::mpsc::Sender;
use futures::{Future, IntoFuture, Sink, Stream};
use slog::{info, warn, Logger};
use tokio::net::{TcpListener, TcpStream};

use tokio_codec::{Decoder, Encoder};

use crate::errors::GeneralError;
use crate::Float;
use metric::{Metric, MetricType};

#[derive(Clone)]
pub struct CarbonServer {
    addr: SocketAddr,
    channel: Sender<BytesMut>,
    //channel: Sender<Metric<Float>>,
    log: Logger,
}

impl CarbonServer {
    //pub(crate) fn new(addr: SocketAddr, channel: Sender<Metric<Float>>, log: Logger) -> Self {
    pub(crate) fn new(addr: SocketAddr, channel: Sender<BytesMut>, log: Logger) -> Self {
        Self { addr, channel, log }
    }
}

impl IntoFuture for CarbonServer {
    type Item = ();
    type Error = GeneralError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { addr, channel, log } = self;

        //info!(log, "carbon  sending metrics");
        let future = TcpListener::bind(&addr)
            .expect("listening peer port")
            .incoming()
            .map_err(|e| GeneralError::Io(e))
            .for_each(move |conn| {
                let elog = log.clone();
                let blog = log.clone();
                let reader = CarbonDecoder::new(elog.clone()).framed(conn);
                reader
                    .map_err(|_| GeneralError::CarbonServer)
                    .forward(channel.clone().sink_map_err(|_| GeneralError::CarbonServer))
                    .map(move |_| info!(blog, "carbon server finished"))
                    .map_err(move |e| {
                        info!(elog, "carbon server error");
                        e
                    })
            });
        Box::new(future)
    }
}

/*
#[derive(Clone)]
pub struct CarbonBackend {
    addr: SocketAddr,
    metrics: Arc<Vec<(Bytes, Bytes, Bytes)>>,
    log: Logger,
}

impl CarbonBackend {
    pub(crate) fn new(
        addr: SocketAddr,
        ts: Duration,
        metrics: Arc<Vec<(Bytes, Float)>>,
        log: Logger,
    ) -> Self {
        let ts: Bytes = ts.as_secs().to_string().into();

        let buf = BytesMut::with_capacity(metrics.len() * 200); // 200 is an approximate for full metric name + value
        let (metrics, _) =
            metrics
                .iter()
                .fold((Vec::new(), buf), |(mut acc, mut buf), (name, metric)| {
                    let mut wr = buf.writer();
                    let buf = match ftoa::write(&mut wr, *metric) {
                        Ok(()) => {
                            buf = wr.into_inner();
                            let metric = buf.take().freeze();
                            acc.push((name.clone(), metric, ts.clone()));
                            buf
                        }
                        Err(_) => {
                            AGG_ERRORS.fetch_add(1, Ordering::Relaxed);
                            wr.into_inner()
                        }
                    };
                    (acc, buf)
                });
        let metrics = Arc::new(metrics);
        let self_ = Self { addr, metrics, log };
        self_
    }
}

impl IntoFuture for CarbonBackend {
    type Item = ();
    type Error = GeneralError;
    type Future = Box<Future<Item = Self::Item, Error = Self::Error>>;

    fn into_future(self) -> Self::Future {
        let Self { addr, metrics, log } = self;

        let conn = TcpStream::connect(&addr).map_err(|e| GeneralError::Io(e));
        let elog = log.clone();
        let future = conn.and_then(move |conn| {
            info!(log, "carbon backend sending metrics");
            let writer = CarbonCodec::new().framed(conn);
            let metric_stream = stream::iter_ok::<_, ()>(SharedIter::new(metrics));
            metric_stream
                .map_err(|_| GeneralError::CarbonBackend)
                .forward(writer.sink_map_err(|_| GeneralError::CarbonBackend))
                .map(move |_| info!(log, "carbon backend finished"))
                .map_err(move |e| {
                    info!(elog, "carbon backend error");
                    e
                })
        });

        Box::new(future)
    }
}

pub struct SharedIter<T> {
    inner: Arc<Vec<T>>,
    current: usize,
}

impl<T> SharedIter<T> {
    pub fn new(inner: Arc<Vec<T>>) -> Self {
        Self { inner, current: 0 }
    }
}

impl<T: Clone> Iterator for SharedIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let n = self.inner.get(self.current).map(|i| i.clone());
        self.current += 1;
        n
    }
}

*/

pub struct CarbonDecoder {
    metric: Metric<Float>,
    log: Logger,
    last_pos: usize,
}

impl CarbonDecoder {
    pub fn new(log: Logger) -> Self {
        Self {
            metric: Metric::new(0f64, MetricType::Gauge(None), None, None).unwrap(),
            log,
            last_pos: 0,
        }
    }
}

impl Decoder for CarbonDecoder {
    type Item = BytesMut;
    // It could be a separate error here, but it's useless, since there is no errors in process of
    // encoding
    type Error = GeneralError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // FIXME: move max_len to options
        let max_len = 10000;
        let mut len = buf.len();
        let last = len - 1;
        if len == 0 {
            // code below uses len - 1
            self.last_pos = 0;
            return Ok(None);
        }

        if self.last_pos < len {
            if let Some(pos) = &buf[self.last_pos..last].iter().position(|c| *c == b'\n') {
                // try to find position of newline
                self.last_pos = 0;
                let metric = buf.split_to(*pos);
                buf.advance(1); // remove \n itself
                return Ok(Some(metric));
            } else {
                self.last_pos = len - 1;
                return Ok(None);
            }
        }

        if len > max_len {
            buf.split_to(len);
            return Err(GeneralError::MetricTooLong);
        } else {
            // TODO: not sure about None here
            return Ok(None);
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // send leftovers if any
        if buf.len() == 0 {
            Ok(None)
        } else {
            //TODO this may not be correct
            let mut buf = buf.take();
            // cut all '\n' at the end
            loop {
                let len = buf.len();
                if len > 0 {
                    if buf[len - 1] == b'\n' {
                        buf.truncate(len - 1);
                    } else {
                        break;
                    }
                }
            }
            Ok(Some(buf))
        }
    }
}

impl Encoder for CarbonDecoder {
    type Item = ();
    type Error = GeneralError;

    fn encode(&mut self, _: Self::Item, _buf: &mut BytesMut) -> Result<(), Self::Error> {
        unreachable!()
    }
}

pub struct CarbonEncoder;

impl Encoder for CarbonEncoder {
    type Item = (Bytes, Bytes, Bytes); // Metric name, value and timestamp
    type Error = Error;

    fn encode(&mut self, m: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let len = m.0.len() + 1 + m.1.len() + 1 + m.2.len() + 1;
        buf.reserve(len);
        buf.put(m.0);
        buf.put(" ");
        buf.put(m.1);
        buf.put(" ");
        buf.put(m.2);
        buf.put("\n");
        Ok(())
    }
}

impl Decoder for CarbonEncoder {
    type Item = ();
    // It could be a separate error here, but it's useless, since there is no errors in process of
    // encoding
    type Error = GeneralError;

    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prepare_log;
    use futures::sync::mpsc::channel;
    use tokio::runtime::current_thread::{spawn, Runtime};
    use tokio::timer::Delay;

    #[test]
    fn carbon_server() {
        let ts = SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap();
        let ts: Bytes = ts.as_secs().to_string().into();

        let mut runtime = Runtime::new().expect("creating runtime for main thread");
        let (bufs_tx, bufs_rx) = channel(10);
        let log = prepare_log("carbon_server_test");

        let rlog = log.clone();
        // spawn carbon server
        let listen: SocketAddr = "127.0.0.1:2003".parse().unwrap();
        let server = CarbonServer::new(listen.clone(), bufs_tx, log);
        runtime.spawn(server.into_future().map_err(|_| ())); // error
        let receiver = bufs_rx.for_each(move |msg| {
            println!("RECV: {:?}", msg);
            Ok(())
        });

        runtime.spawn(receiver);

        let sender = TcpStream::connect(&listen).and_then(move |conn| {
            let writer = CarbonEncoder.framed(conn);
            let sender = writer
                .send(("qwer.asdf.zxcv1".into(), "10".into(), ts.clone()))
                .and_then(move |writer| {
                    writer.send(("qwer.asdf.zxcv2".into(), "20".into(), ts.clone()))
                })
                .map(|_| ())
                .map_err(|_| ());
            spawn(sender.map_err(|_| ()));
            Ok(())
        });

        runtime.spawn(sender.map_err(|_| ()));
        //runtime.block_on(receiver).expect("runtime");
        let test_timeout = Instant::now() + Duration::from_secs(2);
        // let d = Delay::new(Instant::now() + Duration::from_secs(1));
        //let delayed = d.map_err(|_| ()).and_then(|_| sender);
        //runtime.spawn(delayed);

        let test_delay = Delay::new(test_timeout);
        runtime.block_on(test_delay).expect("runtime");
    }
}
