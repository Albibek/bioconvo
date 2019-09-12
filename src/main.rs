// General
pub mod carbon;
pub mod config;
pub mod errors;
pub mod funcs;
pub mod util;
pub mod worker;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{self, Duration, Instant, SystemTime};

use slog::{error, info, o, Drain, Level};

use bytes::Bytes;
use futures::future::{empty, ok};
use futures::sync::mpsc;
use futures::{Future, IntoFuture, Stream};
//use lazy_static::lazy_static;
use slog::warn;

use tokio::runtime::current_thread::{spawn, Runtime};
use tokio::timer::{Delay, Interval};
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    AsyncResolver,
};

//use crate::udp::{start_async_udp, start_sync_udp};
use bioyino_metric::metric::Metric;

use crate::carbon::{CarbonBackend, CarbonServer};
use crate::config::System;
use crate::util::HostOrAddr;

//use crate::consul::ConsulConsensus;
//use crate::errors::GeneralError;
//use crate::management::{MgmtClient, MgmtServer};
//use crate::peer::{NativeProtocolServer, NativeProtocolSnapshot};
//use crate::raft::start_internal_raft;
//use crate::task::{Task, TaskRunner};
//use crate::util::{try_resolve, AggregateOptions, Aggregator, BackoffRetryBuilder, OwnStats, UpdateCounterOptions};

// floating type used all over the code, can be changed to f32, to use less memory at the price of
// precision
// TODO: make in into compilation feature
pub type Float = f64;

// a type to store pre-aggregated data
//pub type Cache = HashMap<Bytes, Metric<Float>>;

// statistic counters
pub static PARSE_ERRORS: AtomicUsize = AtomicUsize::new(0);
pub static PROCESSED_METRICS: AtomicUsize = AtomicUsize::new(0);
pub static LUA_ERRORS: AtomicUsize = AtomicUsize::new(0);

//pub static AGG_ERRORS: AtomicUsize = ATOMIC_USIZE_INIT;
//pub static INGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
//pub static INGRESS_METRICS: AtomicUsize = ATOMIC_USIZE_INIT;
//pub static EGRESS: AtomicUsize = ATOMIC_USIZE_INIT;
//pub static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;
//use lazy_static::lazy_static;
use ccl::dhashmap::DHashMap;
use once_cell::sync::Lazy;

pub static BUCKETS: Lazy<DHashMap<String, DHashMap<Bytes, Metric<Float>>>> =
    Lazy::new(|| DHashMap::default());

fn main() {
    let config = System::load();

    //let config = system.clone();
    let System {
        verbosity,
        //network: Network {
        listen,
        //peer_listen,
        //mgmt_listen,
        //bufsize,
        //multimessage,
        //mm_packets,
        //mm_async,
        //mm_timeout,
        //buffer_flush_time,
        //buffer_flush_length: _,
        //greens,
        //async_sockets,
        //nodes,
        //snapshot_interval,
        //},
        //raft,
        //consul: Consul { start_as: consul_start_as, agent, session_ttl: consul_session_ttl, renew_time: consul_renew_time, key_name: consul_key },
        //metrics: Metrics {
        ////           max_metrics,
        //mut count_updates,
        //update_counter_prefix,
        //update_counter_suffix,
        //update_counter_threshold,
        //fast_aggregation,
        //consistent_parsing: _,
        //log_parse_errors: _,
        //max_unparsed_buffer: _,
        //},
        //carbon,
        n_threads,
        //w_threads,
        //stats_interval: s_interval,
        //task_queue_size,
        //start_as_leader,
        //stats_prefix,
        //consensus,
        code,
        backends,
        ..
    } = config;

    let verbosity = Level::from_str(&verbosity).expect("bad verbosity");

    let mut runtime = Runtime::new().expect("creating runtime for main thread");

    //  let nodes = nodes.into_iter().map(|node| try_resolve(&node)).collect::<Vec<_>>();

    // Set logging
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let filter = slog::LevelFilter::new(drain, verbosity).fuse();
    let drain = slog_async::Async::new(filter).build().fuse();
    let rlog = slog::Logger::root(drain, o!("program"=>"bioconvo"));
    // this lets root logger live as long as it needs
    let _guard = slog_scope::set_global_logger(rlog.clone());

    //let config = Arc::new(config);
    let log = rlog.new(o!("thread" => "main"));

    // create DNS resolver instance
    let (resolver, resolver_task) =
        AsyncResolver::from_system_conf().expect("configuring resolver from system config");

    runtime.spawn(resolver_task);

    // FIXME: unhardcode 100
    let (bufs_tx, bufs_rx) = mpsc::channel(100);
    //   let carbon_log = log.clone();

    // spawn carbon server
    let listen: SocketAddr = "127.0.0.1:2003".parse().unwrap();
    let carbon = CarbonServer::new(listen.clone(), bufs_tx, log.clone());
    runtime.spawn(carbon.into_future().map_err(|_| ())); // TODO: error

    use crossbeam::channel::bounded;
    let (work_tx, work_rx) = bounded(42); // TODO: option for queue length

    //let mut back_chans: HashMap<Bytes, Sender<Arc<Metric<Float>>>> = HashMap::new();
    let mut back_chans = HashMap::new();

    // create backend senders
    for (name, back_config) in backends {
        let log = log.clone();
        let (back_tx, back_rx) = futures::sync::mpsc::channel(42); // TODO think channel size
        let backend_address = HostOrAddr::from_str(&back_config.address, resolver.clone()).expect(
            &format!("parsing backend address `{}`", back_config.address),
        );
        let backend = CarbonBackend::new(backend_address, back_rx, log);
        back_chans.insert(Bytes::from(name), back_tx);
        runtime.spawn(backend.into_future().map_err(|_| ()));
    }

    // read lua code from file
    let mut lua_file = File::open(code).expect("opening lua file");
    let mut lua_code = String::new();
    lua_file
        .read_to_string(&mut lua_code)
        .expect("reading lua file");

    let handle = runtime.handle();
    // spawn worker threads
    for _ in 0..n_threads {
        let work_rx = work_rx.clone();
        let lua_code = lua_code.clone();
        let back_chans = back_chans.clone();
        let handle = handle.clone();
        // TODO builder
        thread::spawn(move || {
            let mut runner =
                worker::Worker::new(&lua_code, back_chans, handle).expect("creating LUA VM");
            work_rx.iter().map(|buf| runner.run(buf)).last();
        });
    }
    drop(work_rx);

    // this is a thread curcuit-breaking the sync processing from async
    // it synchronously reads the receiving part of the async channel and
    // sends data to queue for processing
    // this is the place where scheduling would come in future
    thread::spawn(move || {
        //
        bufs_rx
            .wait()
            .map(|res| {
                match res {
                    Ok(buf) => {
                        work_tx.send(buf).unwrap();
                        true
                    }
                    Err(e) => {
                        println!("ERROR: {:?}", e); // TODO log
                        false
                    }
                }
            })
            .take_while(|&res| res)
            .last();
    });

    // Spawn future gatering bioconvo own stats
    //let own_stat_chan = chans[0].clone();
    //let own_stat_log = rlog.clone();
    //info!(log, "starting own stats counter");
    //let own_stats = OwnStats::new(s_interval, stats_prefix, own_stat_chan, own_stat_log);
    //runtime.spawn(own_stats);

    // // settings safe for asap restart
    //info!(log, "starting snapshot receiver");
    //let peer_server_ret = BackoffRetryBuilder {
    //delay: 1,
    //delay_mul: 1f32,
    //delay_max: 1,
    //retries: ::std::usize::MAX,
    //};
    //let serv_log = rlog.clone();

    //let peer_server = NativeProtocolServer::new(rlog.clone(), peer_listen, chans.clone());
    //let peer_server = peer_server_ret.spawn(peer_server).map_err(move |e| {
    //warn!(serv_log, "shot server gone with error"; "error"=>format!("{:?}", e));
    //});

    // runtime.spawn(peer_server);

    runtime
        .block_on(empty::<(), ()>())
        .expect("running runtime in main thread");
}
