use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::ops::Range;
use std::time::Duration;

use clap::{
    app_from_crate, crate_authors, crate_description, crate_name, crate_version, value_t, Arg,
    SubCommand,
};
use toml;

use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct System {
    /// Logging level
    pub verbosity: String,

    /// Number of threads, use 0 for number of CPUs
    pub n_threads: usize,

    /// Carbon server listening address
    pub listen: SocketAddr,

    /// queue size for single counting thread before packet is dropped
    pub task_queue_size: usize,

    /// How often to gather own stats, in ms. Use 0 to disable (stats are still gathered, but not included in
    /// metric dump)
    pub stats_interval: u64,

    /// Prefix to send own metrics with
    pub stats_prefix: String,

    /// main file with lua code
    pub code: String,

    /// list of backends configuration
    pub backends: HashMap<String, Backend>,

    /// list of buckets configuration
    pub buckets: HashMap<String, Bucket>,
}

impl Default for System {
    fn default() -> Self {
        Self {
            verbosity: "warn".to_string(),
            n_threads: 4,
            listen: "127.0.0.1:2003".parse().unwrap(),
            task_queue_size: 2048,
            stats_interval: 10000,
            stats_prefix: "resources.monitoring.bioyino".to_string(),
            code: "bioconvo.lua".to_string(),
            backends: HashMap::new(),
            buckets: HashMap::new(),
        }
    }
}

impl System {
    pub fn load() -> Self {
        // This is a first copy of args - with the "config" option
        let app = app_from_crate!()
            .arg(
                Arg::with_name("config")
                    .help("configuration file path")
                    .long("config")
                    .short("c")
                    .required(true)
                    .takes_value(true)
                    .default_value("/etc/bioconvo/bioconvo.toml"),
            )
            .arg(
                Arg::with_name("verbosity")
                    .short("v")
                    .help("logging level")
                    .takes_value(true),
            )
            .subcommand(
                SubCommand::with_name("query")
                    .about("send a management command to running server")
                    .arg(
                        Arg::with_name("host")
                            .short("h")
                            .default_value("127.0.0.1:8137"),
                    )
                    .subcommand(SubCommand::with_name("status").about("get server state"))
                    .subcommand(
                        SubCommand::with_name("consensus")
                            .arg(Arg::with_name("action").index(1))
                            .arg(
                                Arg::with_name("leader_action")
                                    .index(2)
                                    .default_value("unchanged"),
                            ),
                    ),
            )
            .get_matches();

        let config = value_t!(app.value_of("config"), String).expect("config file must be string");
        let mut file = File::open(&config).expect(&format!("opening config file at {}", &config));
        let mut config_str = String::new();
        file.read_to_string(&mut config_str)
            .expect("reading config file");
        let mut system: System = toml::de::from_str(&config_str).expect("parsing config");

        if let Some(v) = app.value_of("verbosity") {
            system.verbosity = v.into()
        }
        system
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Bucket {
    /// time required to expire the bucket
    timer: usize,

    // /// lua function to call when bucket is ready
    // end_function: String,
    /// where to send this bucket data
    routes: Vec<String>,
}

impl Default for Bucket {
    fn default() -> Self {
        Self {
            timer: 30,
            routes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default, deny_unknown_fields)]
pub struct Backend {
    /// time required to expire the bucket
    pub address: String,
    pub port: u16,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".parse().unwrap(),
            port: 2003u16,
        }
    }
}
