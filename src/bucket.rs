use ccl::dhashmap::DHashMap;

use bytes::Bytes;
use once_cell::sync::Lazy;
use serde_derive::{Deserialize, Serialize};

use crate::Float;
use bioyino_metric::metric::{Metric, MetricType};

pub static BUCKETS: Lazy<DHashMap<String, DHashMap<Bytes, Metric<Float>>>> =
    Lazy::new(|| DHashMap::default());

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

pub fn store_carbon(bucket_name: &str, name: Bytes, value: Float, timestamp: u64) {
    let bucket = BUCKETS.get_or_insert_with(&String::from(bucket_name), || DHashMap::default());
    match bucket.get_mut(&name) {
        Some(metric) => {
            //
            //        println!("OLD");
        }
        None => {
            bucket.insert(
                name.clone(),
                Metric::new(value, MetricType::Gauge(None), Some(timestamp), None).unwrap(), // TODO: unwrap
            );
        }
    };
    drop(bucket);
}
