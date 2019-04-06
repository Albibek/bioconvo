use bytes::Bytes;
use luajit::ffi::lua_State;
use luajit::{c_int, State};
use std::collections::HashMap;

use crate::BUCKETS;

pub unsafe extern "C" fn log(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    match state.to_str(-1) {
        Some(s) => println!("{}", s),
        // TODO: make this a lua error somehow
        None => println!("first argument must be string"),
    }

    0
}

pub unsafe extern "C" fn store(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    let mut bucket: String = String::new();
    let mut name: Bytes = Bytes::new();
    let mut value: f64 = 0.0;
    let mut timestamp: u64 = 0;
    match state.to_str(-4) {
        Some(s) => bucket = s.into(),
        None => println!("bucket name argument must be string"),
    }
    match state.to_str(-3) {
        Some(s) => name = Bytes::from(s),
        None => println!("name argument must be string"),
    }
    match state.to_double(-2) {
        Some(s) => value = s,
        None => println!("value argument must be double"),
    }
    match state.to_long(-1) {
        Some(s) => timestamp = s as u64,
        None => println!("timestamp argument must be long"),
    }

    let mut buckets = (*BUCKETS).write().unwrap();
    let bucket = buckets.entry(bucket.clone()).or_insert(HashMap::new());
    //let b_entry =

    println!("STORE {:?} {:?} {:?} {:?}", bucket, name, value, timestamp);
    0
}
