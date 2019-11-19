use bytes::Bytes;
use ccl::dhashmap::DHashMap;
use log::error;
use luajit::ffi::{self, lua_State};
use luajit::{c_int, State};
use std::cell::RefCell;
use std::collections::HashMap;

use bioyino_metric::{Metric, MetricType};
use std::sync::Arc;

use crate::bucket::BUCKETS;
use crate::util::show_buckets;

// Since lua-related functions need to have some static signature,
// we need this global thread local state to handle additional information
// about current state of the worker
thread_local!(pub(crate) static LUA_THREAD_STATE: RefCell<LuaThreadState> = RefCell::new(LuaThreadState::new()));

pub(crate) struct LuaThreadState {
    pub(crate) name: Bytes,
    pub(crate) metric: Arc<Metric<f64>>,
}

impl LuaThreadState {
    pub(crate) fn new() -> Self {
        Self {
            name: Bytes::new(),
            metric: Arc::new(Metric::new(0f64, MetricType::Gauge(None), None, None).unwrap()),
        }
    }
}

pub unsafe extern "C" fn error_handler(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    match state.to_str(-1) {
        Some(s) => println!("lua error: {}--", s),
        // TODO: make this a lua error somehow
        None => println!("first argument must be string"),
    }

    state.push("fuuuu".to_string());
    return 1;
}

pub unsafe extern "C" fn log(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    match state.to_str(-1) {
        Some(s) => println!("{}", s),
        // TODO: make this a lua error somehow
        None => println!("first argument must be string"),
    }

    0
}

pub unsafe extern "C" fn metric_name(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    let mut name = String::new();
    LUA_THREAD_STATE.with(|state| {
        let state = state.borrow();
        use std::io::Read;
        let mut reader = state.name.as_ref();
        reader.read_to_string(&mut name).unwrap();
    });
    state.push(name);
    1
}

pub unsafe extern "C" fn metric_value(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    let value: f64 = LUA_THREAD_STATE.with(|state| {
        let state = state.borrow();
        //dbg!(state.metric.value);
        state.metric.value
    });
    state.push(value);
    1
}

pub unsafe extern "C" fn metric_timestamp(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    let value: u64 = LUA_THREAD_STATE.with(|state| {
        let state = state.borrow();
        //dbg!(state.metric.value);
        state.metric.timestamp.unwrap()
    });
    state.push(value);
    1
}

pub unsafe extern "C" fn forward(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);

    let stack_size = ffi::lua_gettop(l); // thin ice here, unsafe functions can be called without unsafe block
    if stack_size < 4 {
        error!("store called with not enough arguments");
        return -1;
    }

    let bucket_name: &str;
    match state.to_str(-4) {
        Some(s) => bucket_name = s,
        None => error!("'forward' function received non-string argument"),
    };
    0
}

pub unsafe extern "C" fn aggregate(l: *mut lua_State) -> c_int {
    let mut state = State::from_ptr(l);
    let mut bucket_name: String = String::new();
    let mut name: Bytes = Bytes::new();
    let mut value: f64 = 0.0;
    let mut timestamp: u64 = 0;

    let stack_size = ffi::lua_gettop(l); // thin ice here, unsafe functions can be called without unsafe block
    if stack_size < 4 {
        println!("store called with not enough arguments");
        return -1;
    }

    match state.to_str(-4) {
        Some(s) => bucket_name = s.into(),
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

    store_carbon(&name);
    show_buckets();

    0
}
