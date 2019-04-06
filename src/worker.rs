use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use luajit::ffi::lua_State;
use luajit::{c_int, ffi, state::ThreadStatus, State};

use futures::prelude::*;
use futures::sync::mpsc::{Receiver, Sender};

use crate::errors::GeneralError;
use crate::funcs;
use crate::Float;
use metric::{Metric, MetricType};
use tokio::runtime::current_thread::Handle;

pub struct Worker {
    luaptr: *mut lua_State, // we need it to call unimplemented functions
    lua: State,
    back_chans: HashMap<String, Sender<(Bytes, Metric<Float>)>>,
    handle: Handle,
}

impl Worker {
    pub fn new<'a>(
        code: &'a str,
        back_chans: HashMap<String, Sender<(Bytes, Metric<Float>)>>,
        handle: Handle,
    ) -> Result<Self, GeneralError> {
        let luaptr = unsafe { ffi::lua_open() };
        let mut state = State::from_ptr(luaptr);
        state.register("log", funcs::log);
        state.register("store", funcs::store);
        match state.do_string(&code) {
            ThreadStatus::Ok => Ok(Self {
                luaptr,
                lua: state,
                back_chans,
                handle,
            }),
            s => Err(GeneralError::Lua(s)),
        }
    }

    fn is_nil(&self) -> bool {
        unsafe { ffi::lua_isnil(self.luaptr, -1) }
    }

    fn is_table(&self) -> bool {
        unsafe { ffi::lua_istable(self.luaptr, -1) }
    }

    fn lua_next(&self) -> c_int {
        unsafe { ffi::lua_next(self.luaptr, -2) }
    }

    pub fn run(&mut self, buf: BytesMut) -> Result<(), GeneralError> {
        let (name, metric) = parse_metric(buf)?;
        // TODO: state.checkstack(4)
        let handlefunc = self.lua.get_global("handle"); // TODO: configurable function name
        if self.is_nil() {
            return Err(GeneralError::LuaRuntime(
                ThreadStatus::RuntimeError,
                "handling function not found".into(),
            ));
        }
        let name = std::str::from_utf8(&name[..])
            .map_err(|_| GeneralError::Parsing("name is bad utf8"))?;
        self.lua.push(name);
        self.lua.push(metric.value);
        self.lua.push(metric.timestamp.unwrap());
        match self.lua.pcall(3, 1, 0) {
            Ok(()) => {
                if self.is_table() {
                    //let mut routes: Vec<String> = Vec::new();
                    //let mut tindex = 1;
                    // push the first key
                    self.lua.push_nil();
                    loop {
                        if self.lua_next() == 0 {
                            break;
                        }
                        // println!("CHECK {:?}", self.lua.is_string(-1));

                        // after lua_next the stack contains key and value
                        let s = self.lua.to_str(-1).unwrap(); // TODO: unwrap

                        let back_name = s.to_string();
                        //routes.push(s.to_string());
                        // leave the key, but pop the value

                        self.lua.pop(1);
                        if let Some(chan) = self.back_chans.get(&back_name) {
                            // TODO Arc<Metric<Float>>
                            self.handle
                                .spawn(
                                    chan.clone()
                                        .send((Bytes::from(name), metric.clone()))
                                        .map_err(|_| ())
                                        .map(|_| ()),
                                )
                                .unwrap();
                        } else {
                            println!("backend '{}' not found", back_name);
                        }
                    }
                //println!("ROUTES: {:?}", routes);
                } else {
                    println!("function didn't return table");
                }

                // pop the return value
                self.lua.pop(1);
                // TODO count stats
                Ok(())
            }
            Err((status, e)) => Err(GeneralError::LuaRuntime(status, e)),
        }
    }
}

fn parse_metric(mut buf: BytesMut) -> Result<(BytesMut, Metric<Float>), GeneralError> {
    // poor man's parsing that still works

    // search name position separately
    let name_pos = &buf[..]
        .iter()
        .position(|c| *c == b' ')
        .ok_or(GeneralError::Parsing("no spaces found"))?;
    let name = buf.split_to(*name_pos);
    buf.advance(1);

    let value_pos = &buf[..]
        .iter()
        .position(|c| *c == b' ')
        .ok_or(GeneralError::Parsing("no spaces found for value"))?;
    let value = std::str::from_utf8(&buf[0..*value_pos])
        .map_err(|_| GeneralError::Parsing("bad metric value"))?;
    let value = value
        .parse::<Float>()
        .map_err(|_| GeneralError::Parsing("value is not a float"))?;
    buf.advance(value_pos + 1);

    let timestamp =
        std::str::from_utf8(&buf[..]).map_err(|_| GeneralError::Parsing("bad timestamp value"))?;
    let timestamp = timestamp
        .parse::<u64>()
        .map_err(|_| GeneralError::Parsing("timestamp is not integer"))?;

    let mut metric = Metric::new(value, MetricType::Gauge(None), None, None)
        .map_err(|_| GeneralError::Parsing("creating metric"))?;

    metric.timestamp = Some(timestamp);
    Ok((name, metric))
}

#[cfg(test)]
mod tests {
    use super::*;
    //   use crate::util::prepare_log;
    //use futures::sync::mpsc::channel;
    //use tokio::runtime::current_thread::{spawn, Runtime};
    //use tokio::timer::Delay;

    #[test]
    fn parsing() {
        let good_metric = "qwer.asdf.zxcv1 10.01 1554473358".into();
        let (name, metric) = parse_metric(good_metric).unwrap();
        assert_eq!(name, "qwer.asdf.zxcv1");
        assert_eq!(metric.value, 10.01f64);
        assert_eq!(metric.timestamp, Some(1554473358));

        //let metric2 = "qwer.asdf.zxcv2".into(), "20".into(), ts.clone()))
    }

    #[test]
    fn worker_lua() {
        let good_metric: BytesMut = "qwer.asdf.zxcv1 10.01 1554473358".into();
        let code = r#"
    function handle(metric, value, timestamp)
        -- log("METRIC:"..metric)
        -- log("VALUE:"..value)
        -- log("TS:"..timestamp)
        local a = true
        if value > 10 then
           a = false
        end
        return {"fuck"}
    end
"#;
        let mut worker = Worker::new(code).unwrap();
        let now = std::time::SystemTime::now();
        for i in 0..1_000_000 {
            worker.run(good_metric.clone()).unwrap();
        }

        println!("TIME: {:?}", now.elapsed());
    }
}
