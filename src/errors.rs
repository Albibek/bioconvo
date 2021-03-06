use failure_derive::Fail;
use luajit::state::ThreadStatus;
use std::net::SocketAddr;

#[derive(Fail, Debug)]
pub enum GeneralError {
    #[fail(display = "I/O error")]
    Io(#[cause] ::std::io::Error),

    #[fail(display = "Error when creating timer: {}", _0)]
    Timer(#[cause] ::tokio::timer::Error),

    #[fail(display = "getting system time")]
    Time(#[cause] ::std::time::SystemTimeError),

    #[fail(display = "Gave up connecting to {}", _0)]
    TcpOutOfTries(SocketAddr),

    #[fail(display = "Could not parse address")]
    AddressParse,

    #[fail(display = "Could not resolve address")]
    AddressResolve, // TODO add address

    #[fail(display = "Carbon server failure")]
    CarbonServer,

    #[fail(display = "Carbon backend failure")]
    CarbonBackend,

    #[fail(display = "future send error")]
    FutureSend,

    #[fail(display = "metric too long")]
    MetricTooLong,

    #[fail(display = "bad metric: {}", _0)]
    Parsing(&'static str),

    #[fail(display = "lua: {:?}", _0)]
    Lua(ThreadStatus),

    #[fail(display = "lua runtime: {:?} {}", _0, _1)]
    LuaRuntime(ThreadStatus, String),
}

impl From<std::io::Error> for GeneralError {
    fn from(e: std::io::Error) -> Self {
        GeneralError::Io(e)
    }
}
