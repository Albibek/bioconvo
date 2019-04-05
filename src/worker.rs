use bytes::BytesMut;

use crate::errors::GeneralError;
use crate::Float;
use metric::{Metric, MetricType};

// A top-level task for metric processing
pub(crate) fn task(buf: BytesMut) {}

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
}
