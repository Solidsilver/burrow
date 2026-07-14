//! Thin control-socket client used by every CLI subcommand.

use anyhow::Context;
use burrow_proto::ctrl::{read_frame, write_frame, CtrlOk, CtrlRequest, CtrlResult};
use tokio::net::UnixStream;

pub async fn call(req: CtrlRequest) -> anyhow::Result<CtrlOk> {
    let socket = burrow_daemon::paths::socket_path();
    let mut stream = UnixStream::connect(&socket).await.with_context(|| {
        format!(
            "cannot reach the burrow daemon at {} — is it running? (start with `burrow daemon run`)",
            socket.display()
        )
    })?;
    write_frame(&mut stream, &req).await.context("sending request to daemon")?;
    let result: CtrlResult = read_frame(&mut stream).await.context("reading daemon reply")?;
    result.map_err(|e| anyhow::anyhow!("{e}"))
}

/// Decimal units, matching what users type in `burrow grant` ("5gb" = 5.0 GB).
pub fn fmt_bytes(n: u64) -> String {
    burrow_daemon::config::fmt_size(n)
}

pub fn fmt_time(unix: u64) -> String {
    chrono::DateTime::from_timestamp(unix as i64, 0)
        .map(|t| t.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| unix.to_string())
}
