//! Port of `app/eslogscli/less_wrapper.go`.

use std::io::{self, IsTerminal, Read, Write};

pub fn is_terminal() -> bool {
    io::stdout().is_terminal() && io::stderr().is_terminal()
}

/// Forwards `r` to the `less` pager (or straight to stdout when stdout isn't
/// a terminal).
///
/// PORT NOTE: Go ignores SIGINT in the current process while `less` runs so
/// `less` can handle Ctrl+C itself; installing signal handlers needs libc and
/// this crate is std-only, so Ctrl+C during paging terminates eslogscli too.
pub fn read_with_less(
    r: &mut dyn Read,
    disable_colors: bool,
    wrap_long_lines: bool,
) -> Result<(), String> {
    if !is_terminal() {
        // Just write everything to stdout if no terminal is available.
        return copy_to_stdout(r);
    }
    read_with_less_terminal(r, disable_colors, wrap_long_lines)
}

fn copy_to_stdout(r: &mut dyn Read) -> Result<(), String> {
    let stdout = io::stdout();
    let mut w = stdout.lock();
    if let Err(err) = io::copy(r, &mut w)
        && !is_err_pipe(&err)
    {
        return Err(format!("error when forwarding data to stdout: {err}"));
    }
    if let Err(err) = w.flush() {
        return Err(format!("cannot sync data to stdout: {err}"));
    }
    Ok(())
}

#[cfg(unix)]
fn read_with_less_terminal(
    r: &mut dyn Read,
    disable_colors: bool,
    wrap_long_lines: bool,
) -> Result<(), String> {
    use std::process::{Command, Stdio};

    // PORT NOTE: Go resolves the `less` binary via exec.LookPath; the port
    // honors $PAGER first and falls back to `less`.
    let pager = std::env::var("PAGER")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "less".to_string());
    let mut opts: Vec<&str> = vec!["-F", "-X"];
    if !disable_colors {
        opts.push("-R");
    }
    if !wrap_long_lines {
        opts.push("-S");
    }
    let mut child = Command::new(&pager)
        .args(&opts)
        .env("LESSCHARSET", "utf-8")
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|err| format!("cannot start 'less' process: {err}"))?;

    // Forward data from r to 'less'. When 'less' exits early (e.g. the user
    // presses `q`), the copy fails with a broken pipe, which is ignored below
    // - this replaces Go's close-the-pipe-from-a-goroutine dance.
    let mut stdin = child.stdin.take().expect("BUG: child stdin must be piped");
    let copy_result = io::copy(r, &mut stdin);
    let _ = stdin.flush();
    drop(stdin);

    // Wait until 'less' finished and verify its status.
    let status = child
        .wait()
        .map_err(|err| format!("unexpected error when waiting for 'less' process: {err}"))?;
    if !status.success() {
        return Err(format!(
            "'less' finished with unexpected code {}",
            status.code().unwrap_or(-1)
        ));
    }

    if let Err(err) = copy_result
        && !is_err_pipe(&err)
    {
        return Err(format!("error when forwarding data to 'less': {err}"));
    }

    Ok(())
}

/// PORT NOTE: there is no ubiquitous `less` on Windows, so the port prints
/// the response directly to stdout instead of paging it.
#[cfg(not(unix))]
fn read_with_less_terminal(
    r: &mut dyn Read,
    _disable_colors: bool,
    _wrap_long_lines: bool,
) -> Result<(), String> {
    copy_to_stdout(r)
}

pub fn is_err_pipe(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::BrokenPipe
}
