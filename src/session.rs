//! One pty process: reads land in a `Ring`, waiters wake on change, EOF reaps
//! the exit status.
//!
//! The core is sync and runtime-free. A dedicated reader thread pulls bytes
//! from the pty master into the ring under a mutex; a condvar serves blocking
//! waiters (`wait_change`, `wait`), and an optional callback hook lets an
//! async embedding (Python asyncio, a tokio adaptor) hear about changes
//! without the core knowing anything about event loops.

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::ring::Ring;

fn now() -> f64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() }

pub struct State {
    pub ring: Ring,
    pub alive: bool,
    pub exit_code: Option<i32>,
    pub last_activity: f64,
}

type Callback = Arc<dyn Fn() + Send + Sync>;

struct Inner {
    state: Mutex<State>,
    cond: Condvar,
    callback: Mutex<Option<Callback>>,
    master: OwnedFd,
    child: Mutex<Option<Child>>,
    pid: i32,
}

impl Inner {
    // Snapshot the callback out of its lock before calling: the callback may
    // acquire the GIL, which a GIL-holding thread setting the callback would
    // otherwise deadlock against.
    fn notify(&self) {
        self.cond.notify_all();
        let cb = self.callback.lock().unwrap().clone();
        if let Some(cb) = cb { cb(); }
    }
}

pub struct PtyCore { inner: Arc<Inner> }

impl PtyCore {
    pub fn spawn(argv: &[String], cwd: Option<&str>, env: Option<&HashMap<String, String>>, rows: u16, cols: u16, buffer_bytes: usize) -> io::Result<PtyCore> {
        let first = argv.first().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "argv must not be empty"))?;
        let (master, slave) = openpty(rows, cols)?;
        let mut cmd = Command::new(first);
        cmd.args(&argv[1..]);
        if let Some(c) = cwd { cmd.current_dir(c); }
        if let Some(e) = env {
            cmd.env_clear();
            cmd.envs(e);
        }
        cmd.stdin(Stdio::from(slave.try_clone()?));
        cmd.stdout(Stdio::from(slave.try_clone()?));
        cmd.stderr(Stdio::from(slave));
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() < 0 { return Err(io::Error::last_os_error()); }
                if libc::ioctl(0, libc::TIOCSCTTY as _, 0) < 0 { return Err(io::Error::last_os_error()); }
                Ok(())
            });
        }
        let child = cmd.spawn()?;
        drop(cmd); // close the parent's slave fds, so EOF can arrive on the master
        let pid = child.id() as i32;
        let inner = Arc::new(Inner {
            state: Mutex::new(State { ring: Ring::new(buffer_bytes), alive: true, exit_code: None, last_activity: now() }),
            cond: Condvar::new(),
            callback: Mutex::new(None),
            master,
            child: Mutex::new(Some(child)),
            pid,
        });
        let t_inner = inner.clone();
        thread::Builder::new().name("ptymini-reader".into()).spawn(move || reader(t_inner))?;
        Ok(PtyCore { inner })
    }

    /// Replace the change hook, called (from the reader thread) after every
    /// ring append and once at EOF.
    pub fn set_callback(&self, cb: Callback) { *self.inner.callback.lock().unwrap() = Some(cb); }

    pub fn write(&self, data: &[u8]) -> io::Result<()> {
        self.inner.state.lock().unwrap().last_activity = now();
        let fd = self.inner.master.as_raw_fd();
        let mut rest = data;
        while !rest.is_empty() {
            let n = unsafe { libc::write(fd, rest.as_ptr() as *const libc::c_void, rest.len()) };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.raw_os_error() == Some(libc::EINTR) { continue; }
                return Err(e);
            }
            rest = &rest[n as usize..];
        }
        Ok(())
    }

    pub fn resize(&self, rows: u16, cols: u16) -> io::Result<()> {
        let ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
        let r = unsafe { libc::ioctl(self.inner.master.as_raw_fd(), libc::TIOCSWINSZ as _, &ws) };
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// Signal the child. ESRCH when it is already reaped, matching kill(2).
    pub fn kill(&self, sig: i32) -> io::Result<()> {
        if !self.alive() { return Err(io::Error::from_raw_os_error(libc::ESRCH)); }
        let r = unsafe { libc::kill(self.inner.pid, sig) };
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    pub fn read_from(&self, offset: u64, max_bytes_out: Option<usize>) -> (Vec<u8>, u64, u64) {
        self.inner.state.lock().unwrap().ring.read_from(offset, max_bytes_out)
    }

    pub fn start(&self) -> u64 { self.inner.state.lock().unwrap().ring.start }
    pub fn end(&self) -> u64 { self.inner.state.lock().unwrap().ring.end }
    pub fn alive(&self) -> bool { self.inner.state.lock().unwrap().alive }
    pub fn exit_code(&self) -> Option<i32> { self.inner.state.lock().unwrap().exit_code }
    pub fn last_activity(&self) -> f64 { self.inner.state.lock().unwrap().last_activity }
    pub fn set_last_activity(&self, v: f64) { self.inner.state.lock().unwrap().last_activity = v; }

    /// Block until `ring.end` passes `seen_end` (default: its value now) or
    /// EOF; false on timeout. The sync twin of the Python bridge's
    /// `wait_change`.
    pub fn wait_change(&self, seen_end: Option<u64>, timeout: Option<f64>) -> bool {
        let mut st = self.inner.state.lock().unwrap();
        let seen = seen_end.unwrap_or(st.ring.end);
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t.max(0.0)));
        while st.alive && st.ring.end == seen {
            match deadline {
                None => st = self.inner.cond.wait(st).unwrap(),
                Some(d) => {
                    let left = d.saturating_duration_since(Instant::now());
                    if left.is_zero() { return false; }
                    st = self.inner.cond.wait_timeout(st, left).unwrap().0;
                }
            }
        }
        true
    }

    /// Block until the pty dies or `timeout` (seconds) elapses; the exit code
    /// (negative: terminating signal number), or None on timeout or when the
    /// child could not be reaped.
    pub fn wait(&self, timeout: Option<f64>) -> Option<i32> {
        let mut st = self.inner.state.lock().unwrap();
        let deadline = timeout.map(|t| Instant::now() + Duration::from_secs_f64(t.max(0.0)));
        while st.alive {
            match deadline {
                None => st = self.inner.cond.wait(st).unwrap(),
                Some(d) => {
                    let left = d.saturating_duration_since(Instant::now());
                    if left.is_zero() { return None; }
                    st = self.inner.cond.wait_timeout(st, left).unwrap().0;
                }
            }
        }
        st.exit_code
    }
}

fn openpty(rows: u16, cols: u16) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut master: libc::c_int = 0;
    let mut slave: libc::c_int = 0;
    let mut ws = libc::winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 };
    let r = unsafe { libc::openpty(&mut master, &mut slave, std::ptr::null_mut(), std::ptr::null_mut(), &mut ws) };
    if r < 0 { return Err(io::Error::last_os_error()); }
    unsafe { Ok((OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave))) }
}

fn reader(inner: Arc<Inner>) {
    let fd = inner.master.as_raw_fd();
    let mut buf = vec![0u8; 65536];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n > 0 {
            {
                let mut st = inner.state.lock().unwrap();
                st.ring.append(&buf[..n as usize]);
                st.last_activity = now();
            }
            inner.notify();
        } else if n < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) { continue; } else {
            break; // 0 at EOF (macOS), EIO once the child is gone (Linux)
        }
    }
    let code = inner.child.lock().unwrap().take().and_then(|mut c| c.wait().ok()).map(|s| s.code().unwrap_or_else(|| -s.signal().unwrap_or(0)));
    {
        let mut st = inner.state.lock().unwrap();
        st.alive = false;
        st.exit_code = code;
    }
    inner.notify();
}
