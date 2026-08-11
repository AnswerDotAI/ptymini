"""The bgterm API: sync, cursor-paged background terminal sessions

The [bgterm](https://github.com/AnswerDotAI/bgterm) package, folded in: a *sync*, sid-based API for background terminal sessions — start once, send input later, wait a bounded time, read what arrived since your last look. It is the cursor view of a pty session, for callers that live outside an event loop (kernel tools, plain scripts, an LLM deciding between actions): each call runs directly against the sync Rust core and blocks, with the GIL released while waiting, so `write_stdin(sid, "2+2\n", 500)` means what it always meant. The buffering, paging, and drop accounting are the `Ring`'s, which was extracted from bgterm in the first place; `PollResult` and every function signature are unchanged from the original package.

Docs: https://AnswerDotAI.github.io/ptymini/bg.html.md"""


__all__ = ['DEFAULT_MAX_BUFFER_BYTES', 'DEFAULT_MAX_OUTPUT_BYTES', 'Cmd', 'BgtermError', 'PollResult', 'list_sessions',
           'start_bgterm', 'write_stdin', 'poll', 'read', 'wait', 'terminate', 'kill', 'close_bgterm', 'Session']

import itertools, os, signal, threading
from dataclasses import dataclass
from ._core import PtyCore

DEFAULT_MAX_BUFFER_BYTES = 1_000_000
DEFAULT_MAX_OUTPUT_BYTES = 65_536
Cmd = str | list[str]

class BgtermError(RuntimeError):
    "Raised when a bgterm session cannot be started, found, or controlled."

@dataclass(slots=True)
class PollResult:
    "Unread output returned by a `write_stdin()` or `poll()` call."

    text: str                 # `data` decoded with the session's encoding
    data: bytes               # The unread bytes returned by this call
    start_offset: int         # Absolute offset of the first returned byte
    end_offset: int           # Absolute offset just past the last returned byte (the new cursor)
    buffer_start_offset: int  # Absolute offset of the oldest byte the ring retains
    buffer_end_offset: int    # Total bytes the session has ever output
    bytes_returned: int       # Length of `data`
    remaining_bytes: int      # Unread bytes still in the ring after this page
    dropped_bytes: int        # Unread bytes lost to ring trimming before this read
    running: bool             # Was the child still alive at read time?
    exit_code: int | None     # Exit code once dead (negative: terminating signal number)

    @property
    def truncated(self):
        "Return whether output was paged or older unread bytes were dropped."
        return self.remaining_bytes > 0 or self.dropped_bytes > 0

class _BgSession:
    "Sync cursor facade over one Rust `PtyCore`."
    def __init__(self, s:PtyCore, encoding:str, errors:str):
        self.s, self.encoding, self.errors, self.cursor, self._closed = s, encoding, errors, 0, False

    @classmethod
    def start(cls, cmd:Cmd=None, cwd=None, env=None, shell=None, encoding='utf-8', errors='replace',
        max_buffer_bytes=DEFAULT_MAX_BUFFER_BYTES):
        if cmd is None: cmd = os.environ.get('SHELL', '/bin/sh')
        if shell is None: shell = isinstance(cmd, str)
        if shell and isinstance(cmd, str): cmd = ['/bin/sh', '-c', cmd]
        elif isinstance(cmd, str): cmd = [cmd]
        env = dict(os.environ if env is None else env, TERM='dumb')
        return cls(PtyCore(list(cmd), cwd=cwd, env=env, buffer_bytes=max_buffer_bytes), encoding, errors)

    @property
    def running(self): return self.s.alive
    @property
    def exit_code(self): return self.s.exit_code

    def _read(self, max_output_bytes):
        if max_output_bytes is not None and max_output_bytes < 0: raise ValueError('max_output_bytes must be >= 0')
        r = self.s
        payload, end_offset, dropped = r.read_from(self.cursor, max_output_bytes)
        start_offset = end_offset - len(payload)
        self.cursor = end_offset
        return PollResult(payload.decode(self.encoding, errors=self.errors), payload, start_offset, end_offset,
            r.start, r.end, len(payload), max(0, r.end - end_offset), dropped, self.s.alive, self.s.exit_code)

    def read(self, max_output_bytes=DEFAULT_MAX_OUTPUT_BYTES): return self._read(max_output_bytes)

    def write_stdin(self, chars='', yield_time_ms=0, max_output_bytes=DEFAULT_MAX_OUTPUT_BYTES):
        if chars:
            data = chars if isinstance(chars, bytes) else chars.encode(self.encoding)
            try: self.s.write(data)
            except OSError as e: raise BgtermError('session PTY is no longer writable') from e
        if yield_time_ms > 0 and self.cursor >= self.s.end and self.s.alive: self.s.wait_change(timeout=yield_time_ms / 1000)
        return self._read(max_output_bytes)

    def poll(self, yield_time_ms=0, max_output_bytes=DEFAULT_MAX_OUTPUT_BYTES): return self.write_stdin('', yield_time_ms, max_output_bytes)

    def wait(self, timeout_ms=None): return self.s.wait(timeout=None if timeout_ms is None else max(timeout_ms, 0) / 1000)

    def terminate(self):
        if self.running:
            try: self.s.kill(signal.SIGTERM)
            except ProcessLookupError: pass

    def kill(self):
        if self.running:
            try: self.s.kill(signal.SIGKILL)
            except ProcessLookupError: pass

    def close(self, terminate=True, kill_after_ms=1000):
        if self._closed: return
        self._closed = True
        if terminate and self.running:
            self.terminate()
            if self.wait(kill_after_ms) is None and self.running:
                self.kill()
                self.wait(kill_after_ms)

_REGISTRY_LOCK = threading.Lock()
_SESSIONS = {}
_NEXT_SID = itertools.count(1)


def _lookup(sid: int):
    try: return _SESSIONS[int(sid)]
    except KeyError as e: raise BgtermError(f"unknown bgterm session id {sid!r}") from e


def list_sessions():
    "List active in-process bgterm session ids."
    with _REGISTRY_LOCK: return tuple(sorted(_SESSIONS))


def start_bgterm(
    cmd:Cmd=None,          # Command argv, or a string command line; None spawns `$SHELL`
    cwd:str=None,          # Working directory for the child
    env:dict=None,         # Child environment; None inherits this process's (`TERM=dumb` overlays either)
    shell:bool=None,       # Run a string `cmd` via `/bin/sh -c`? None: yes when `cmd` is a string
    encoding:str='utf-8',  # Decoding for `PollResult.text`
    errors:str='replace',  # Decode error handling
    max_buffer_bytes:int=DEFAULT_MAX_BUFFER_BYTES,  # Ring bound: how much unread output is retained
):
    "Start a PTY-backed session and return its integer session id."
    session = _BgSession.start(cmd, cwd, env, shell, encoding, errors, max_buffer_bytes)
    with _REGISTRY_LOCK:
        sid = next(_NEXT_SID)
        _SESSIONS[sid] = session
    return sid


def write_stdin(
    sid:int,              # Session id from `start_bgterm`
    chars:str|bytes='',   # Input to write; str encodes with the session's encoding, bytes pass through
    yield_time_ms:int=0,  # Max ms to wait for output when none is unread
    max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
):
    "Write to a session PTY, wait briefly, and return unread output."
    return _lookup(sid).write_stdin(chars, yield_time_ms, max_output_bytes)


def poll(
    sid:int,              # Session id from `start_bgterm`
    yield_time_ms:int=0,  # Max ms to wait for output when none is unread
    max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
):
    "Wait for unread session output, then return it without writing input."
    return _lookup(sid).poll(yield_time_ms, max_output_bytes)


def read(
    sid:int,  # Session id from `start_bgterm`
    max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
):
    "Return unread session output immediately."
    return _lookup(sid).read(max_output_bytes)


def wait(
    sid:int,              # Session id from `start_bgterm`
    timeout_ms:int=None,  # Max ms to wait; None waits indefinitely
):
    "Wait for a session to exit and return its exit code."
    return _lookup(sid).wait(timeout_ms)


def terminate(sid: int):
    "Terminate the session child process."
    _lookup(sid).terminate()


def kill(sid: int):
    "Kill the session child process."
    _lookup(sid).kill()


def close_bgterm(
    sid:int,                # Session id from `start_bgterm`
    terminate:bool=True,    # Ask the child to exit (SIGTERM, then SIGKILL) before closing?
    kill_after_ms:int=1000, # Ms to wait after each signal before escalating
):
    "Close a session and remove it from the in-process registry."
    with _REGISTRY_LOCK: session = _SESSIONS.pop(int(sid), None)
    if session is None: return
    session.close(terminate, kill_after_ms)

class Session:
    "Thin OO wrapper around the sid-based bgterm API."

    def __init__(self,
        sid:int,                 # Session id from `start_bgterm`
        close_on_exit:bool=True, # Close the session when a `with` block exits?
    ):
        self.sid = int(sid)
        self.close_on_exit = close_on_exit
        self._closed = False

    @classmethod
    def start(cls,
        cmd:Cmd=None,          # Command argv, or a string command line; None spawns `$SHELL`
        cwd:str=None,          # Working directory for the child
        env:dict=None,         # Child environment; None inherits this process's (`TERM=dumb` overlays either)
        shell:bool=None,       # Run a string `cmd` via `/bin/sh -c`? None: yes when `cmd` is a string
        encoding:str='utf-8',  # Decoding for `PollResult.text`
        errors:str='replace',  # Decode error handling
        max_buffer_bytes:int=DEFAULT_MAX_BUFFER_BYTES,  # Ring bound: how much unread output is retained
        close_on_exit:bool=True,  # Close the session when a `with` block exits?
    ):
        "Start a PTY-backed session and wrap it in `Session`."
        return cls(start_bgterm(cmd, cwd, env, shell, encoding, errors, max_buffer_bytes), close_on_exit)

    @classmethod
    def open(cls,
        sid:int,                  # An existing session id to wrap
        close_on_exit:bool=False, # Close the session when a `with` block exits?
    ):
        "Wrap an existing session id."
        _lookup(sid)
        return cls(sid, close_on_exit)

    def __enter__(self): return self

    def __exit__(self, exc_type, exc, tb):
        if self.close_on_exit: self.close()
        return False

    @property
    def running(self):
        "Return whether the wrapped session is still running."
        if self._closed: return False
        return _lookup(self.sid).running

    @property
    def exit_code(self):
        "Return the wrapped session's exit code, if available."
        if self._closed: return None
        return _lookup(self.sid).exit_code

    def write_stdin(self,
        chars:str|bytes='',   # Input to write; str encodes with the session's encoding, bytes pass through
        yield_time_ms:int=0,  # Max ms to wait for output when none is unread
        max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
    ):
        "Write to this session PTY, wait briefly, and return unread output."
        return write_stdin(self.sid, chars, yield_time_ms, max_output_bytes)

    def poll(self,
        yield_time_ms:int=0,  # Max ms to wait for output when none is unread
        max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
    ):
        "Wait for unread output from this session, then return it."
        return poll(self.sid, yield_time_ms, max_output_bytes)

    def read(self,
        max_output_bytes:int=DEFAULT_MAX_OUTPUT_BYTES,  # Page size cap on returned bytes; None returns everything unread
    ):
        "Return unread output from this session immediately."
        return read(self.sid, max_output_bytes)

    def wait(self,
        timeout_ms:int=None,  # Max ms to wait; None waits indefinitely
    ):
        "Wait for this session to exit and return its exit code."
        return wait(self.sid, timeout_ms)

    def terminate(self):
        "Terminate this session child process."
        terminate(self.sid)

    def kill(self):
        "Kill this session child process."
        kill(self.sid)

    def close(self,
        terminate:bool=True,    # Ask the child to exit (SIGTERM, then SIGKILL) before closing?
        kill_after_ms:int=1000, # Ms to wait after each signal before escalating
    ):
        "Close this session and make repeated closes a no-op."
        if self._closed: return
        close_bgterm(self.sid, terminate, kill_after_ms)
        self._closed = True
