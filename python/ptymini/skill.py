r"""Drive line-oriented interactive programs in the background: REPLs, shells, ssh, debuggers. Start a pty session once, send input later, wait a bounded time for the reply, and read what arrived since your last look. Use this for CLI work. Use `fastmux` for TUIs, rich terminal apps, and terminals shared with the user.

API: `ptymini.bg` (bgterm): sync, sid-based calls straight into the Rust pty core; no event loop. Sessions live in this process and end with it; nothing is shared server-side. The whole flow:

    sid = start_bgterm(['ipython', '--simple-prompt'])
    poll(sid, 5000, until=r'In \[1\]')
    r = write_stdin(sid, '2+2\n', 5000, until=r'In \[2\]')
    close_bgterm(sid)

`wait_ms`/`until=`/`settle_ms` work as in `fastmux.bg` (`doc(poll)` defines them); every wait is bounded and returns whatever arrived on timeout. For `until`, match text only the awaited reply produces, e.g. the next prompt. Differences from fastmux: waits are event-driven (no `interval_ms`); reading consumes the stream, so `until` never sees output an earlier call returned.

Read-shaped calls return a `PollResult` (`doc(PollResult)` for fields; `truncated` flags paging or drops). A dead child ends any wait at once. `read()` returns immediately; `wait()` blocks until exit; `terminate`/`kill`/`close_bgterm` end the child; `list_sessions()` lists live sids; `Session` wraps a sid for `with` blocks.

`ptymini.core` is the asyncio surface (multi-client attach, replay buffers, named registry). Run `doc(func)` for full parameter docments before first use.
"""

from .bg import *
from .bg import __all__
