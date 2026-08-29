r"""Drive line-oriented interactive programs in the background: REPLs, shells, ssh, debuggers. Start a pty session once, send input later, wait a bounded time for the reply, and read what arrived since your last look. Use this for CLI work. Use `fastmux` for TUIs, rich terminal apps, and terminals shared with the user.

The API is `ptymini.bg`, the bgterm interface. Calls are sync and sid-based. They run directly against the Rust pty core and need no event loop. Sessions live inside this process and end with it. Nothing is shared server-side.

The whole flow:

    sid = start_bgterm(['ipython', '--simple-prompt'])
    poll(sid, 5000, until=r'In \[1\]')
    r = write_stdin(sid, '2+2\n', 5000, until=r'In \[2\]')
    close_bgterm(sid)

The wait parameters are `fastmux.bg`'s. `wait_ms` bounds the wait for new output. `until=` returns as soon as the accumulated text matches that regex. Match on text only the awaited reply can produce, such as the next prompt. `settle_ms` keeps collecting until output has stopped for that long, and runs at most `settle_ms` past the `wait_ms` deadline. Every wait is bounded. On timeout the call returns whatever arrived. Two behaviors differ from fastmux. Waits are event-driven, and there is no `interval_ms`. Reading consumes the stream, and `until` never sees output an earlier call returned.

Every read-shaped call returns a `PollResult`. `text` and `data` hold the output. `remaining_bytes` and `truncated` report paging. `dropped_bytes` counts unread output lost when the ring overran its bound. `running` and `exit_code` report liveness; a negative exit code is the terminating signal number. A dead child ends any wait at once. `read()` returns immediately. `wait()` blocks until exit. `terminate`, `kill`, and `close_bgterm` end the child. `list_sessions()` lists live sids. `Session` wraps a sid for `with` blocks.

`ptymini.core` is the asyncio surface: multi-client attach, replay buffers, and a named registry. Run `doc(func)` for full parameter docments before first use.
"""

from .bg import *
from .bg import __all__
