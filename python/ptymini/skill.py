# NB: this file is designed for an LLM reader, not a human. Add it to your LLM's context and ask it to explain.
r"""Drive line-oriented interactive programs in the background: REPLs, shells, ssh, debuggers. Start a pty session once, send input later, wait a bounded time for the reply, and read what arrived since your last look. Use this for CLI work. Use `fastmux` for TUIs, rich terminal apps, and terminals shared with the user.

- `ptymini.bg` (bgterm): sync, sid-based, straight into the Rust pty core, no event loop; `ptymini.core`: asyncio (multi-client attach, replay buffers, named registry)
- bgterm sessions: in-process; end with it

```python
sid = start_bgterm(['ipython', '--simple-prompt'])
poll(sid, 5000, until=r'In \[1\]')
r = write_stdin(sid, '2+2\n', 5000, until=r'In \[2\]')
close_bgterm(sid)
```

- waits (`wait_ms`, `until=`, `settle_ms`): as `fastmux.bg`, but event-driven, no `interval_ms`; bounded: timeout returns what arrived; dead child ends any wait at once
- `until`: text only the awaited reply produces, e.g. next prompt; sees only this call's output    /reading consumes the stream
"""

from .bg import *
from .bg import __all__
