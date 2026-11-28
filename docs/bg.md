---
title: Background terminal sessions
description: Synchronous, cursor-paged PTY sessions for scripts and LLM tools
order: 2
module: ptymini.bg
---

`ptymini.bg` provides a synchronous, cursor-paged API over the same Rust PTY core for callers outside an event loop. Start a session once, send commands later, and read only the bytes produced since your previous read.

Import `ptymini.bg` explicitly (`ptymini` does not re-export it at top level).

```python
from ptymini.bg import *
```

## Starting and driving sessions

`start_bgterm` spawns a child process and returns an integer session ID (`sid`). `write_stdin` and `poll` block with the GIL released until `wait_ms` expires, `until` matches a regex in the accumulated text, or output stops arriving for `settle_ms`.

```python
sid = start_bgterm("python3 -q")
res = write_stdin(sid, "print(6 * 7)\n", wait_ms=500, until=r"42")
print(res.text)
close_bgterm(sid)
```

{{< doc start_bgterm >}}

{{< doc write_stdin >}}

{{< doc poll >}}

{{< doc read >}}

{{< doc wait >}}

## Lifecycle and cleanup

{{< doc list_sessions >}}

{{< doc terminate >}}

{{< doc kill >}}

{{< doc close_bgterm >}}

## Object wrapper and results

`Session` wraps a `sid` as a context manager that closes the session on exit. Every read returns a `PollResult` with decoded text, raw bytes, cursor offsets, drop counts, and child process status.

```python
with Session.start(["/bin/sh"]) as s:
    out = s.write_stdin("echo ready\n", wait_ms=500, until="ready")
    assert out.running
```

{{< doc Session >}}

{{< doc PollResult >}}

{{< doc BgtermError >}}
