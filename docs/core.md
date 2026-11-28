---
title: Asyncio sessions
description: Asyncio PTY sessions, offset-tracked output rings, and named session registries
order: 1
module: ptymini.core
---

`ptymini.core` wraps a Rust PTY reader and bounded byte `Ring` behind an asyncio API. A single session supports both streaming consumers (`attach`, which replays scrollback and yields live bytes) and cursor consumers (`read_from`, which pages unread bytes by absolute offset).

Import `ptymini.core` explicitly (`ptymini` does not re-export it at top level); the registry example below also uses `asyncio`.

```python
import asyncio
from ptymini.core import *
```

## Sessions and streaming

Spawn a child on a PTY with `PtySession`, feed input with `write`, resize with `resize`, and read output through `attach` or `read_from`. `terminate` sends `SIGHUP`, `SIGCONT`, `SIGINT`, and `SIGTERM` in turn, escalating to `SIGKILL` when `force=True`.

```python
sess = PtySession(["python3", "-i"])
sess.write(b"2 + 2\n")
async for chunk in sess.attach():
    print(chunk.decode(), end="")
    if b"4" in chunk:
        break
await sess.terminate()
```

{{< doc PtySession >}}

{{< doc PtySession.attach >}}

{{< doc PtySession.read_from >}}

{{< doc PtySession.wait_change >}}

{{< doc PtySession.write >}}

{{< doc PtySession.resize >}}

{{< doc PtySession.terminate >}}

{{< doc PtySession.wait >}}

## Output ring

`Ring` stores a bounded window of output bytes indexed by monotonically increasing absolute offsets. `read_from` reports how many bytes fell off the back of the ring so callers detect dropped output explicitly. When `attach(gaps=True)` encounters trimmed bytes, it yields a `Gap` count before the next chunk.

{{< doc Ring >}}

{{< doc Ring.append >}}

{{< doc Ring.read_from >}}

{{< doc Gap >}}

## Named registry

`PtyRegistry` manages named `PtySession` instances with get-or-create semantics and optional idle culling. Run `cull_loop` as a background task inside an embedding server.

```python
reg = PtyRegistry(argv=["/bin/bash"], cull_timeout=600)
sess = await reg.create("shell-1")
task = asyncio.create_task(cull_loop(reg, interval=60))
```

{{< doc PtyRegistry >}}

{{< doc PtyRegistry.create >}}

{{< doc PtyRegistry.get >}}

{{< doc PtyRegistry.delete >}}

{{< doc PtyRegistry.cull >}}

{{< doc PtyRegistry.shutdown >}}

{{< doc cull_loop >}}
