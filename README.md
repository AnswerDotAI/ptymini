# ptymini

`ptymini` provides asyncio-native pty sessions (`ptymini.core`) and a synchronous background-terminal API (`ptymini.bg`) backed by a compiled Rust core.

A `PtySession` spawns one pty that multiple clients can attach to as independent byte streams, each primed from a byte-bounded `Ring` replay buffer so reconnecting clients resume with recent scrollback instead of a blank screen. `PtyRegistry` adds named get-or-create sessions, injected shell setup (`rc`), and inactivity culling without adding a server, framework, or cross-process persistence. Web exposure belongs to the embedding app (such as [jupygate](https://github.com/AnswerDotAI/jupygate)), and durable session management belongs to tmux.

`ptymini.bg` exposes the [bgterm](https://github.com/AnswerDotAI/bgterm) API for callers outside an event loop, such as scripts, kernel tools, and LLM agents. Calls block in the Rust core with the GIL released and share `fastmux.bg`'s wait semantics (`wait_ms`, `until`, and `settle_ms`).

## Install

```bash
pip install ptymini
```

## Asyncio sessions (`ptymini.core`)

`PtySession` supports two ways to read from the same bounded `Ring` buffer:

- `attach()` returns an async iterator that replays retained scrollback, yields new bytes as they arrive, and ends at EOF. Each attachment tracks its own position, and stopping an iterator leaves the terminal and other readers untouched. With `gaps=True`, a reader that fell behind receives a `Gap(n)` count of lost bytes before resuming at the oldest retained byte.
- `read_from(offset, max_bytes_out=None)` and `wait_change(seen_end=offset, timeout=...)` read pages from an explicit byte offset without advancing any other reader.

```python
import asyncio
from ptymini.core import PtySession

async def main():
    async with PtySession(["bash", "--norc", "--noprofile", "-i"]) as t:
        t.write(b"echo hi $((6*7))\n")
        async for chunk in t.attach():
            if b"hi 42" in chunk: break
        t.resize(40, 100)
        await t.terminate(force=True)
        print("exit:", await t.wait())

asyncio.run(main())
```

`terminate()` sends `SIGHUP`, `SIGCONT`, `SIGINT`, and `SIGTERM` in order until the child exits (following terminado's `PtyWithClients.terminate`, BSD-licensed, Copyright (c) Jupyter Development Team). With `force=True`, it sends `SIGKILL` if the child still has not exited. Exiting an `async with` block calls `terminate(force=True)`.

`PtyRegistry` manages named sessions. `create(name=...)` returns an existing session or spawns a new one, auto-numbering when `name` is omitted. `env` replaces the inherited environment, `appendenv` overrides keys in it, and `rc` writes shell setup to a private `.zshrc` directory and substitutes `{rcfile}` and `{rcdir}` in `argv` and environment values (use `--rcfile {rcfile}` for Bash or `ZDOTDIR={rcdir}` for Zsh). Set `cull_timeout` and call `cull_ready()`, `cull()`, or run `cull_loop(reg)` to terminate inactive sessions.

```python
from ptymini.core import PtyRegistry

async with PtyRegistry(argv=["bash", "--norc", "--noprofile", "-i"]) as terms:
    t = await terms.create(name="scratch")
    tr = await terms.create(
        argv=["bash", "--noprofile", "--rcfile", "{rcfile}", "-i"],
        rc="PS1='$ '\nalias hi='echo rc-worked'",
    )
```

## Synchronous background sessions (`ptymini.bg`)

`start_bgterm` spawns a child process and returns an integer session ID. String commands run through `/bin/sh -c` with `TERM=dumb` by default (pass `term="xterm-256color"` to test full terminal output). `poll`, `write_stdin`, and `read` advance the session cursor and return a `PollResult` whose `remaining_bytes`, `dropped_bytes`, and `truncated` fields report unread backlog and buffer overflow.

- `wait_ms` bounds the wait for new output or an `until` regex match.
- `until` matches unread text only (never text already returned by an earlier read).
- `settle_ms` waits for output to go quiet after bursts arrive; total wait never exceeds `wait_ms + settle_ms`, and process exit ends any wait immediately.

```python
from ptymini.bg import Session, close_bgterm, poll, start_bgterm, write_stdin

sid = start_bgterm(["ipython", "--simple-prompt"])
poll(sid, 5000, until=r"In \[1\]")
res = write_stdin(sid, "2+2\n", 5000, until=r"In \[2\]")
close_bgterm(sid)

with Session.start(["python", "-c", "print('bye'); raise SystemExit(3)"]) as sess:
    print(sess.poll(3000).text)
    assert sess.wait(3000) == 3
```

## Tests

```bash
pytest -q
```
