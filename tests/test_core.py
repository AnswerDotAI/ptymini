import os, sys, time
import pytest
from ptymini.core import Gap, PtyRegistry, PtySession, Ring

BASH = ["bash", "--norc", "--noprofile", "-i"]
BENV = dict(os.environ, PS1="$ ", TERM="dumb")


async def _read_until(t, pat: bytes, offset: int = 0, timeout: float = 10.0):
    buf, end = b"", time.monotonic() + timeout
    while pat not in buf:
        data, offset, _ = t.read_from(offset)
        buf += data
        if pat in buf or not (t.alive or data): break
        if not data: await t.wait_change(seen_end=offset, timeout=end - time.monotonic())
    return buf, offset


def test_ring_offsets_paging_and_overflow():
    r = Ring(max_bytes=8)
    r.append(b"abcdef")
    assert r.read_from(0) == (b"abcdef", 6, 0)
    r.append(b"ghij")
    assert (r.start, r.end, len(r)) == (2, 10, 8)
    assert r.read_from(0) == (b"cdefghij", 10, 2)
    page, off1, _ = r.read_from(2, max_bytes_out=3)
    assert (page, off1) == (b"cde", 5)
    assert r.read_from(off1)[0] == b"fghij"
    assert r.read_from(10)[0] == b""


@pytest.mark.asyncio
async def test_pty_session_cursor_stream_resize_terminate_and_exit():
    async with PtySession(BASH, env=BENV) as t:
        t.write(b"echo hi $((6*7))\n")
        out, off = await _read_until(t, b"hi 42")
        assert t.alive and b"hi 42" in out

        chunks = []
        async for chunk in t.attach():
            chunks.append(chunk)
            if b"hi 42" in b"".join(chunks): break
        assert b"echo hi" in b"".join(chunks)

        t.resize(40, 100)
        t.write(b"stty size\n")
        out, off = await _read_until(t, b"40 100", offset=off)
        assert b"40 100" in out

        assert await t.terminate(force=True)
        code = await t.wait()
        assert not t.alive and code < 0
        final = b"".join([c async for c in t.attach()])
        assert b"hi 42" in final

    async with PtySession([sys.executable, "-c", 'print("bye"); raise SystemExit(3)']) as p:
        assert await p.wait() == 3
        assert not p.alive


@pytest.mark.asyncio
async def test_pty_session_attach_reports_gap_on_overflow():
    async with PtySession(["bash", "-c", 'printf a; sleep 0.3; printf "%0999d" 7'], buffer_bytes=64) as g:
        s = g.attach(gaps=True)
        assert await anext(s) == b"a"
        await g.wait()
        nxt = await anext(s)
        assert isinstance(nxt, Gap) and int(nxt) == 935
        rest = b"".join([c async for c in s])
        assert len(rest) == 64 and rest.endswith(b"0007")


@pytest.mark.asyncio
async def test_pty_registry_names_rc_cull_and_shutdown():
    async with PtyRegistry(argv=BASH) as terms:
        ta = await terms.create(env=BENV)
        assert ta.name == "1"
        assert await terms.create(name="1") is ta
        await terms.create(name="scratch", env=BENV)

        tr = await terms.create(
            argv=["bash", "--noprofile", "--rcfile", "{rcfile}", "-i"],
            rc="PS1='$ '\nalias hi='echo rc-worked'",
            env=dict(BENV),
        )
        tr.write(b"hi\n")
        out, _ = await _read_until(tr, b"rc-worked")
        assert b"rc-worked" in out

        assert terms.cull_ready() == []
        terms.cull_timeout = 3600
        ta.last_activity -= 7200
        assert terms.cull_ready() == ["1"]
        await terms.cull()
        assert terms.get("1") is None and terms.get("scratch") is not None

    assert len(terms) == 0
