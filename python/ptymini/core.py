"""`PtySession` and `PtyRegistry`: asyncio pty sessions over one offset-tracked output ring

Every design here serves two consumer shapes over one pty. *Streams* (a websocket handler, a renderer) want every byte in order, pushed as it arrives, with recent scrollback replayed on attach. *Cursors* (an LLM tool that reads "what's new since I last looked") want bounded-memory buffering with paging and honest drop accounting. Both are views over a single structure: the `Ring`, a byte buffer with absolute offsets. Each pty read appends to the ring; a session-wide change event wakes waiters; `read_from(offset)` serves both a stream's next chunk and a cursor's next page, and reports how many bytes fell off the back. There is no server here, no framework, and no persistence beyond the process: web exposure belongs to the embedding app (e.g. jupygate), durable sessions belong to tmux.

The ring and the pty process live in the Rust core (`ptymini._core`), which owns a reader thread per session; this module is the asyncio surface over it. The core's change hook lands on the loop via `call_soon_threadsafe`, so no event loop machinery crosses the language boundary.

Docs: https://AnswerDotAI.github.io/ptymini/core.html.md"""

import asyncio, itertools, logging, os, signal, time
from pathlib import Path
from tempfile import mkdtemp
from fastcore.basics import patch
from ._core import PtyCore, Ring

log = logging.getLogger('ptymini')

__all__ = ['log', 'DEFAULT_ARGV', 'Ring', 'PtySession', 'Gap', 'PtyRegistry', 'cull_loop']


class PtySession:
    "One pty process: reads land in a `Ring`, a change event wakes waiters, EOF reaps the exit status."
    delay = 0.1  # Seconds between escalation signals (ptyprocess's delayafterterminate)

    def __init__(self,
        argv:list[str],          # Command to spawn on the pty
        cwd:str=None,            # Working directory for the child
        env:dict=None,           # Child environment; None inherits this process's
        rows:int=24, cols:int=80,
        buffer_bytes:int=1_000_000,  # Ring bound: how much recent output is retained
        name:str=None,           # Optional handle, set by `PtyRegistry`
    ):
        self.name = name
        self._loop = asyncio.get_running_loop()
        self._evt, self._eof_evt = asyncio.Event(), asyncio.Event()
        self.core = PtyCore(list(argv), cwd=cwd, env=env, rows=rows, cols=cols, buffer_bytes=buffer_bytes)
        self.core.set_callback(self._on_change)

    def _on_change(self): self._loop.call_soon_threadsafe(self._notify)

    def _notify(self):
        evt, self._evt = self._evt, asyncio.Event()
        evt.set()
        if not self.core.alive: self._eof_evt.set()

    @property
    def ring(self): return self.core  # the core serves the ring view: `read_from`, `start`, `end`
    @property
    def alive(self): return self.core.alive
    @property
    def exit_code(self): return self.core.exit_code
    @property
    def last_activity(self): return self.core.last_activity
    @last_activity.setter
    def last_activity(self, v): self.core.last_activity = v

    async def wait_change(self,
        seen_end:int=None,   # Wake when `ring.end` passes this (None: the value at call time)
        timeout:float=None,  # Max seconds to wait; None waits indefinitely
    )->bool:                 # True when new output or EOF arrived, False on timeout
        if seen_end is None: seen_end = self.core.end
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.alive and self.core.end == seen_end:
            t = None if deadline is None else max(0, deadline - time.monotonic())
            try: await asyncio.wait_for(self._evt.wait(), t)
            except asyncio.TimeoutError: return False
        return True

    def read_from(self, offset:int, max_bytes_out:int=None): return self.core.read_from(offset, max_bytes_out)
    def write(self, data:bytes): self.core.write(data)
    def resize(self, rows:int, cols:int): self.core.resize(rows, cols)
    def kill(self, sig:int=signal.SIGTERM): self.core.kill(sig)

    async def wait(self)->int:
        "The exit code, once the pty dies (negative: terminating signal number)."
        await self._eof_evt.wait()
        return self.exit_code

    def model(self)->dict: return dict(name=self.name, alive=self.alive, last_activity=self.last_activity)

    async def __aenter__(self): return self
    async def __aexit__(self, *exc): await self.terminate(force=True)


@patch
async def terminate(self:PtySession, force:bool=False)->bool:
    "SIGHUP/SIGCONT/SIGINT/SIGTERM in turn, then SIGKILL if `force`; True if the child ended."
    for sig in (signal.SIGHUP, signal.SIGCONT, signal.SIGINT, signal.SIGTERM):
        if not self.alive: return True
        try: self.kill(sig)
        except ProcessLookupError: return True
        await asyncio.sleep(self.delay)
    if not self.alive: return True
    if force:
        try: self.kill(signal.SIGKILL)
        except ProcessLookupError: return True
        await asyncio.sleep(self.delay)
    return not self.alive


class Gap(int):
    "Bytes lost to ring trimming between two yields of an `attach` stream."

@patch
async def attach(self:PtySession,
    gaps:bool=False,       # Also yield `Gap(n)` markers when the stream fell behind the ring?
    from_start:bool=True,  # Begin at the oldest retained byte (replay); False starts at the live edge
):
    "Async byte stream over the session's output: replay, then live; ends at EOF."
    off = self.core.start if from_start else self.core.end
    while True:
        data, off, dropped = self.read_from(off)
        if dropped and gaps: yield Gap(dropped)
        if data: yield data
        elif not self.alive: return
        else: await self.wait_change(seen_end=off)


DEFAULT_ARGV = [os.environ.get('SHELL') or 'bash', '-i']

class PtyRegistry:
    "Named-session registry: get-or-create, list, terminate. An embedding app or a test drives this."

    def __init__(self,
        argv:list[str]=None,      # Default spawn command; creation requests may override it
        cull_timeout:float=0,     # Seconds of inactivity before a terminal is reaped (0 disables)
    ):
        self.argv = argv or DEFAULT_ARGV
        self.cull_timeout, self.terms = cull_timeout, {}

    def _next_name(self): return next(str(i) for i in itertools.count(1) if str(i) not in self.terms)

    async def create(self,
        name:str=None,           # Existing name reattaches; None auto-numbers
        argv:list[str]=None,     # Spawn command; the registry default if None
        cwd:str=None,            # Working directory for the child
        env:dict=None,           # Replaces the inherited environment
        appendenv:dict=None,     # Overlays the base environment
        rc:str=None,             # Shell setup text, written to a private dir; `{rcfile}`/`{rcdir}` substitute into argv and env values
        rows:int=24, cols:int=80,
    )->PtySession:
        "Get session `name` if it exists, else spawn one (auto-named when `name` is None)."
        if name and name in self.terms: return self.terms[name]
        name = name or self._next_name()
        argv = list(argv or self.argv)
        if rc is not None:
            rcdir = mkdtemp(prefix='ptymini-rc-')
            rcfile = str(Path(rcdir)/'.zshrc')  # one name serves bash (--rcfile {rcfile}) and zsh (ZDOTDIR={rcdir})
            Path(rcfile).write_text(rc)
            subst = lambda s: s.replace('{rcfile}', rcfile).replace('{rcdir}', rcdir)
            argv = [subst(a) for a in argv]
            if env: env = {k: subst(v) for k, v in env.items()}
            if appendenv: appendenv = {k: subst(v) for k, v in appendenv.items()}
        full_env = dict(os.environ if env is None else env) | (appendenv or {})  # `env` replaces the inherited environment; `appendenv` overlays the base
        t = PtySession(argv, cwd=cwd, env=full_env, rows=rows, cols=cols, name=name)
        self.terms[name] = t
        return t

    async def delete(self, name:str):
        t = self.terms.pop(name)
        await t.terminate(force=True)

    def cull_ready(self)->list[str]:
        "Names of terminals past the inactivity timeout (empty when culling is disabled)."
        if not self.cull_timeout: return []
        now = time.time()
        return [n for n, t in self.terms.items() if now - t.last_activity > self.cull_timeout]

    async def cull(self):
        for n in self.cull_ready():
            log.info('culling inactive session %s', n)
            await self.delete(n)

    async def shutdown(self):
        "Reap every terminal; nothing survives the registry."
        await asyncio.gather(*[self.delete(n) for n in list(self.terms)], return_exceptions=True)

    def get(self, name:str)->PtySession|None: return self.terms.get(name)
    def values(self): return self.terms.values()
    def __len__(self): return len(self.terms)

    async def __aenter__(self): return self
    async def __aexit__(self, *exc): await self.shutdown()


async def cull_loop(reg:PtyRegistry, interval:float=300):
    "Periodic cull sweep; an embedding app runs this as a task while it serves."
    while True:
        await asyncio.sleep(interval)
        await reg.cull()
