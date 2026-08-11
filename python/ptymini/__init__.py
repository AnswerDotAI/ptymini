"""Asyncio pty sessions: spawn, write, multi-client attach with replay, and a named registry

Modules:

- `ptymini.bg`: The bgterm API: sync, cursor-paged background terminal sessions
- `ptymini.core`: `PtySession` and `PtyRegistry`: asyncio pty sessions over one offset-tracked output ring"""

from ._core import __version__
