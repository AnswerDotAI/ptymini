
import sys, time

from ptymini.bg import Session, close_bgterm, list_sessions, poll, read, start_bgterm, write_stdin


def test_functional_write_stdin_collects_output_between_calls():
    cmd = [sys.executable, "-u", "-c", "import time; print('ready', flush=True); time.sleep(0.5); print('later', flush=True)"]
    sid = start_bgterm(cmd)
    try:
        assert sid in list_sessions()
        first = poll(sid, 100)
        assert "ready" in first.text

        second = poll(sid, 700)
        assert "later" in second.text
        assert second.dropped_bytes == 0
    finally: close_bgterm(sid)


def test_empty_write_stdin_returns_buffered_backlog_in_pages():
    cmd = [sys.executable, "-u", "-c", "import sys; sys.stdout.write('x' * 4096); sys.stdout.flush()"]
    sid = start_bgterm(cmd)
    try:
        time.sleep(0.1)
        first = write_stdin(sid, max_output_bytes=1024)
        second = read(sid, 1024)
        assert first.bytes_returned == 1024
        assert second.bytes_returned == 1024
        assert first.remaining_bytes > 0
    finally: close_bgterm(sid)


def test_small_buffer_reports_dropped_bytes():
    cmd = [sys.executable, "-u", "-c", "import sys; sys.stdout.write('a' * 4096); sys.stdout.flush()"]
    sid = start_bgterm(cmd, max_buffer_bytes=512)
    try:
        time.sleep(0.1)
        result = poll(sid, max_output_bytes=None)
        assert result.bytes_returned == 512
        assert result.dropped_bytes == 4096 - 512
    finally: close_bgterm(sid)


def test_functional_session_accepts_input_and_returns_echoed_output():
    cmd = [
        sys.executable,
        "-u",
        "-c",
        "print('ready', flush=True)\n"
        "import sys\n"
        "for line in sys.stdin:\n"
        "    sys.stdout.write(f'ACK:{line}')\n"
        "    sys.stdout.flush()\n"]
    sid = start_bgterm(cmd)
    try:
        ready = poll(sid, 500)
        assert "ready" in ready.text

        reply = write_stdin(sid, "hello\n", 100)
        if "ACK:hello" not in reply.text: reply = poll(sid, 500)
        assert "ACK:hello" in reply.text
    finally: close_bgterm(sid)


def test_object_wrapper_delegates_to_sid_api():
    cmd = [sys.executable, "-u", "-c", "print('ready', flush=True); raise SystemExit(3)"]
    with Session.start(cmd) as sess:
        ready = sess.poll(500)
        assert "ready" in ready.text
        assert sess.sid in list_sessions()
        assert sess.running
        assert sess.wait(3000) == 3
