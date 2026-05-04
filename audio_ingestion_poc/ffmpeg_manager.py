"""
Runs FFmpeg as a subprocess, reads raw PCM bytes 
from its stdout pipe,and yields those bytes to the 
chunk builder

This module owns one FFmpeg process per session
In production each session runs as its own Kubernetes Job,
so there is exactly one FFmpeg process per pod
crash isolation is free
"""

import logging
import subprocess
import time
import threading
from collections.abc import Iterator
from typing import Callable, Optional

log = logging.getLogger(__name__)

#predeclared variables

# PCM parameters (match Whisper & Silero-VAD)
SAMPLE_RATE= 16_000 #hz
SAMPLE_WIDTH=2 #bytes per sample (int16)
CHANNELS=1 #mono

#FFmpeg writes per read() call 
READ_CHUNK_BYTES = 4096 * SAMPLE_WIDTH

MAX_RETRIES   = 3
BASE_BACKOFF  = 1 #seconds




#Custom Exception
class FFmpegDiedError(Exception):
    """triggered when FFmpeg exhausted all retries and the session is now degraded"""

#FFmpegManager Class
class FFmpegManager:
    """
    Starts FFmpeg against a HLS manifest URL and exposes the decoded
    PCM stream as a byte iterator that the chunk builder consumes.

    on_degraded is an optional callback fired when all retries are
    exhausted so the orchestrator can update the session state in Redis
    without this module knowing anything about Redis.
    """

    def __init__(
        self,
        manifest_url: str,
        session_id: str,
        on_degraded: Optional[Callable[[str], None]] = None,
        ffmpeg_bin: str = "ffmpeg",
    ):
        self._url= manifest_url
        self._session_id= session_id
        self._on_degraded= on_degraded
        self._ffmpeg_bin= ffmpeg_bin

        self._proc: Optional[subprocess.Popen] = None
        self._stop_event= threading.Event()
        self._retry_count = 0


    def read(self) -> Iterator[bytes]:
        """
        Start FFmpeg and yield raw PCM bytes indefinitely.

        Restarts FFmpeg transparently on failure up to MAX_RETRIES times.
        Raises FFmpegDiedError after the final failed attempt.
        Stops cleanly when stop() is called.
        """
        try:
            while not self._stop_event.is_set():
                self._start_process()

                try:
                    yield from self._drain_stdout()
                except Exception as exc:
                    log.warning("session=%s ffmpeg read error: %s", self._session_id, exc)

                if self._stop_event.is_set():
                    break

                # restart logic for when FFmpeg died unexpectedly
                if self._retry_count >= MAX_RETRIES:
                    log.error("session=%s ffmpeg died after %d retries, marking degraded",self._session_id, MAX_RETRIES)
                    if self._on_degraded:
                        self._on_degraded(self._session_id)
                    raise FFmpegDiedError(f"FFmpeg failed {MAX_RETRIES} times for session {self._session_id}")

                wait = BASE_BACKOFF * (2 ** self._retry_count)
                self._retry_count += 1
                log.warning("session=%s ffmpeg died, retry %d/%d in %ds",self._session_id, self._retry_count, MAX_RETRIES, wait)
                time.sleep(wait)
        finally:
            self.stop()

    def stop(self) -> None:
        self._stop_event.set()
        self._kill_process()
        log.info("session=%s ffmpeg stopped", self._session_id)

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None


    #Internal methods
    def _build_ffmpeg_cmd(self) -> list[str]:
        """
        FFmpeg command that reads from an HLS manifest and writes raw
        PCM s16le at 16 kHz mono to stdout.

        Reconnect flags are only added for HTTP/HTTPS sources, FFmpeg
        rejects them for local files and non-HTTP protocols.
        """
        cmd = [self._ffmpeg_bin]

        # reconnect flags only make sense for network streams
        if self._url.startswith(("http://", "https://")):
            cmd += [
                "-reconnect",           "1",
                "-reconnect_streamed",  "1",
                "-reconnect_delay_max", "5",
            ]

        cmd += [
            "-i", self._url,
            "-vn",
            "-acodec","pcm_s16le",
            "-ar",str(SAMPLE_RATE),
            "-ac",str(CHANNELS),
            "-f","s16le",
            "-loglevel","error",
            "pipe:1",
        ]
        return cmd

    def _start_process(self) -> None:
        """Spawn a fresh FFmpeg subprocess, replacing any previous one."""
        self._kill_process()

        cmd = self._build_ffmpeg_cmd()
        log.info("session=%s starting ffmpeg (attempt %d)", self._session_id, self._retry_count + 1)

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,   # captured so we can log errors
            bufsize=0,                # unbuffered — we want bytes as they arrive
        )

        # Log FFmpeg stderr in a background thread so it never blocks stdout reads
        threading.Thread(
            target=self._log_stderr,
            args=(self._proc,),
            daemon=True,
            name=f"ffmpeg-stderr-{self._session_id}",
        ).start()

        log.info("session=%s ffmpeg pid=%d", self._session_id, self._proc.pid)

    def _drain_stdout(self) -> Iterator[bytes]:
        """
        Read from FFmpeg stdout and yield raw bytes to the chunk builder
        """
        assert self._proc and self._proc.stdout

        while not self._stop_event.is_set():
            raw = self._proc.stdout.read(READ_CHUNK_BYTES)

            if not raw:
                rc = self._proc.wait()
                if rc == 0:
                    log.info("session=%s stream ended cleanly", self._session_id)
                    self._stop_event.set()  # no retry on clean end
                else:
                    log.warning("session=%s ffmpeg crashed rc=%d", self._session_id, rc)
                return

            # Reset retry count since we successfully got data
            if self._retry_count > 0:
                self._retry_count = 0

            yield raw

    def _kill_process(self) -> None:
        """Terminate the FFmpeg process if it is still running."""
        if self._proc is None:
            return
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
        self._proc = None

    def _log_stderr(self, proc: subprocess.Popen) -> None:
        """Read FFmpeg stderr and forward every line to the logger."""
        if proc.stderr is None:
            return
        for line in proc.stderr:
            stripped = line.decode(errors="replace").rstrip()
            if stripped:
                log.warning("session=%s ffmpeg: %s", self._session_id, stripped)