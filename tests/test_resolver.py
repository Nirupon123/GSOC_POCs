"""
"""

import unittest
import imageio_ffmpeg
from collections.abc import Iterator

from audio_ingestion_poc.url_resolver import URLResolver, TapMethod, ResolvedStream
from audio_ingestion_poc.ffmpeg_manager import FFmpegManager, SAMPLE_RATE, SAMPLE_WIDTH


# 2 seconds of audio at 16 kHz, 16-bit mono = 64 000 bytes
CHUNK_DURATION_SECS  = 2
CHUNK_SIZE_BYTES     = SAMPLE_RATE * SAMPLE_WIDTH * CHUNK_DURATION_SECS


class SimpleChunkBuilder:
    """
    Accumulates raw PCM bytes from FFmpegManager and emits fixed-size
    chunks that are ready for VAD / ASR processing.

    This is a stand-in for the real ChunkBuilder. It exists only to
    make the integration test realistic without requiring the full
    chunk_builder module to be implemented yet.
    """

    def __init__(self, chunk_size: int = CHUNK_SIZE_BYTES):
        self._chunk_size = chunk_size
        self._buffer = bytearray()

    def feed(self, raw: bytes) -> list[bytes]:
        """
        Push raw PCM bytes in, get back a (possibly empty) list of
        complete chunks. Partial data is held in an internal buffer
        until enough bytes arrive to fill the next chunk.
        """
        self._buffer.extend(raw)
        complete_chunks = []
        while len(self._buffer) >= self._chunk_size:
            complete_chunks.append(bytes(self._buffer[: self._chunk_size]))
            del self._buffer[: self._chunk_size]
        return complete_chunks

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)


# Shared constants

LOFI_GIRL_URL = "https://www.youtube.com/watch?v=jfKfPfyJRdk"
FFMPEG_BIN    = imageio_ffmpeg.get_ffmpeg_exe()  



# URLResolver unit tests
class TestURLResolver(unittest.TestCase):
    """Verifies that URLResolver correctly extracts HLS manifests."""

    def setUp(self):
        self.resolver = URLResolver()

    def test_youtube_live_resolves_to_hls_manifest(self):
        """
        Hitting the real YouTube API to confirm yt-dlp can extract a
        valid HLS manifest URL from a live-stream watch page.
        """
        print(f"\n[URLResolver] Resolving: {LOFI_GIRL_URL}")
        result = self.resolver.resolve(
            stream_url=LOFI_GIRL_URL,
            stream_type="youtube",
            session_id="unit-resolver-001",
        )

        print(f"[URLResolver] Manifest (first 80 chars): {result.manifest_url[:80]}...")
        self.assertIn(".m3u8", result.manifest_url,
                      "Resolved URL must be an HLS manifest")
        self.assertEqual(result.tap_method, TapMethod.FFMPEG_HLS)
        self.assertFalse(result.cached, "First call must NOT be a cache hit")

    def test_second_resolve_is_cache_hit(self):
        """
        Calling resolve() twice with the same session_id must return the
        cached manifest on the second call without running yt-dlp again.
        """
        session_id = "unit-resolver-cache-001"
        first  = self.resolver.resolve(LOFI_GIRL_URL, "youtube", session_id)
        second = self.resolver.resolve(LOFI_GIRL_URL, "youtube", session_id)

        self.assertTrue(second.cached, "Second call must be a cache hit")
        self.assertEqual(first.manifest_url, second.manifest_url,
                         "Cached URL must match the original")

    def test_unsupported_stream_type_raises(self):
        """Zoom streams cannot be tapped server-side; resolver must raise."""
        from audio_ingestion_poc.url_resolver import UnsupportedStreamError
        with self.assertRaises(UnsupportedStreamError):
            self.resolver.resolve("https://zoom.us/j/123", "zoom", "unit-resolver-zoom")

    def test_invalid_stream_type_raises_value_error(self):
        """Passing a completely unknown stream type must raise ValueError."""
        with self.assertRaises(ValueError):
            self.resolver.resolve("https://example.com", "nonexistent", "unit-resolver-bad")


# FFmpegManager unit tests
class TestFFmpegManager(unittest.TestCase):
    """
    Verifies that FFmpegManager streams raw PCM bytes from a live HLS
    manifest and shuts down cleanly.
    """

    @classmethod
    def setUpClass(cls):
        """
        Resolve the manifest once for the whole test class to avoid
        calling yt-dlp multiple times.
        """
        resolver = URLResolver()
        result = resolver.resolve(LOFI_GIRL_URL, "youtube", "unit-ffmpeg-setup")
        cls.manifest_url = result.manifest_url

    def _make_ffmpeg(self, session_id: str) -> FFmpegManager:
        return FFmpegManager(
            manifest_url=self.manifest_url,
            session_id=session_id,
            ffmpeg_bin=FFMPEG_BIN,
        )

    def test_reads_non_empty_pcm_bytes(self):
        """FFmpegManager must yield non-empty byte chunks."""
        ffmpeg = self._make_ffmpeg("unit-ffmpeg-read")
        chunks_received = 0

        try:
            for chunk in ffmpeg.read():
                self.assertGreater(len(chunk), 0, "Chunk must not be empty")
                chunks_received += 1
                if chunks_received >= 3:
                    break
        finally:
            ffmpeg.stop()

        self.assertEqual(chunks_received, 3)

    def test_is_running_reflects_process_state(self):
        """is_running must be True while streaming and False after stop()."""
        ffmpeg = self._make_ffmpeg("unit-ffmpeg-state")

        try:
            for _ in ffmpeg.read():
                # After the first chunk arrives the process must be alive
                self.assertTrue(ffmpeg.is_running, "Process must be running")
                break
        finally:
            ffmpeg.stop()

        self.assertFalse(ffmpeg.is_running, "Process must be stopped after stop()")


# End-to-end integration test
class TestPipelineIntegration(unittest.TestCase):
    """
    Mirrors the real production flow
    """

    TARGET_CHUNKS = 3   # build 3 complete 2-second audio chunks

    def test_full_pipeline_url_resolver_to_chunk_builder(self):
        """
        Runs the complete audio-ingestion pipeline against a live YouTube
        stream and asserts that each layer hands off the correct data type
        to the next.
        """

        # Stage 1: URL Resolution 
        print("\n[Pipeline] Stage 1: URLResolver -> resolving HLS manifest...")
        resolver = URLResolver()
        resolved: ResolvedStream = resolver.resolve(
            stream_url=LOFI_GIRL_URL,
            stream_type="youtube",
            session_id="integration-test-001",
        )

        self.assertIn(".m3u8", resolved.manifest_url)
        self.assertEqual(resolved.tap_method, TapMethod.FFMPEG_HLS)
        print(f"[Pipeline] OK Manifest URL obtained ({len(resolved.manifest_url)} chars)")

        # Stage 2: FFmpeg byte streaming 
        print("[Pipeline] Stage 2: FFmpegManager -> streaming raw PCM...")
        ffmpeg = FFmpegManager(
            manifest_url=resolved.manifest_url,
            session_id=resolved.session_id,
            ffmpeg_bin=FFMPEG_BIN,
        )

        # Stage 3: Chunk assembly 
        print(f"[Pipeline] Stage 3: ChunkBuilder -> assembling {CHUNK_SIZE_BYTES}-byte chunks "
              f"({CHUNK_DURATION_SECS}s @ {SAMPLE_RATE}Hz mono 16-bit)...")
        builder = SimpleChunkBuilder(chunk_size=CHUNK_SIZE_BYTES)
        complete_chunks: list[bytes] = []

        try:
            for raw_pcm in ffmpeg.read():
                # Feed raw bytes into the chunk builder
                new_chunks = builder.feed(raw_pcm)
                for audio_chunk in new_chunks:
                    complete_chunks.append(audio_chunk)
                    print(f"  [Pipeline] OK Chunk {len(complete_chunks)} ready: "
                          f"{len(audio_chunk)} bytes  |  "
                          f"buffer remaining: {builder.buffered_bytes} bytes")

                if len(complete_chunks) >= self.TARGET_CHUNKS:
                    print(f"[Pipeline] Reached target of {self.TARGET_CHUNKS} chunks. Stopping.")
                    break
        finally:
            ffmpeg.stop()

        # Assertions
        self.assertGreaterEqual(len(complete_chunks), self.TARGET_CHUNKS,
                                "Pipeline must produce the expected number of chunks")

        for i, chunk in enumerate(complete_chunks):
            self.assertEqual(len(chunk), CHUNK_SIZE_BYTES,
                             f"Chunk {i + 1} size must be exactly {CHUNK_SIZE_BYTES} bytes")
            # PCM s16le bytes: verify the length is always a multiple of SAMPLE_WIDTH
            self.assertEqual(len(chunk) % SAMPLE_WIDTH, 0,
                             f"Chunk {i + 1} length must be a multiple of {SAMPLE_WIDTH} (int16)")

        self.assertFalse(ffmpeg.is_running, "FFmpeg must be stopped after the pipeline finishes")
        print("\n[Pipeline] OK All stages passed. End-to-end pipeline is working correctly.")


if __name__ == "__main__":
    unittest.main(verbosity=2)