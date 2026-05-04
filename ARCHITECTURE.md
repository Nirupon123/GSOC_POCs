# Architecture Rationale: Audio Ingestion POC

This document explains the technical decisions made during the implementation of the Audio Ingestion service for the GSoC project.

## 1. URL Resolution (`url_resolver.py`)
- **Tool Selection:** `yt-dlp` was chosen over lighter libraries because of its robust maintenance and support for thousands of streaming platforms.
- **Extraction Logic:** We prioritize HLS (`m3u8`) manifests. HLS is ideal for live streams as it allows the downstream player (FFmpeg) to handle network jitter and chunking natively.
- **Environment Safety:** The resolver calls `yt_dlp` as a module (`python -m`) rather than a standalone binary to ensure consistency across different OS environments and virtual environments.

## 2. FFmpeg Management (`ffmpeg_manager.py`)
- **Streaming Architecture:** We use `subprocess.PIPE` to stream audio directly into memory. This minimizes Disk I/O and provides the lowest possible latency for real-time transcription.
- **Audio Pre-processing:**
    - **Format:** `s16le` (Signed 16-bit Little Endian)
    - **Sample Rate:** `16,000 Hz`
    - **Channels:** `1 (Mono)`
    - *Decision:* These parameters are the native input requirements for Whisper ASR and Silero VAD. By handling this in FFmpeg (written in C), we offload the CPU-intensive resampling work from the Python interpreter.
- **Reliability Patterns:**
    - **Exponential Backoff:** Prevents aggressive reconnection attempts during network outages.
    - **Background Stderr Consumption:** Prevents "Pipe Clogging" where FFmpeg hangs because its error buffer is full.
    - **Generator Pattern:** The `read()` method is a Python generator, allowing the `chunk_builder` to control the flow of data (backpressure).

## 3. Maintainability
- **Type Hinting:** Extensive use of `dataclasses`, `Enums`, and type hints to make the code self-documenting and IDE-friendly.
- **Separation of Concerns:** `URLResolver` only cares about *finding* the stream; `FFmpegManager` only cares about *decoding* it. This makes it easy to swap out the decoder or the resolver later without breaking the whole system.
