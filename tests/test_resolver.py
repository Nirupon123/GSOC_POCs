import unittest
from audio_ingestion_poc.url_resolver import URLResolver, TapMethod

class TestURLResolverRealNetwork(unittest.TestCase):
    def setUp(self):
        """Creates a fresh resolver instance for the test."""
        # Note: This requires yt-dlp to actually be installed on your machine!
        self.resolver = URLResolver()

    def test_real_youtube_live_extraction(self):
        """
        WARNING: This test hits the real internet.
        It uses the Lofi Girl 24/7 Live Stream to verify actual extraction.
        """
        lofi_girl_live_url = "https://www.youtube.com/watch?v=jfKfPfyJRdk"
        session_id = "real-network-test-001"

        print(f"\n[!] Connecting to YouTube for: {lofi_girl_live_url}")
        print("[!] Running yt-dlp (this may take 2-5 seconds)...")

        # 1. Execute the real resolution
        result = self.resolver.resolve(
            stream_url=lofi_girl_live_url,
            stream_type="youtube",
            session_id=session_id
        )

        # 2. Verify the results
        print(f"[SUCCESS] Extracted Manifest: {result.manifest_url[:80]}...") # Print first 80 chars
        
        # We can't predict the exact URL YouTube will generate, 
        # but we know it MUST be an HLS playlist (ending in .m3u8)
        self.assertTrue(
            result.manifest_url.endswith(".m3u8") or ".m3u8" in result.manifest_url,
            "The extracted URL is not a valid HLS manifest!"
        )
        
        self.assertEqual(result.tap_method, TapMethod.FFMPEG_HLS)
        self.assertFalse(result.cached)

        # 3. Test the cache in the real world
        print("[!] Testing cache hit for the same URL...")
        cached_result = self.resolver.resolve(
            stream_url=lofi_girl_live_url,
            stream_type="youtube",
            session_id=session_id
        )
        
        # It should be lightning fast and flagged as cached
        self.assertTrue(cached_result.cached)
        self.assertEqual(result.manifest_url, cached_result.manifest_url)


if __name__ == '__main__':
    unittest.main()