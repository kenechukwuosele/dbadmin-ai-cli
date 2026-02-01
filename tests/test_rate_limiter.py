"""Unit tests for rate limiter."""

import time
import pytest
from dbadmin.ai.rate_limiter import (
    RateLimiter, 
    RateLimitConfig, 
    RateLimitExceeded,
    TokenBucket,
)


class TestTokenBucket:
    """Tests for TokenBucket class."""
    
    def test_initial_capacity(self):
        """Test bucket starts at full capacity."""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        assert bucket.consume(10) is True
        assert bucket.consume(1) is False
    
    def test_consume_tokens(self):
        """Test consuming tokens from bucket."""
        bucket = TokenBucket(capacity=5, refill_rate=1.0)
        assert bucket.consume(3) is True
        assert bucket.consume(3) is False  # Only 2 left
        assert bucket.consume(2) is True
    
    def test_refill_over_time(self):
        """Test bucket refills over time."""
        bucket = TokenBucket(capacity=10, refill_rate=100.0)  # 100 per second
        bucket.consume(10)  # Empty bucket
        time.sleep(0.1)  # Wait 0.1 seconds = ~10 tokens
        assert bucket.consume(5) is True
    
    def test_time_until_available(self):
        """Test calculating wait time."""
        bucket = TokenBucket(capacity=10, refill_rate=10.0)  # 10 per second
        bucket.consume(10)  # Empty bucket
        wait_time = bucket.time_until_available(5)
        assert 0.4 < wait_time < 0.6  # ~0.5 seconds for 5 tokens


class TestRateLimiter:
    """Tests for RateLimiter class."""
    
    def test_rate_limit_check_passes(self):
        """Test rate limit check passes for available capacity."""
        config = RateLimitConfig(requests_per_minute=100, tokens_per_minute=100000)
        limiter = RateLimiter(config)
        # Should not raise
        limiter.check_rate_limit("openai", estimated_tokens=1000)
    
    def test_rate_limit_exceeded_for_requests(self):
        """Test rate limit exceeded when requests exhausted."""
        # Use very low limits to ensure we hit them
        config = RateLimitConfig(
            requests_per_minute=2, 
            tokens_per_minute=100000,
            provider_limits={}  # Override default provider limits
        )
        limiter = RateLimiter(config)
        
        # Exhaust the request budget immediately
        limiter.check_rate_limit("test_provider", 100)
        limiter.check_rate_limit("test_provider", 100)
        
        # Third call should fail
        with pytest.raises(RateLimitExceeded):
            limiter.check_rate_limit("test_provider", 100)
    
    def test_per_provider_limits(self):
        """Test different providers have different limits."""
        config = RateLimitConfig(
            requests_per_minute=10,
            provider_limits={
                "openai": {"requests_per_minute": 5, "tokens_per_minute": 50000},
                "groq": {"requests_per_minute": 20, "tokens_per_minute": 100000},
            }
        )
        limiter = RateLimiter(config)
        
        # OpenAI should hit limit at 5
        for _ in range(5):
            limiter.check_rate_limit("openai", 100)
        with pytest.raises(RateLimitExceeded):
            limiter.check_rate_limit("openai", 100)
    
    def test_disabled_rate_limiting(self):
        """Test rate limiting can be disabled."""
        config = RateLimitConfig(requests_per_minute=1, enabled=False)
        limiter = RateLimiter(config)
        
        # Should not raise even after many calls
        for _ in range(100):
            limiter.check_rate_limit("openai", 1000)
    
    def test_usage_stats_tracking(self):
        """Test usage statistics are tracked."""
        config = RateLimitConfig(requests_per_minute=100, tokens_per_minute=100000)
        limiter = RateLimiter(config)
        
        limiter.check_rate_limit("openai", 500)
        limiter.check_rate_limit("openai", 300)
        
        stats = limiter.get_usage_stats("openai")
        assert stats["requests"] == 2
        assert stats["tokens"] == 800
    
    def test_actual_token_adjustment(self):
        """Test adjusting for actual token usage."""
        config = RateLimitConfig(requests_per_minute=100, tokens_per_minute=1000)
        limiter = RateLimiter(config)
        
        # Use estimated tokens
        limiter.check_rate_limit("openai", estimated_tokens=500)
        
        # Record that we only used 200
        limiter.record_actual_tokens("openai", actual_tokens=200, estimated_tokens=500)
        
        # Should have 300 tokens "refunded"
        capacity = limiter.get_remaining_capacity("openai")
        assert capacity["tokens"] > 400  # 500 + 300 refund minus some decay
