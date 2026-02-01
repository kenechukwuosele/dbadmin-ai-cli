"""Rate limiting for LLM API calls to prevent cost explosions.

Implements a token bucket algorithm with sliding window for both
request count and token usage limits.
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Optional


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded."""
    
    def __init__(self, message: str, retry_after_seconds: float = 0):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_minute: int = 20
    tokens_per_minute: int = 100_000
    enabled: bool = True
    
    # Per-provider overrides (some providers have different limits)
    provider_limits: dict[str, dict] = field(default_factory=lambda: {
        "openai": {"requests_per_minute": 60, "tokens_per_minute": 150_000},
        "groq": {"requests_per_minute": 30, "tokens_per_minute": 100_000},
        "openrouter": {"requests_per_minute": 100, "tokens_per_minute": 200_000},
        "anthropic": {"requests_per_minute": 50, "tokens_per_minute": 100_000},
        "ollama": {"requests_per_minute": 1000, "tokens_per_minute": 10_000_000},  # Local, essentially unlimited
    })


class TokenBucket:
    """Token bucket rate limiter with sliding window."""
    
    def __init__(self, capacity: int, refill_rate: float):
        """Initialize token bucket.
        
        Args:
            capacity: Maximum tokens in bucket
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from the bucket.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if insufficient
        """
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    def time_until_available(self, tokens: int = 1) -> float:
        """Calculate time until tokens are available.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Seconds until tokens available (0 if available now)
        """
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                return 0.0
            needed = tokens - self.tokens
            return needed / self.refill_rate


class RateLimiter:
    """Rate limiter for LLM API calls.
    
    Tracks both request count and token usage with separate buckets.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        """Initialize rate limiter.
        
        Args:
            config: Rate limit configuration, uses defaults if None
        """
        self.config = config or RateLimitConfig()
        self._request_buckets: dict[str, TokenBucket] = {}
        self._token_buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        
        # Track usage for monitoring
        self._usage_stats: dict[str, dict] = {}
    
    def _get_limits(self, provider: str) -> tuple[int, int]:
        """Get rate limits for a provider.
        
        Returns:
            (requests_per_minute, tokens_per_minute)
        """
        if provider in self.config.provider_limits:
            limits = self.config.provider_limits[provider]
            return limits.get("requests_per_minute", self.config.requests_per_minute), \
                   limits.get("tokens_per_minute", self.config.tokens_per_minute)
        return self.config.requests_per_minute, self.config.tokens_per_minute
    
    def _get_buckets(self, provider: str) -> tuple[TokenBucket, TokenBucket]:
        """Get or create rate limit buckets for a provider."""
        with self._lock:
            if provider not in self._request_buckets:
                rpm, tpm = self._get_limits(provider)
                # Refill rate is tokens per second
                self._request_buckets[provider] = TokenBucket(rpm, rpm / 60.0)
                self._token_buckets[provider] = TokenBucket(tpm, tpm / 60.0)
                self._usage_stats[provider] = {"requests": 0, "tokens": 0}
            
            return self._request_buckets[provider], self._token_buckets[provider]
    
    def check_rate_limit(
        self, 
        provider: str, 
        estimated_tokens: int = 1000
    ) -> None:
        """Check if request can proceed, raise if not.
        
        Args:
            provider: LLM provider name
            estimated_tokens: Estimated tokens for this request
            
        Raises:
            RateLimitExceeded: If rate limit would be exceeded
        """
        if not self.config.enabled:
            return
        
        request_bucket, token_bucket = self._get_buckets(provider)
        
        # Check request limit
        if not request_bucket.consume(1):
            wait_time = request_bucket.time_until_available(1)
            raise RateLimitExceeded(
                f"Request rate limit exceeded for {provider}. "
                f"Try again in {wait_time:.1f}s",
                retry_after_seconds=wait_time
            )
        
        # Check token limit (estimate before request)
        if not token_bucket.consume(estimated_tokens):
            wait_time = token_bucket.time_until_available(estimated_tokens)
            # Refund the request token since we can't proceed
            request_bucket.tokens += 1
            raise RateLimitExceeded(
                f"Token rate limit exceeded for {provider}. "
                f"Try again in {wait_time:.1f}s",
                retry_after_seconds=wait_time
            )
        
        # Track usage
        with self._lock:
            self._usage_stats[provider]["requests"] += 1
            self._usage_stats[provider]["tokens"] += estimated_tokens
    
    def record_actual_tokens(self, provider: str, actual_tokens: int, estimated_tokens: int) -> None:
        """Adjust token count after knowing actual usage.
        
        If actual usage differs from estimate, adjust the bucket.
        
        Args:
            provider: LLM provider name
            actual_tokens: Actual tokens used
            estimated_tokens: Previously estimated tokens
        """
        if not self.config.enabled:
            return
        
        _, token_bucket = self._get_buckets(provider)
        
        # Adjust for difference
        diff = actual_tokens - estimated_tokens
        if diff != 0:
            with token_bucket._lock:
                token_bucket.tokens -= diff  # Can go negative temporarily
    
    def get_usage_stats(self, provider: Optional[str] = None) -> dict:
        """Get usage statistics.
        
        Args:
            provider: Specific provider, or None for all
            
        Returns:
            Usage statistics dict
        """
        with self._lock:
            if provider:
                return self._usage_stats.get(provider, {"requests": 0, "tokens": 0})
            return dict(self._usage_stats)
    
    def get_remaining_capacity(self, provider: str) -> dict[str, float]:
        """Get remaining capacity for a provider.
        
        Returns:
            Dict with 'requests' and 'tokens' remaining
        """
        request_bucket, token_bucket = self._get_buckets(provider)
        return {
            "requests": max(0, request_bucket.tokens),
            "tokens": max(0, token_bucket.tokens),
        }


# Global rate limiter instance
_global_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance."""
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = RateLimiter()
    return _global_limiter


def configure_rate_limiter(config: RateLimitConfig) -> RateLimiter:
    """Configure and return the global rate limiter.
    
    Args:
        config: Rate limit configuration
        
    Returns:
        Configured rate limiter
    """
    global _global_limiter
    _global_limiter = RateLimiter(config)
    return _global_limiter
