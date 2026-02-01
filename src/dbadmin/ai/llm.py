"""Plug-and-play LLM client supporting any OpenAI-compatible API."""

import os
import logging
from dataclasses import dataclass
from typing import Generator, Optional

from openai import OpenAI, APIError, APIConnectionError, RateLimitError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from dbadmin.ai.rate_limiter import get_rate_limiter, RateLimitExceeded, RateLimitConfig

logger = logging.getLogger(__name__)


# Pre-configured providers (all OpenAI-compatible)
PROVIDERS = {
    # Free/cheap options
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "env_key": "OPENROUTER_API_KEY",
        "default_model": "meta-llama/llama-3.1-8b-instruct:free",
        "models": [
            "meta-llama/llama-3.1-8b-instruct:free",
            "meta-llama/llama-3.1-70b-instruct",
            "mistralai/mistral-7b-instruct:free",
            "google/gemma-2-9b-it:free",
            "anthropic/claude-3.5-sonnet",
            "openai/gpt-4o",
        ],
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "env_key": "GROQ_API_KEY",
        "default_model": "llama-3.1-70b-versatile",
        "models": ["llama-3.1-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768"],
    },
    "ollama": {
        "base_url": "http://localhost:11434/v1",
        "env_key": None,
        "default_model": "llama3.1",
        "models": ["llama3.1", "mistral", "codellama", "phi3"],
    },
    # Paid options
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "env_key": "OPENAI_API_KEY",
        "default_model": "gpt-4o",
        "models": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
    },
    "anthropic": {
        "base_url": "https://api.anthropic.com/v1",
        "env_key": "ANTHROPIC_API_KEY",
        "default_model": "claude-3-5-sonnet-20241022",
        "models": ["claude-3-5-sonnet-20241022", "claude-3-opus-20240229"],
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "env_key": "TOGETHER_API_KEY",
        "default_model": "meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo",
        "models": ["meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo", "mistralai/Mixtral-8x7B-Instruct-v0.1"],
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "env_key": "DEEPSEEK_API_KEY",
        "default_model": "deepseek-chat",
        "models": ["deepseek-chat", "deepseek-coder"],
    },
}


# Fallback chains for reliability - try these providers in order on failure
FALLBACK_CHAINS = {
    "openai": [("groq", "llama-3.1-70b-versatile"), ("openrouter", "meta-llama/llama-3.1-70b-instruct")],
    "groq": [("openrouter", "meta-llama/llama-3.1-70b-instruct"), ("openai", "gpt-4o-mini")],
    "anthropic": [("openai", "gpt-4o"), ("groq", "llama-3.1-70b-versatile")],
    "openrouter": [("groq", "llama-3.1-70b-versatile"), ("openai", "gpt-4o-mini")],
    "together": [("groq", "llama-3.1-70b-versatile"), ("openrouter", "meta-llama/llama-3.1-70b-instruct")],
    "deepseek": [("groq", "llama-3.1-70b-versatile"), ("openrouter", "meta-llama/llama-3.1-70b-instruct")],
    "ollama": [("groq", "llama-3.1-8b-instant"), ("openrouter", "meta-llama/llama-3.1-8b-instruct:free")],
}


class AllProvidersFailedError(Exception):
    """Raised when all providers in the fallback chain have failed."""
    pass


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    provider: str
    tokens_used: int = 0
    used_fallback: bool = False  # True if response came from fallback provider


class LLMClient:
    """Plug-and-play LLM client.
    
    Supports any OpenAI-compatible API. Just set the right env var:
    
    Free options:
        OPENROUTER_API_KEY - Access to 100+ models, many free
        GROQ_API_KEY - Fast Llama/Mixtral inference
        (no key) - Ollama local models
    
    Paid options:
        OPENAI_API_KEY - GPT-4, GPT-4o
        ANTHROPIC_API_KEY - Claude models
        TOGETHER_API_KEY - Together.ai models
        DEEPSEEK_API_KEY - DeepSeek models
    
    Custom provider:
        Set LLM_BASE_URL + LLM_API_KEY for any OpenAI-compatible API
    """
    
    def __init__(
        self,
        provider: str = None,
        model: str = None,
        api_key: str = None,
        base_url: str = None,
    ):
        """Initialize LLM client.
        
        Args:
            provider: Provider name (openrouter, groq, openai, etc.) or auto-detect
            model: Model name, or use provider default
            api_key: API key, or read from environment
            base_url: Custom base URL for any OpenAI-compatible API
        """
        # Custom provider via env vars
        if base_url or os.getenv("LLM_BASE_URL"):
            self.provider = "custom"
            self.base_url = base_url or os.getenv("LLM_BASE_URL")
            self.api_key = api_key or os.getenv("LLM_API_KEY", "")
            self.model = model or os.getenv("LLM_MODEL", "default")
        else:
            # Auto-detect or use specified provider
            self.provider = provider or self._auto_detect_provider()
            config = PROVIDERS.get(self.provider, PROVIDERS["openrouter"])
            
            self.base_url = config["base_url"]
            self.model = model or config["default_model"]
            
            if api_key:
                self.api_key = api_key
            elif config["env_key"]:
                self.api_key = os.getenv(config["env_key"], "")
            else:
                self.api_key = "not-needed"
        
        # Initialize OpenAI-compatible client
        extra_headers = {}
        if self.provider == "openrouter":
            extra_headers = {
                "HTTP-Referer": "https://github.com/dbadmin-ai",
                "X-Title": "DbAdmin AI",
            }
        
        self._client = OpenAI(
            api_key=self.api_key or "dummy",
            base_url=self.base_url,
            default_headers=extra_headers if extra_headers else None,
        )
    
    def _auto_detect_provider(self) -> str:
        """Auto-detect provider based on available API keys."""
        # Check in order of preference (free first)
        for provider, config in PROVIDERS.items():
            env_key = config.get("env_key")
            if env_key and os.getenv(env_key):
                return provider
            if provider == "ollama" and self._check_ollama():
                return provider
        return "openrouter"  # Default
    
    def _check_ollama(self) -> bool:
        """Check if Ollama is running."""
        try:
            import httpx
            return httpx.get("http://localhost:11434/api/tags", timeout=1).status_code == 200
        except Exception:
            return False
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(min=1, max=10),
        retry=retry_if_exception_type((APIConnectionError,))
    )
    def complete(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000,
        check_rate_limit: bool = True,
    ) -> LLMResponse:
        """Get a completion from the LLM.
        
        Args:
            messages: Chat messages
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            check_rate_limit: Whether to enforce rate limiting
            
        Raises:
            RateLimitExceeded: If rate limit is exceeded
        """
        # Estimate tokens (rough: 4 chars = 1 token)
        estimated_tokens = sum(len(m.get("content", "")) for m in messages) // 4 + max_tokens
        
        # Check rate limit before making request
        if check_rate_limit:
            rate_limiter = get_rate_limiter()
            rate_limiter.check_rate_limit(self.provider, estimated_tokens)
        
        response = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        
        actual_tokens = response.usage.total_tokens if response.usage else 0
        
        # Record actual token usage for accurate rate limiting
        if check_rate_limit:
            rate_limiter.record_actual_tokens(self.provider, actual_tokens, estimated_tokens)
        
        return LLMResponse(
            content=response.choices[0].message.content,
            model=self.model,
            provider=self.provider,
            tokens_used=actual_tokens,
        )
    
    def complete_with_fallback(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> LLMResponse:
        """Get completion with automatic fallback to backup providers.
        
        Tries the primary provider first, then falls back to alternatives
        if it fails due to API errors or rate limits.
        
        Args:
            messages: Chat messages
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            
        Returns:
            LLMResponse with used_fallback=True if fallback was used
            
        Raises:
            AllProvidersFailedError: If all providers fail
        """
        errors = []
        
        # Try primary provider first
        try:
            return self.complete(messages, temperature, max_tokens)
        except (APIError, APIConnectionError, RateLimitError, RateLimitExceeded) as e:
            logger.warning(f"Primary provider {self.provider} failed: {e}")
            errors.append((self.provider, self.model, str(e)))
        
        # Try fallback chain
        fallback_chain = FALLBACK_CHAINS.get(self.provider, [])
        
        for fallback_provider, fallback_model in fallback_chain:
            # Check if fallback provider is available
            config = PROVIDERS.get(fallback_provider, {})
            env_key = config.get("env_key")
            
            if env_key and not os.getenv(env_key):
                continue  # Skip unavailable providers
            
            try:
                logger.info(f"Trying fallback: {fallback_provider}/{fallback_model}")
                fallback_client = LLMClient(provider=fallback_provider, model=fallback_model)
                response = fallback_client.complete(messages, temperature, max_tokens)
                response.used_fallback = True
                return response
            except (APIError, APIConnectionError, RateLimitError, RateLimitExceeded) as e:
                logger.warning(f"Fallback {fallback_provider} failed: {e}")
                errors.append((fallback_provider, fallback_model, str(e)))
                continue
        
        # All providers failed
        error_summary = "; ".join([f"{p}/{m}: {e}" for p, m, e in errors])
        raise AllProvidersFailedError(f"All providers failed: {error_summary}")
    
    def stream(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000,
        check_rate_limit: bool = True,
    ) -> Generator[str, None, None]:
        """Stream a completion response.
        
        Args:
            messages: Chat messages
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            check_rate_limit: Whether to enforce rate limiting
        """
        # Estimate tokens for rate limiting
        estimated_tokens = sum(len(m.get("content", "")) for m in messages) // 4 + max_tokens
        
        if check_rate_limit:
            rate_limiter = get_rate_limiter()
            rate_limiter.check_rate_limit(self.provider, estimated_tokens)
        
        response = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )
        
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
    
    @classmethod
    def list_providers(cls) -> dict:
        """List all supported providers and their models."""
        return {name: config["models"] for name, config in PROVIDERS.items()}
    
    @classmethod
    def get_available_provider(cls) -> str | None:
        """Get the first available provider based on env vars."""
        for provider, config in PROVIDERS.items():
            if config.get("env_key") and os.getenv(config["env_key"]):
                return provider
        return None


def get_llm_client(
    provider: str = None,
    model: str = None,
) -> LLMClient:
    """Get configured LLM client with auto-detection."""
    return LLMClient(provider=provider, model=model)
