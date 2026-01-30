"""Plug-and-play LLM client supporting any OpenAI-compatible API."""

import os
from dataclasses import dataclass
from typing import Generator

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential


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


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    provider: str
    tokens_used: int = 0


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
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def complete(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> LLMResponse:
        """Get a completion from the LLM."""
        response = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        
        return LLMResponse(
            content=response.choices[0].message.content,
            model=self.model,
            provider=self.provider,
            tokens_used=response.usage.total_tokens if response.usage else 0,
        )
    
    def stream(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> Generator[str, None, None]:
        """Stream a completion response."""
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
