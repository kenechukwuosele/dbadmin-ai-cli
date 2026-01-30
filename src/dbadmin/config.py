"""Configuration management for DbAdmin AI using Pydantic settings."""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConnection(BaseSettings):
    """Individual database connection settings."""
    
    model_config = SettingsConfigDict(extra="ignore")
    
    url: str
    name: str = ""
    db_type: str = ""  # postgresql, mysql, mongodb, redis


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    # LLM API Keys (in priority order)
    openrouter_api_key: Optional[str] = Field(default=None)
    groq_api_key: Optional[str] = Field(default=None)
    openai_api_key: SecretStr = Field(default=SecretStr(""))
    
    # LLM Settings
    openai_model: str = Field(default="gpt-4o")
    openai_embedding_model: str = Field(default="text-embedding-3-small")
    
    # Database URLs (optional - can be added dynamically)
    postgres_url: Optional[str] = Field(default=None)
    mysql_url: Optional[str] = Field(default=None)
    mongodb_url: Optional[str] = Field(default=None)
    redis_url: Optional[str] = Field(default=None)
    
    # ChromaDB settings
    chroma_persist_dir: Path = Field(default=Path("./data/chroma"))
    
    # Logging
    log_level: str = Field(default="INFO")
    
    # Analysis settings
    slow_query_threshold_ms: int = Field(default=1000)  # 1 second
    health_check_interval_seconds: int = Field(default=10)
    
    def get_configured_databases(self) -> dict[str, str]:
        """Get all configured database URLs."""
        dbs = {}
        if self.postgres_url:
            dbs["postgresql"] = self.postgres_url
        if self.mysql_url:
            dbs["mysql"] = self.mysql_url
        if self.mongodb_url:
            dbs["mongodb"] = self.mongodb_url
        if self.redis_url:
            dbs["redis"] = self.redis_url
        return dbs


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
