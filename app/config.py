"""
Enhanced LLM-driven configuration for Version 2
"""

import os
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # OpenAI API configuration
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4o", alias="OPENAI_MODEL")  # Enhanced model
    openai_fallback_model: str = Field(default="gpt-4", alias="OPENAI_FALLBACK_MODEL")  # Fallback model
    
    # Application settings
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    request_timeout: int = Field(default=240, alias="REQUEST_TIMEOUT")  # Request timeout
    
    # API configuration
    max_request_size: int = Field(default=10 * 1024 * 1024, alias="MAX_REQUEST_SIZE")  # 10MB
    timeout_seconds: int = Field(default=240, alias="TIMEOUT_SECONDS")  # 4 minutes (safe for 5min limit)
    
    # Data processing settings
    max_plot_size: int = Field(default=100000, alias="MAX_PLOT_SIZE")  # 100KB for base64 images
    default_figure_size: tuple = (8, 6)
    
    # LLM-specific settings
    llm_temperature: float = Field(default=0.1, alias="LLM_TEMPERATURE")  # Low for consistent results
    llm_max_tokens: int = Field(default=4000, alias="LLM_MAX_TOKENS")
    llm_timeout: int = Field(default=30, alias="OPENAI_TIMEOUT")  # Updated from 20 to 30
    
    # Chart generation settings
    chart_dpi: int = Field(default=80, alias="CHART_DPI")
    chart_max_size_kb: int = Field(default=100, alias="CHART_MAX_SIZE_KB")
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore"
    }

def get_settings() -> Settings:
    """Get application settings instance"""
    return Settings()
