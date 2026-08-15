from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str
    app_version: str

    debug: bool
    log_level: str

    database_url: str

    redis_url: str
    queue_name: str

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
    )

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug_value(cls, value: object) -> object:
        if isinstance(value, str):
            normalized = value.strip().lower()

            if normalized in {"release", "production", "prod", "false", "0", "no", "off"}:
                return False

            if normalized in {"debug", "development", "dev", "true", "1", "yes", "on"}:
                return True

        return value


settings = Settings()
