from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    neon_database_url: str | None = None
    secret_key: str
    environment: str = "development"
    debug: bool = True

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
