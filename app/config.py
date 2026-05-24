from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str

    audio_bucket_name: str
    audio_endpoint_url: str | None = None
    audio_access_key_id: str
    audio_secret_access_key: str
    audio_region: str = "auto"

    worker_poll_seconds: int = 5
    local_tmp_dir: str = "/tmp/echo-labs-worker"

    model_config = SettingsConfigDict(extra="ignore")


settings = Settings()