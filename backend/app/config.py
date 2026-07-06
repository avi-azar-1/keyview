from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    static_dir: str = "./static"
    redis_scan_count: int = 1000
    redis_pipeline_batch: int = 100


settings = Settings()
