from pydantic_settings import BaseSettings


AUTO_DOMAIN = ".temp.com"


class Settings(BaseSettings):
    static_dir: str = "./static"
    redis_scan_count: int = 10000
    redis_pipeline_batch: int = 1000
    prefix_tree_max_depth: int = 64
    prefix_suggestion_count: int = 15


settings = Settings()
