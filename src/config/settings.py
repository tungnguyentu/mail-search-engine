from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    EMAIL_EVENTS_TOPIC: str = "email.events"
    INDEX_DIR: str = "email_index"
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    S3_ENDPOINT_URL: str
    S3_BUCKET_NAME: str

    class Config:
        env_file = ".env"

settings = Settings()
print(settings)