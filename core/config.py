# core/config.py  (pydantic v2 + pydantic-settings)
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # .env를 읽고, 비어있는 값은 무시하며, 알 수 없는 키는 무시
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    # 조달청 OpenAPI
    service_key: str = Field(default="", alias="SERVICE_KEY")
    bid_public_base: str = Field(
        default="http://apis.data.go.kr/1230000/ad/BidPublicInfoService",
        alias="BID_PUBLIC_BASE",
    )
    scsbid_base: str = Field(
        default="http://apis.data.go.kr/1230000/as/ScsbidInfoService",  # ★ /as 포함
        alias="SCSBID_BASE",
    )
    cntrct_info_base: str = Field(
        default="http://apis.data.go.kr/1230000/ao/CntrctInfoService",
        alias="CNTRCT_INFO_BASE",
    )
    cntrct_proc_base: str = Field(
        default="http://apis.data.go.kr/1230000/ao/CntrctProcssIntgOpenService",
        alias="CNTRCT_PROC_BASE",
    )
    pubstd_base: str = Field(
        default="http://apis.data.go.kr/1230000/ao/PubDataOpnStdService",
        alias="PUBSTD_BASE",
    )

    # DB (호스트에서 실행 기본값: localhost. 컨테이너 내부라면 .env에서 db로 바꾸세요)
    pg_dsn: str = Field(
        default="postgresql+psycopg://user:pass@localhost:5432/eest",
        alias="PG_DSN",
    )

    # 배치
    poll_window_minutes: int = Field(default=60, alias="POLL_WINDOW_MINUTES")

settings = Settings()