import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, kw_only=True)
class TelegramSecrets:
    app_api_id: int
    app_api_hash: str

    @classmethod
    def load(cls, secrets_file_path: Path) -> "TelegramSecrets":
        with secrets_file_path.open("rb") as secrets_file:
            raw_secrets = tomllib.load(secrets_file)

            return TelegramSecrets(
                app_api_id=raw_secrets["app_api_id"],
                app_api_hash=raw_secrets["app_api_hash"],
            )
