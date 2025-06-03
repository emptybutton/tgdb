import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, kw_only=True)
class TelegramSecrets:
    api_id: int
    api_hash: str

    @classmethod
    def load(cls, secrets_file_path: Path) -> "TelegramSecrets":
        with secrets_file_path.open("rb") as secrets_file:
            raw_secrets = tomllib.load(secrets_file)

            return TelegramSecrets(
                api_id=raw_secrets["api_id"],
                api_hash=raw_secrets["api_hash"],
            )
