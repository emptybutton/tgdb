from pathlib import Path

from tgdb.infrastructure.pyyaml.config import TgdbConfig


def test_no_errors() -> None:
    TgdbConfig.load(Path("deploy/common/tgdb/config/config.yaml"))
