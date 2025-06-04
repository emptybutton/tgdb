from pathlib import Path

from tgdb.infrastructure.pyyaml.conf import Conf


def test_no_errors() -> None:
    Conf.load(Path("deploy/dev/tgdb/conf.yaml"))
