from dataclasses import dataclass
from pathlib import Path

import typenv


@dataclass(frozen=True)
class Envs:
    config_path: Path

    @classmethod
    def load(cls) -> "Envs":
        env = typenv.Env()

        return Envs(
            config_path=Path(env.str("CONFIG_PATH")),
        )
