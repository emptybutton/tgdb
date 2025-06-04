from dataclasses import dataclass
from pathlib import Path

import typenv


@dataclass(frozen=True)
class Envs:
    conf_path: Path

    @classmethod
    def load(cls) -> "Envs":
        env = typenv.Env()

        return Envs(
            conf_path=Path(env.str("CONF_PATH")),
        )
