from dataclasses import dataclass

from tgdb.telethon.in_telegram.value import InTelegramValue
from tgdb.telethon.primitive import (
    Primitive,
    decoded_primitive,
    encoded_primitive,
)


@dataclass(frozen=True)
class InTelegramPrimitive[PrimitiveT: Primitive](InTelegramValue[PrimitiveT]):
    primitive_type: type[PrimitiveT]

    def _encoded(self, primitive: PrimitiveT) -> str:
        return encoded_primitive(primitive)

    def _decoded(self, encoded_primitive: str) -> PrimitiveT:
        return decoded_primitive(encoded_primitive, self.primitive_type)
