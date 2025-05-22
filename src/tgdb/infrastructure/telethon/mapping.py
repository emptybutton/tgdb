from telethon.tl.custom.message import Message as TelethonMessage
from telethon.tl.types import PeerUser

from tgdb.entities.message import Message


def message(telethon_message: TelethonMessage) -> Message:
    assert isinstance(telethon_message.from_id, PeerUser)

    return Message(
        id=telethon_message.id,
        author_id=telethon_message.from_id.user_id,
    )
