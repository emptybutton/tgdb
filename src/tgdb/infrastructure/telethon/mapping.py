from telethon.types import Message as TelethonMessage

from tgdb.entities.message import Message


def message(telethon_message: TelethonMessage) -> Message:
    return Message(
        id=telethon_message.id,
        author_id=telethon_message.from_id.user_id,
    )
