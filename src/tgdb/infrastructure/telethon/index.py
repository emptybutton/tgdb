from telethon.tl.types import Message

from tgdb.entities.relation.tuple import TID


type MessageID = int
type SenderID = int
type ChatID = int

type MessageIndex = tuple[MessageID, SenderID]
type TupleIndex = tuple[ChatID, TID]


def message_index(message: Message) -> MessageIndex:
    return message.id, message.sender_id  # type: ignore[attr-defined]
