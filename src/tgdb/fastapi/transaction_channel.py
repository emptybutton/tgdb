from asyncio import gather

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, WebSocket

from tgdb.async_queque import AsyncQueque
from tgdb.telethon.transaction import TransactionResult
from tgdb.telethon.transaction_operator.operator import (
    TransactionOperator,
)


transaction_channel_router = APIRouter()


@transaction_channel_router.websocket("/transactions")
@inject
async def transaction_channel_route(
    websocket: WebSocket,
    input_async_queque: FromDishka[AsyncQueque[TransactionOperator]],
    output_async_queque: FromDishka[AsyncQueque[TransactionResult]],
) -> None:
    await websocket.accept()

    await gather(
        _accept_operators(websocket, input_async_queque),
        _send_results(websocket, output_async_queque),
    )


async def _accept_operators(
    websocket: WebSocket,
    input_async_queque: AsyncQueque[str],
) -> None:
    async for operator in websocket.iter_text():
        input_async_queque.push(operator)


async def _send_results(
    websocket: WebSocket,
    output_async_queque: AsyncQueque[TransactionResult],
) -> None:
    async for transaction_result in output_async_queque:
        encoded_transaction_result = transaction_result.model_dump_json(
            by_alias=True
        )
        await websocket.send_text(encoded_transaction_result)
