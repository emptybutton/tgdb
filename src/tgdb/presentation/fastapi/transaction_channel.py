from asyncio import gather

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, WebSocket

from tgdb.application.ports.heap import Heap
from tgdb.application.ports.log import LogSlot
from tgdb.application.ports.logic_clock import LogicClock
from tgdb.application.ports.async_queque import AsyncQueque
from tgdb.entities.operator import Operator, applied_operator
from tgdb.entities.transaction import (
    TransactionCommit,
    TransactionFailedCommit,
    TransactionOkCommit,
)
from tgdb.entities.transaction_horizon import (
    TransactionHorizon,
    create_transaction_horizon,
)


transaction_channel_router = APIRouter()


input_operators: AsyncQueque[Operator]
output_commits: AsyncQueque[TransactionCommit]


@transaction_channel_router.websocket("/transactions")
@inject
async def transaction_channel_route(
    websocket: WebSocket,
    input_operators: FromDishka[AsyncQueque[Operator]],
    output_commits: FromDishka[AsyncQueque[TransactionCommit]],
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
