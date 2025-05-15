from collections.abc import Sequence
from typing import NoReturn, cast
from uuid import UUID

from tgdb.async_queque import AsyncQueque
from tgdb.entities.row import (
    RowSchema,
    decoded_row,
    schema_name_of_encoded_row,
)
from tgdb.entities.transaction_effect import (
    schema_of_row_in_transaction,
)
from tgdb.telethon.in_telegram.message_queque import (
    InTelegramMessageQueque,
)
from tgdb.telethon.in_telegram.primitive import (
    InTelegramPrimitive,
)
from tgdb.telethon.in_telegram.row_heap import InTelegramRowHeap
from tgdb.telethon.transaction import (
    AsyncTransactionResults,
    Transaction,
    TransactionResult,
)
from tgdb.telethon.transaction_operator.operator import (
    TransactionOperator,
)
from tgdb.telethon.transaction_operator.transaction_mark import (
    TransactionState,
    transaction_state_mark_schema,
    transaction_uniqueness_mark_schema,
)


#     | 1 | 2 3
#  1  | 2 | 3
# 1 2 | 3 |


# 7               |-------|
# 6            |--------|
# 5              |-----|
# 4          |-----|


#  |---|
#        |--|

# 1 rv----wa
# 3     rv----wb


async def replicated_telegram_log_from_other_telegram_log(
    master_log: InTelegramMessageQueque,
    replica_log: InTelegramMessageQueque,
) -> None:
    last_replicated_message = await replica_log.top_message()

    async for message in master_log:
        is_message_duplicated = (
            last_replicated_message is not None
            and message.message == last_replicated_message.message
        )

        if not is_message_duplicated:
            await replica_log.push(message.message)

        await master_log.commit(message)
        last_replicated_message = message


# async def statically_replicated_telegram_heap_from_telegram_log(
#     log: InTelegramMessageQueque,
#     heap: InTelegramRowHeap,
#     schema_by_schema_name: dict[str, RowSchema],
#     block: bool,
# ) -> None:
#     schema_in_transaction_by_schema_name = {
#         _: schema_of_row_in_transaction(schema)
#         for _, schema in schema_by_schema_name.items()
#     }
#     schema_in_transaction_by_schema_name[transaction_state_mark_schema.name] = (
#         transaction_state_mark_schema
#     )
#     schema_in_transaction_by_schema_name[
#         transaction_uniqueness_mark_schema.name
#     ] = transaction_uniqueness_mark_schema

#     active_transaction_by_id = dict[UUID, Transaction]()


async def dynamically_replicated_telegram_heap_from_telegram_log(
    input_operators: AsyncQueque[TransactionOperator],
    output_results: AsyncQueque[TransactionResult],
    log: InTelegramMessageQueque,
    heap: InTelegramRowHeap,
    schema_by_schema_name: dict[str, RowSchema],
) -> NoReturn:
    schema_in_transaction_by_schema_name = {
        _: schema_of_row_in_transaction(schema)
        for _, schema in schema_by_schema_name.items()
    }
    schema_in_transaction_by_schema_name[transaction_state_mark_schema.name] = (
        transaction_state_mark_schema
    )
    schema_in_transaction_by_schema_name[
        transaction_uniqueness_mark_schema.name
    ] = transaction_uniqueness_mark_schema

    active_transaction_by_id = dict[UUID, Transaction]()

    last_committed_offset = 0

    async for message in log:
        if message.message is None:
            continue

        schema_name = schema_name_of_encoded_row(message.message)

        if schema_name is None:
            raise ValueError

        schema = schema_in_transaction_by_schema_name[schema_name]
        row = decoded_row(schema, message.message)

        if schema == transaction_state_mark_schema:
            transaction_id = cast(UUID, row[0])
            specified_transaction_state = cast(TransactionState, row[-2])
        else:
            transaction_id = cast(UUID, row[-1])
            specified_transaction_state = None

        if specified_transaction_state is TransactionState.started:
            if transaction_id not in active_transaction_by_id:
                new_transaction = Transaction(transaction_id)
                new_transaction.start(
                    active_transaction_by_id.values(), message.id
                )
                active_transaction_by_id[transaction_id] = new_transaction

        elif specified_transaction_state is TransactionState.rollbacked:
            transaction = active_transaction_by_id[transaction_id]

            transaction.rollback()
            del active_transaction_by_id[transaction_id]

        elif specified_transaction_state is TransactionState.committed:
            transaction = active_transaction_by_id[transaction_id]

            result = transaction.commit()

            del active_transaction_by_id[transaction_id]

            output_results.push(result)

            min(active_transaction_by_id)

        # elif (
        #     specified_transaction_state is None
        #     and schema == transaction_state_mark_schema
        # ):
        #     transaction = active_transaction_by_id[transaction_id]
        #     transaction[transaction_id].append(row)

        # if schema != transaction_state_mark_schema:
        #     transaction_id = cast(UUID, row[-1])

        #     if transaction_id not in rows_in_transaction_by_transaction_id:
        #         transaction_start_message_ids.append(message.id)
        #         transaction_start_message_id_by_transaction_id[
        #             transaction_id
        #         ] = message.id

        #     rows_in_transaction_by_transaction_id[transaction_id].append(row)

        #     continue

        # transaction_id = cast(UUID, row_in_transaction[-1])
        # transaction_state = cast(TransactionState, row_in_transaction[-2])

        # rows_in_transaction = (
        #     rows_in_transaction_by_transaction_id[transaction_id]
        # )

        # if transaction_result is TransactionState.committed:
        #     transaction_effect = (
        #         many(map(effect_from_row_in_transaction, rows_in_transaction))
        #     )


# async def replicated_telegram_heap_from_telegram_log(
    # log: InTelegramMessageQueque,
    # heap: InTelegramRowHeap,
    # current_replication_slot: InTelegramPrimitive[int],
    # other_replication_slots: Sequence[InTelegramPrimitive[int]],
    # schema_by_schema_name: dict[str, RowSchema],
# ) -> NoReturn:
    # schema_in_transaction_by_schema_name = {
    #     _: schema_of_row_in_transaction(schema)
    #     for _, schema in schema_by_schema_name.items()
    # }
    # schema_in_transaction_by_schema_name[transaction_result_row_schema.name] = (
    #     transaction_result_row_schema
    # )

    # rows_in_transaction_and_messages_by_transaction_id = defaultdict[
    #     UUID, list[tuple[Row, Message]]
    # ](list)

#     async for message in log:
        # if message.message is None:
        #     continue

        # schema_name = schema_name_of_encoded_row(message.message)

        # if schema_name is None:
        #     raise ValueError

        # schema = schema_in_transaction_by_schema_name[schema_name]
        # row = decoded_row(schema, message.message)

#         if schema != transaction_result_row_schema:
#             transaction_id = cast(UUID, row[-2])
#             transaction_result = cast(TransactionResult, row[-1])

#             if transaction_result is TransactionResult.rollbacked:
#                 replication_slots = await gather(*replication_slot_pointers)
#                 replication_slot = min(replication_slots, default=0)

#                 rows_in_transaction_and_messages = (
#                     rows_in_transaction_and_messages_by_transaction_id[
#                         transaction_id
#                     ]
#                 )

#                 message_to_delete = [
#                     message
#                     for _, message in rows_in_transaction_and_messages
#                 ]
#                 message_to_delete.append(message)

#                 messages_to_delete = [
#                     message for message in message_to_delete
#                     if message.id <= replication_slot
#                 ]

#                 self.


#         else:
#             transaction_id = cast(UUID, row[-1])

#             rows_in_transaction_by_transaction_id[transaction_id].append(
#                 row_in_transaction
#             )


# async def _executed_rollback(
#     log: InTelegramMessageQueque,
#     replication_slot_pointers: Sequence[InTelegramPrimitive[int]],
#     rows_in_transaction_and_messages
# ) -> None:
#     replication_slots = await gather(*replication_slot_pointers)
#     replication_slot = min(replication_slots, default=0)
