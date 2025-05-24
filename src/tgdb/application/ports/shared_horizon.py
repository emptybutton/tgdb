from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager

from tgdb.entities.transaction_horizon import TransactionHorizon


class SharedHorizon(AbstractAsyncContextManager[TransactionHorizon], ABC): ...
