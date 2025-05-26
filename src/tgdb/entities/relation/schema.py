from collections.abc import Iterator
from dataclasses import dataclass

from tgdb.entities.relation.domain import Domain


@dataclass(frozen=True)
class Schema[IDDomainT: Domain]:
    id_domain: IDDomainT
    other_domains: tuple[Domain, ...]

    def __iter__(self) -> Iterator[Domain]:
        yield self.id_domain
        yield from self.other_domains

    def __len__(self) -> int:
        return len(self.other_domains) + 1
