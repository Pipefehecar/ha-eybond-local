"""Single-use, exact-route capability; never a general mutation-fence bypass."""

from dataclasses import dataclass
from typing import Awaitable, Callable


@dataclass(slots=True)
class ProtectedLearningRoutePermit:
    entry_id: str
    owner_id: str
    collector_pn: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    validate: Callable[[], Awaitable[None]]
    consumed: bool = False

    async def consume(self, *, entry_id, owner_id, collector_pn, listen_port, upstream_host, upstream_port):
        if (self.consumed or (entry_id, owner_id, collector_pn, listen_port, upstream_host, upstream_port)
                != (self.entry_id, self.owner_id, self.collector_pn, self.listen_port,
                    self.upstream_host, self.upstream_port)):
            raise RuntimeError("cloud_learning_retry_route_mismatch")
        self.consumed = True
        await self.validate()
