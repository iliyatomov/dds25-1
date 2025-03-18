import asyncio
from typing import Any, Optional

class EventWaiter:

    def __init__(self):
        self.events = {}

    async def wait_for_event(self, event_id: str, timeout: int = None) -> Optional[Any]:
        event = self.events.setdefault(event_id, asyncio.Event())

        try:
            await asyncio.wait_for(event.wait(), timeout)
            return event.data
        except asyncio.TimeoutError:
            return None

    def trigger_event(self, event_id: str, data: Any) -> bool:
        if event_id not in self.events:
            return False

        self.events[event_id].data = data
        self.events[event_id].set()
        return True
