import asyncio
from typing import Any, Optional

class CustomEvent:
    """A wrapper around asyncio.Event to store associated data."""
    def __init__(self):
        self.event = asyncio.Event()
        self.data = None

class EventWaiter:

    def __init__(self):
        self.events = {}

    async def wait_for_event(self, event_id: str, timeout: int = None) -> Optional[Any]:
        # Use CustomEvent to store both the event and its data
        event = self.events.setdefault(event_id, CustomEvent())

        try:
            await asyncio.wait_for(event.event.wait(), timeout)
            return event.data
        except asyncio.TimeoutError:
            return None
        finally:
            # Clean up the event after it is triggered or times out
            self.events.pop(event_id, None)

    def trigger_event(self, event_id: str, data: Any) -> bool:
        if event_id not in self.events:
            return False

        # Set the data and trigger the event
        self.events[event_id].data = data
        self.events[event_id].event.set()
        return True