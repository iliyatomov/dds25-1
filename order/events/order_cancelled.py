from msgspec import Struct

class OrderCancelledEvent(Struct):
    order_id: str
    user_id: str
    items: list[tuple[str, int]]
    total_cost: int
    order_handling_service_id: str