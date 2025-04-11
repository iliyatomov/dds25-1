from msgspec import Struct

class OrderCancelledEvent(Struct):
    order_id: str
    checkout_id: str
    user_id: str
    order_handling_service_id: str