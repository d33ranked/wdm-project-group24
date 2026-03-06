from msgspec import Struct

class OrderValue(Struct):
    paid: bool
    items: dict[str, int]
    user_id: str
    total_cost: int

class ItemRequest(Struct): 
    item_id: str
    quantity: int

class BatchItemRequest(Struct):
    order_id: str
    items: list[ItemRequest]

    def from_order_value(order_id: str, order_value: OrderValue) -> 'BatchItemRequest':
        items: list[ItemRequest] = []
        for (item_id, quantity) in order_value.items.items():
            items.append(ItemRequest(item_id=item_id, quantity=quantity))

        return BatchItemRequest(order_id=order_id, items=items)

class PaymentRequest(Struct):
    user_id: str
    order_id: str
    amount: int

    def from_order_value(order_id: str, order_value: OrderValue) -> 'PaymentRequest':
        return PaymentRequest(user_id=order_value.user_id, order_id=order_id, amount=order_value.total_cost)

class OrderCheckoutStatus(Struct):
    checkout_id: str
    order_id: str
    total_cost: int
    payment_success: bool = False
    stock_success: bool = False