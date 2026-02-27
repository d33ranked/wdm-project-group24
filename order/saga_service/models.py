from msgspec import Struct

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

class StockRequest(Struct):
    item_id: str
    quantity: int

class BatchStockRequest(Struct):
    order_id: str
    items: list[StockRequest]

    def from_order_value(order_id: str, order_value: OrderValue) -> 'BatchStockRequest':
        items: list[StockRequest] = []
        for (item_id, quantity) in order_value.items:
            items.append(StockRequest(item_id=item_id, quantity=quantity))

        return BatchStockRequest(order_id=order_id, items=items)

class PaymentRequest(Struct):
    user_id: str
    amount: int

    def from_order_value(order_value: OrderValue) -> 'PaymentRequest':
        return PaymentRequest(user_id=order_value.user_id, amount=order_value.total_cost)