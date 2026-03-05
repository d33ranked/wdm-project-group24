from msgspec import Struct

class UserValue(Struct):
    credit: int

class PaymentRequest(Struct):
    user_id: str
    order_id: str
    amount: int


class PaymentResponseSuccess(Struct):
    order_id: str
    user_id: str
    amount_subtracted: int
    old_amount: int
    new_amount: int

class PaymentResponseFailure(Struct):
    order_id: str
    user_id: str
    amount_account: int
    msg: str
