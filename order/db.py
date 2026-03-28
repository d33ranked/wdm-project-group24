import json


def get_order(r, order_id: str) -> dict:
    data = r.hgetall(f"order:{order_id}")
    if not data:
        raise ValueError(f"Order {order_id} not found")
    return {
        "paid": data["paid"] == "true",
        "items": json.loads(data.get("items", "[]")),
        "user_id": data["user_id"],
        "total_cost": int(data.get("total_cost", 0)),
    }


def get_order_for_update(r, order_id: str) -> dict:
    return get_order(r, order_id)


def mark_paid(r, order_id: str):
    r.hset(f"order:{order_id}", "paid", "true")
