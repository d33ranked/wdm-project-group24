import asyncio

from db import get_order_from_db
from models import OrderValue, BatchStockRequest, PaymentRequest
from kafka_app import request_stock, request_payment, rollback_stock, rollback_payment, pending_sagas, app

async def _checkout(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    saga = {
        'stock_event': asyncio.Event(),
        'payment_event': asyncio.Event(),
        'stock_ok': False,
        'payment_ok': False,
    }
    pending_sagas[order_id] = saga

    await asyncio.gather(
        request_stock(BatchStockRequest.from_order_value(order_id, order_entry)),
        request_payment(PaymentRequest.from_order_value(order_entry)),
    )

    try:
        await asyncio.wait_for(
            asyncio.gather(
                saga['stock_event'].wait(),
                saga['payment_event'].wait(),
            ),
            timeout=30
        )
    except asyncio.TimeoutError:
        app.logger.warning(f"Saga timed out for {order_id}")
    finally:
        pending_sagas.pop(order_id, None)

    stock_ok = saga['stock_ok']
    payment_ok = saga['payment_ok']

    if not stock_ok or not payment_ok:
        app.logger.warning(f"Saga failed for {order_id}: stock_ok={stock_ok}, payment_ok={payment_ok}")
        rollbacks = []
        if stock_ok:
            rollbacks.append(rollback_stock(order_id))
        if payment_ok:
            rollbacks.append(rollback_payment(PaymentRequest.from_order_value(order_entry)))
        if rollbacks:
            await asyncio.gather(*rollbacks)
        return {"error": "Checkout failed"}, 400

    return {"status": "success"}, 200

