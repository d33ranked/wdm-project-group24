"""
Phase 1 — Redis Storage Tests
==============================
Mode-agnostic tests that specifically exercise behaviours introduced by
the PostgreSQL → Redis migration.  They run in both TPC and SAGA modes.

What is covered here that test_common.py does NOT already cover
---------------------------------------------------------------
1. Batch init — integer-keyed items/users created by /batch_init endpoints.
2. Add-stock idempotency — same Idempotency-Key header → stock unchanged on
   retry.
3. Pay idempotency — same Idempotency-Key header → credit unchanged on retry.
4. Order item quantity merge — addItem same item_id twice adds quantities,
   not a second entry.
5. Zero / negative amount guard — add_stock and add_funds reject amount ≤ 0.
6. Item not found — subtract from a nonexistent UUID returns 4xx, not 5xx.
7. User not found — pay from a nonexistent UUID returns 4xx, not 5xx.
"""

import uuid

from run import api, check, json_field


# ---------------------------------------------------------------------------
# 1. Batch Init — Integer-Keyed Items and Users
# ---------------------------------------------------------------------------
def test_batch_init_stock_and_payment():
    """POST /batch_init creates items and users reachable by integer key."""
    N            = 5
    START_STOCK  = 50
    ITEM_PRICE   = 10
    START_MONEY  = 200

    r_stock = api("POST", f"/stock/batch_init/{N}/{START_STOCK}/{ITEM_PRICE}")
    check("Batch Init Stock Returns 200",
          r_stock.status_code == 200, f"got {r_stock.status_code}")

    r_pay = api("POST", f"/payment/batch_init/{N}/{START_MONEY}")
    check("Batch Init Payment Returns 200",
          r_pay.status_code == 200, f"got {r_pay.status_code}")

    # Verify items are reachable by integer key (key schema: item:{i})
    for i in range(N):
        data = api("GET", f"/stock/find/{i}")
        check(f"Batch-Init Item {i} Is Readable After Batch Init",
              data.status_code == 200, f"got {data.status_code}")
        s = json_field(data, "stock")
        p = json_field(data, "price")
        check(f"Item {i} Has Stock={START_STOCK}",  s == START_STOCK, f"got {s}")
        check(f"Item {i} Has Price={ITEM_PRICE}", p == ITEM_PRICE,  f"got {p}")

    # Verify users are reachable by integer key (key schema: user:{i})
    for i in range(N):
        data = api("GET", f"/payment/find_user/{i}")
        check(f"Batch-Init User {i} Is Readable After Batch Init",
              data.status_code == 200, f"got {data.status_code}")
        c = json_field(data, "credit")
        check(f"User {i} Has Credit={START_MONEY}", c == START_MONEY, f"got {c}")


# ---------------------------------------------------------------------------
# 2. Add-Stock Idempotency — Same Idempotency-Key Does Not Double-Add
# ---------------------------------------------------------------------------
def test_add_stock_idempotency():
    """Sending the same add_stock request twice with one Idempotency-Key is a no-op.

    Phase 1 change: idempotency is now stored in Redis hashes (idem:{key})
    with a 1-hour TTL.  This verifies the hash write + TTL path is correct.
    """
    INITIAL = 10
    ADD_AMT  = 5
    IDEM_KEY = str(uuid.uuid4())

    item = json_field(api("POST", "/stock/item/create/1"), "item_id")
    api("POST", f"/stock/add/{item}/{INITIAL}")

    headers = {"Idempotency-Key": IDEM_KEY}

    r1 = api("POST", f"/stock/add/{item}/{ADD_AMT}", headers=headers)
    check("First Add-Stock Request With Idempotency-Key Succeeds",
          r1.status_code == 200, f"got {r1.status_code}")

    r2 = api("POST", f"/stock/add/{item}/{ADD_AMT}", headers=headers)
    check("Second Add-Stock Request With Same Key Returns 200 (Cached)",
          r2.status_code == 200, f"got {r2.status_code}")

    stock = json_field(api("GET", f"/stock/find/{item}"), "stock")
    expected = INITIAL + ADD_AMT   # added only ONCE
    check(f"Stock Is {expected} — Idempotency Key Prevented The Second Add",
          stock == expected, f"got {stock}")


# ---------------------------------------------------------------------------
# 3. Pay Idempotency — Same Idempotency-Key Does Not Double-Deduct
# ---------------------------------------------------------------------------
def test_pay_idempotency():
    """Sending the same /pay request twice with one Idempotency-Key is a no-op.

    Symmetric to test_add_stock_idempotency but for the payment service's
    deduct_credit Lua script.
    """
    INITIAL   = 100
    PAY_AMT   = 30
    IDEM_KEY  = str(uuid.uuid4())

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{INITIAL}")

    headers = {"Idempotency-Key": IDEM_KEY}

    r1 = api("POST", f"/payment/pay/{user}/{PAY_AMT}", headers=headers)
    check("First Pay Request With Idempotency-Key Succeeds",
          r1.status_code == 200, f"got {r1.status_code}")

    r2 = api("POST", f"/payment/pay/{user}/{PAY_AMT}", headers=headers)
    check("Second Pay Request With Same Key Returns 200 (Cached)",
          r2.status_code == 200, f"got {r2.status_code}")

    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    expected = INITIAL - PAY_AMT   # deducted only ONCE
    check(f"Credit Is {expected} — Idempotency Key Prevented The Second Deduction",
          credit == expected, f"got {credit}")


# ---------------------------------------------------------------------------
# 4. Order Item Quantity Merge — Same Item Added Twice Merges Quantities
# ---------------------------------------------------------------------------
def test_order_item_quantity_merge():
    """Adding the same item to an order twice merges quantities, not double-lists.

    Redis stores items as a JSON list of [item_id, qty] pairs inside
    order:{order_id}.  The merge logic must locate the existing entry and
    increment its quantity rather than appending a new one.
    """
    PRICE = 10
    STOCK = 20
    QTY_1 = 3
    QTY_2 = 4
    CREDIT = 1000

    user = json_field(api("POST", "/payment/create_user"), "user_id")
    api("POST", f"/payment/add_funds/{user}/{CREDIT}")
    item = json_field(api("POST", f"/stock/item/create/{PRICE}"), "item_id")
    api("POST", f"/stock/add/{item}/{STOCK}")

    order = json_field(api("POST", f"/orders/create/{user}"), "order_id")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY_1}")
    api("POST", f"/orders/addItem/{order}/{item}/{QTY_2}")

    order_data = api("GET", f"/orders/find/{order}").json()
    items_list = order_data.get("items", [])
    total_cost = order_data.get("total_cost", 0)

    check("Order Items List Has Exactly One Entry After Adding Same Item Twice",
          len(items_list) == 1, f"got {len(items_list)} entries: {items_list}")

    merged_qty = items_list[0][1] if items_list else 0
    expected_qty = QTY_1 + QTY_2
    check(f"Merged Quantity Is {expected_qty} (= {QTY_1} + {QTY_2})",
          merged_qty == expected_qty, f"got {merged_qty}")

    expected_cost = PRICE * expected_qty
    check(f"Total Cost Reflects Merged Quantity: {expected_cost}",
          total_cost == expected_cost, f"got {total_cost}")


# ---------------------------------------------------------------------------
# 5. Zero / Negative Amount Rejected
# ---------------------------------------------------------------------------
def test_zero_amount_rejected():
    """add_stock and add_funds reject amount ≤ 0 with 4xx.

    This is a business-rule guard enforced by the HTTP endpoint and also by
    the stream handler.  Both must be correct post-migration.
    """
    item = json_field(api("POST", "/stock/item/create/5"), "item_id")
    user = json_field(api("POST", "/payment/create_user"), "user_id")

    for amt in ("0", "-1", "-100"):
        r_s = api("POST", f"/stock/add/{item}/{amt}")
        check(f"Add Stock With Amount={amt} Returns 4xx",
              400 <= r_s.status_code < 500, f"got {r_s.status_code}")

        r_p = api("POST", f"/payment/add_funds/{user}/{amt}")
        check(f"Add Funds With Amount={amt} Returns 4xx",
              400 <= r_p.status_code < 500, f"got {r_p.status_code}")

    # Confirm no phantom stock or credit was created
    stock  = json_field(api("GET", f"/stock/find/{item}"), "stock")
    credit = json_field(api("GET", f"/payment/find_user/{user}"), "credit")
    check("Stock Remains 0 After All Rejected Adds", stock == 0, f"got {stock}")
    check("Credit Remains 0 After All Rejected Adds", credit == 0, f"got {credit}")


# ---------------------------------------------------------------------------
# 6. Item Not Found — subtract returns 4xx, not 5xx
# ---------------------------------------------------------------------------
def test_item_not_found_subtract():
    """Subtracting from a nonexistent item UUID returns 4xx, not 500.

    Validates that the deduct_stock_batch Lua script's NOT_FOUND sentinel
    is surfaced cleanly at the HTTP layer after the Redis migration.
    """
    ghost = str(uuid.uuid4())

    r_sub = api("POST", f"/stock/subtract/{ghost}/1")
    check("Subtract From Nonexistent Item Returns 4xx (Not 500)",
          400 <= r_sub.status_code < 500, f"got {r_sub.status_code}")

    r_find = api("GET", f"/stock/find/{ghost}")
    check("Find Nonexistent Item Returns 4xx",
          400 <= r_find.status_code < 500, f"got {r_find.status_code}")


# ---------------------------------------------------------------------------
# 7. User Not Found — pay returns 4xx, not 5xx
# ---------------------------------------------------------------------------
def test_user_not_found_pay():
    """Paying from a nonexistent user UUID returns 4xx, not 500.

    Validates that the deduct_credit Lua script's NOT_FOUND sentinel is
    surfaced cleanly at the HTTP layer after the Redis migration.
    """
    ghost = str(uuid.uuid4())

    r_pay = api("POST", f"/payment/pay/{ghost}/50")
    check("Pay From Nonexistent User Returns 4xx (Not 500)",
          400 <= r_pay.status_code < 500, f"got {r_pay.status_code}")

    r_find = api("GET", f"/payment/find_user/{ghost}")
    check("Find Nonexistent User Returns 4xx",
          400 <= r_find.status_code < 500, f"got {r_find.status_code}")


# ---------------------------------------------------------------------------
# Ordered test list — imported by run.py
# ---------------------------------------------------------------------------
TESTS = [
    ("Batch Init: Integer-Keyed Items And Users Are Readable", test_batch_init_stock_and_payment),
    ("Add-Stock Idempotency: Same Key Does Not Double-Add", test_add_stock_idempotency),
    ("Pay Idempotency: Same Key Does Not Double-Deduct", test_pay_idempotency),
    ("Order Item Merge: Same Item Added Twice Merges Quantities", test_order_item_quantity_merge),
    ("Zero/Negative Amount Rejected By Stock And Payment", test_zero_amount_rejected),
    ("Item Not Found: Subtract Returns 4xx Not 5xx", test_item_not_found_subtract),
    ("User Not Found: Pay Returns 4xx Not 5xx", test_user_not_found_pay),
]
