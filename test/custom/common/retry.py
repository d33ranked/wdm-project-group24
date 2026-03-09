"""
Retry tests: normal checkout succeeds; business failure (4xx) is not retried.
Run with gateway and services up (e.g. docker-compose up).
"""
import requests
import os
import sys

GATEWAY = os.environ.get("BASE_URL", "http://localhost:8000")

PASS = 0
FAIL = 0


def ok(desc: str, condition: bool, fail_msg: str = ""):
    global PASS, FAIL
    if condition:
        print(f"  [PASS] {desc}")
        PASS += 1
        return True
    else:
        print(f"  [FAIL] {desc}" + (f" ({fail_msg})" if fail_msg else ""))
        FAIL += 1
        return False


def main():
    global PASS, FAIL
    print("=== Retry tests ===")
    print(f"Base URL: {GATEWAY}")
    print("")

    # --- Test 1: Normal checkout (retries should not break success) ---
    print("  Setup: user, 100 credit; item, 5 stock; order with 2 items (cost 20)")
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/100")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/5")
    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/2")

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    stock_after = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit_after = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]

    print(f"  Checkout status: {r.status_code}; stock={stock_after}, credit={credit_after}")
    print("")

    ok("checkout returns 200", r.status_code == 200, f"got {r.status_code}")
    ok("stock reduced by 2 (3 left)", stock_after == 3, f"expected 3, got {stock_after}")
    ok("credit reduced by 20 (80 left)", credit_after == 80, f"expected 80, got {credit_after}")
    print("")

    # --- Test 2: Business failure (4xx) — should not retry, stock unchanged ---
    print("  Setup: user 10 credit; item price 999; order 1 item (cost 999)")
    user2 = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user2}/10")
    item2 = requests.post(f"{GATEWAY}/stock/item/create/999").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item2}/5")
    order2 = requests.post(f"{GATEWAY}/orders/create/{user2}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order2}/{item2}/1")

    r2 = requests.post(f"{GATEWAY}/orders/checkout/{order2}")
    stock_after2 = requests.get(f"{GATEWAY}/stock/find/{item2}").json()["stock"]

    print(f"  Checkout status: {r2.status_code}; stock unchanged: {stock_after2}")
    print("")

    ok("checkout returns 400 (insufficient credit)", r2.status_code == 400, f"got {r2.status_code}")
    ok("stock unchanged at 5 (no partial deduct)", stock_after2 == 5, f"expected 5, got {stock_after2}")
    print("")

    print("=== Summary ===")
    print(f"  Passed: {PASS}  Failed: {FAIL}")
    if FAIL == 0:
        print("  Result:  Retry tests passed.")
        return 0
    else:
        print("  Result:  Retry tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
