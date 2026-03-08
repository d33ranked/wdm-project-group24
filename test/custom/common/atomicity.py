"""
Atomicity test: concurrent stock subtract with FOR UPDATE.
Expect exactly 5 successes (stock=5) and final stock=0; no oversell.
Run with gateway and services up (e.g. docker-compose up).
"""
import requests
import concurrent.futures
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
    print("=== Atomicity (concurrent stock subtract) ===")
    print(f"Base URL: {GATEWAY}")
    print("")

    # Setup: item with exactly 5 stock
    item_resp = requests.post(f"{GATEWAY}/stock/item/create/10")
    item_id = item_resp.json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item_id}/5")
    stock_before = requests.get(f"{GATEWAY}/stock/find/{item_id}").json()["stock"]
    print(f"  Setup: item={item_id}, stock={stock_before} (10 concurrent subtract-1 requests)")
    print("")

    # Fire 10 concurrent subtract-1 requests
    def subtract_one(_):
        return requests.post(f"{GATEWAY}/stock/subtract/{item_id}/1").status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(subtract_one, range(10)))

    successes = results.count(200)
    non_200 = [c for c in results if c != 200]
    final_stock = requests.get(f"{GATEWAY}/stock/find/{item_id}").json()["stock"]

    print(f"  Results: {successes} succeeded (200), {len(non_200)} failed (statuses: {non_200 or 'none'})")
    print(f"  Final stock: {final_stock}")
    print("")

    ok(
        "exactly 5 subtracts succeed (stock was 5)",
        successes == 5,
        f"expected 5, got {successes}",
    )
    ok(
        "final stock is 0 (no oversell)",
        final_stock == 0,
        f"expected 0, got {final_stock}",
    )
    if non_200:
        ok(
            "rejected requests are 4xx",
            all(400 <= c < 500 for c in non_200),
            f"got {non_200}",
        )
    print("")
    print("=== Summary ===")
    print(f"  Passed: {PASS}  Failed: {FAIL}")
    if FAIL == 0:
        print("  Result:  Atomicity test passed (PostgreSQL FOR UPDATE prevents oversell).")
        return 0
    else:
        print("  Result:  Atomicity test failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
