import requests

GATEWAY = "http://localhost:8000"

def test_successful_2pc_checkout():
    """Full 2PC checkout: prepare all, commit all."""
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/200")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/3")

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 200

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert stock == 7, f"Expected 7, got {stock}"
    assert credit == 170, f"Expected 170, got {credit}"
    print("SUCCESS: 2PC checkout works")

def test_2pc_stock_vote_no():
    """Stock votes NO: all prepared participants get ABORT."""
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/200")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/2")  # only 2 in stock

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/5")  # want 5

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 400

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert stock == 2, f"Stock should be unchanged at 2, got {stock}"
    assert credit == 200, f"Credit should be unchanged at 200, got {credit}"
    print("SUCCESS: 2PC stock vote NO works, nothing changed")

def test_2pc_payment_vote_no():
    """Stock votes YES but Payment votes NO: stock gets ABORT."""
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/5")  # not enough
    item = requests.post(f"{GATEWAY}/stock/item/create/100").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/1")  # costs 100, user has 5

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 400

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert stock == 10, f"Stock should be restored to 10, got {stock}"
    assert credit == 5, f"Credit should be unchanged at 5, got {credit}"
    print("SUCCESS: 2PC payment vote NO works, stock restored")

if __name__ == "__main__":
    test_successful_2pc_checkout()
    test_2pc_stock_vote_no()
    test_2pc_payment_vote_no()
    print("\nALL 2PC TESTS PASSED")