import requests
import time

ORDER_URL = STOCK_URL = PAYMENT_URL = "http://127.0.0.1:8000"


def wait_for_service(url: str, timeout: float = 30.0, interval: float = 1.0) -> bool:
    """Wait for a service to become ready by polling /ready endpoint."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/ready", timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ready":
                    return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(interval)

    print("Couldnt reach service at {url} within {timeout} seconds")
    return False


def wait_for_all_services(timeout: float = 30.0) -> bool:
    """Wait for all microservices to become ready."""
    services = [
        ("orders", ORDER_URL),
        ("stock", STOCK_URL),
        ("payment", PAYMENT_URL),
    ]

    all_ready = True
    for name, url in services:
        print(f"Waiting for {name} service to be ready...")
        if not wait_for_service(f"{url}/{name}", timeout):
            print(f"{name} service failed to become ready within {timeout}s")
            all_ready = False
        else:
            print(f"{name} service is ready")

    return all_ready


########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> tuple[int, dict]:
    response = requests.post(f"{STOCK_URL}/stock/item/create/{price}")
    return response.status_code, (response.json() if status_code_is_success(response.status_code) else None)

def find_item(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}").json()


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}").status_code


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}").status_code


########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}").status_code


def create_user() -> tuple[int, dict]:
    response = requests.post(f"{PAYMENT_URL}/payment/create_user")
    print(f"Create user response: {response.status_code}, {response.text}")
    return response.status_code, (response.json() if status_code_is_success(response.status_code) else None)


def find_user(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}").json()


def add_credit_to_user(user_id: str, amount: float) -> int:
    return requests.post(
        f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}"
    ).status_code


########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> dict:
    return requests.post(f"{ORDER_URL}/orders/create/{user_id}").json()


def add_item_to_order(order_id: str, item_id: str, quantity: int) -> int:
    response = requests.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}")
    print(f"Add item to order response: {response.status_code}, {response.text}")
    return response.status_code


def find_order(order_id: str) -> dict:
    return requests.get(f"{ORDER_URL}/orders/find/{order_id}").json()


def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{ORDER_URL}/orders/checkout/{order_id}")


########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    return 200 <= status_code < 300


def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500
