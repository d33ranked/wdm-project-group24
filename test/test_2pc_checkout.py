import unittest

import utils as tu


class TestMicroservices(unittest.TestCase):

    def test_2pc_rollback_when_payment_votes_no_after_stock_prepare(self):
        user: dict = tu.create_user()
        self.assertIn('user_id', user)
        user_id: str = user['user_id']

        item: dict = tu.create_item(10)
        self.assertIn('item_id', item)
        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 5)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)
        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 2)  # total = 20
        self.assertTrue(tu.status_code_is_success(add_item_response))

        # Keep funds below total cost so payment votes NO.
        add_credit_response = tu.add_credit_to_user(user_id, 5)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        stock_before: int = tu.find_item(item_id)['stock']
        credit_before: int = tu.find_user(user_id)['credit']
        order_before: dict = tu.find_order(order_id)
        self.assertFalse(order_before['paid'])

        checkout_response = tu.safe_checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        stock_after: int = tu.find_item(item_id)['stock']
        credit_after: int = tu.find_user(user_id)['credit']
        order_after: dict = tu.find_order(order_id)

        self.assertEqual(stock_after, stock_before)
        self.assertEqual(credit_after, credit_before)
        self.assertFalse(order_after['paid'])

    def test_2pc_rollback_when_stock_votes_no_for_unavailable_item(self):
        user: dict = tu.create_user()
        self.assertIn('user_id', user)
        user_id: str = user['user_id']

        # Give enough money so payment can prepare if stock prepares.
        add_credit_response = tu.add_credit_to_user(user_id, 100)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)
        order_id: str = order['order_id']

        # Create a real item so addItem succeeds, but keep stock lower than ordered quantity.
        item: dict = tu.create_item(10)
        self.assertIn('item_id', item)
        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 2)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        stock_before: int = tu.find_item(item_id)['stock']
        credit_before: int = tu.find_user(user_id)['credit']
        order_before: dict = tu.find_order(order_id)
        self.assertFalse(order_before['paid'])

        checkout_response = tu.safe_checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        stock_after: int = tu.find_item(item_id)['stock']
        credit_after: int = tu.find_user(user_id)['credit']
        order_after: dict = tu.find_order(order_id)

        # Stock, payment, and order state must be unchanged after global rollback.
        self.assertEqual(stock_after, stock_before)
        self.assertEqual(credit_after, credit_before)
        self.assertFalse(order_after['paid'])

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.safe_checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.safe_checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.safe_checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)


if __name__ == '__main__':
    unittest.main()

