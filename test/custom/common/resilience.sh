#!/usr/bin/env bash
# Resilience test: checkout while stock is briefly down; retries should eventually succeed.
# Requires: docker compose, stack running, STOCK_SERVICE_CONTAINER set or default below.
# Usage: ./test/custom/common/resilience.sh

set -e

BASE_URL="${BASE_URL:-http://localhost:8000}"
STOCK_SERVICE_CONTAINER="${STOCK_SERVICE_CONTAINER:-wdm-project-group24-stock-service-1}"

PASS=0
FAIL=0

json_field() {
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$1',''))" 2>/dev/null || echo ""
}

echo "=== Resilience (checkout during brief stock outage) ==="
echo "Base URL: $BASE_URL"
echo ""

# Setup: user, item, order
echo "  Setup: user 100 credit; item 5 stock; order 2 items"
USER_ID=$(curl -s -X POST "$BASE_URL/payment/create_user" | json_field "user_id")
curl -s -o /dev/null -X POST "$BASE_URL/payment/add_funds/$USER_ID/100"
ITEM_ID=$(curl -s -X POST "$BASE_URL/stock/item/create/10" | json_field "item_id")
curl -s -o /dev/null -X POST "$BASE_URL/stock/add/$ITEM_ID/5"
ORDER_ID=$(curl -s -X POST "$BASE_URL/orders/create/$USER_ID" | json_field "order_id")
curl -s -o /dev/null -X POST "$BASE_URL/orders/addItem/$ORDER_ID/$ITEM_ID/2"
echo "  Order $ORDER_ID ready. Stopping stock, then running checkout (retries until stock is back)."
echo ""

# Stop stock so checkout will fail initially; bring it back after 3s so retries succeed
echo "  Stopping stock container for ~3s..."
docker stop "$STOCK_SERVICE_CONTAINER" > /dev/null 2>&1
# In background: wait 3s then start stock
(sleep 3; docker start "$STOCK_SERVICE_CONTAINER" > /dev/null 2>&1) &
# Run checkout now (stock is down); order service retries with backoff until stock is back
CHECKOUT_CODE=$(curl -s -o /tmp/resilience_checkout.txt -w "%{http_code}" -X POST "$BASE_URL/orders/checkout/$ORDER_ID" --max-time 30)
wait
echo "  Stock container restarted."
echo ""

STOCK_AFTER=$(curl -s "$BASE_URL/stock/find/$ITEM_ID" | json_field "stock")
CREDIT_AFTER=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")

echo "  Checkout status: $CHECKOUT_CODE; stock=$STOCK_AFTER, credit=$CREDIT_AFTER"
echo ""

if [ "$CHECKOUT_CODE" = "200" ]; then
  echo "  [PASS] checkout succeeded (200)"
  ((PASS++)) || true
else
  echo "  [FAIL] checkout expected 200 (got $CHECKOUT_CODE)"
  ((FAIL++)) || true
fi

if [ "$STOCK_AFTER" = "3" ]; then
  echo "  [PASS] stock=3 (2 deducted)"
  ((PASS++)) || true
else
  echo "  [FAIL] stock expected 3 (got $STOCK_AFTER)"
  ((FAIL++)) || true
fi

if [ "$CREDIT_AFTER" = "80" ]; then
  echo "  [PASS] credit=80 (20 deducted)"
  ((PASS++)) || true
else
  echo "  [FAIL] credit expected 80 (got $CREDIT_AFTER)"
  ((FAIL++)) || true
fi

echo ""
echo "=== Summary ==="
echo "  Passed: $PASS  Failed: $FAIL"
if [ "$FAIL" -eq 0 ]; then
  echo "  Result:  Resilience test passed (retries recovered after stock came back)."
  exit 0
else
  echo "  Result:  Resilience test failed."
  exit 1
fi
