"""Payment 2PC participant — prepare/commit/abort via Redis Lua scripts.

Key schema
----------
user:{user_id}               Hash  { credit }
prepared:payment:{txn_id}   Hash  { user_id, amount }  TTL 600s

How prepare works
-----------------
prepare_payment Lua script (atomic):
  1. If prepared:payment:{txn_id} already exists → return 0 (idempotent)
  2. Check user exists and has sufficient credit → error if not
  3. Deduct credit from user
  4. Record {user_id, amount} in the reservation hash
  5. Set 600s TTL on the reservation hash (coordinator-crash safety net)

Commit: just deletes the reservation hash (credit was already deducted at prepare time).
Abort:  reads reservation hash, restores credit to user, deletes hash.

Recovery
--------
The order service (coordinator) scans its own txn:* keys on startup and calls
commit or abort on all participants. No recovery logic is needed here.
"""

import redis as redis_lib
from flask import g, abort, Response

_redis_pool = None
_scripts = None


def init_routes(app, redis_pool, scripts):
    global _redis_pool, _scripts
    _redis_pool = redis_pool
    _scripts = scripts

    # PREPARE — credit check + deduct + record reservation (all atomic)
    @app.post("/prepare/<txn_id>/<user_id>/<amount>")
    def prepare_transaction(txn_id: str, user_id: str, amount: int):
        amount = int(amount)
        # KEYS[1] = "prepared:payment:{txn_id}"
        # KEYS[2] = "user:{user_id}"
        # ARGV[1] = amount
        # ARGV[2] = user_id (stored in reservation for abort recovery)
        try:
            _scripts.prepare_payment(
                keys=[f"prepared:payment:{txn_id}", f"user:{user_id}"],
                args=[amount, user_id],
                client=g.redis,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                abort(400, f"User: {user_id} not found!")
            if "INSUFFICIENT_CREDIT" in err:
                abort(400, f"User: {user_id} has insufficient credit!")
            raise
        # result 0 = already prepared (idempotent), result >= 0 = new credit balance
        return Response("Transaction prepared", status=200)

    # COMMIT — credit was already deducted at prepare time; just delete reservation
    @app.post("/commit/<txn_id>")
    def commit_transaction(txn_id: str):
        _scripts.commit_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction committed", status=200)

    # ABORT — read reservation, restore credit to user, delete reservation
    @app.post("/abort/<txn_id>")
    def abort_transaction(txn_id: str):
        _scripts.abort_payment(keys=[f"prepared:payment:{txn_id}"], client=g.redis)
        return Response("Transaction aborted", status=200)


def recovery(redis_pool, scripts):
    # coordinator-driven recovery: order service scans its txn:* keys on startup
    # and explicitly calls commit/abort on all participants including this one
    # prepared:payment:{txn_id} keys carry a 600s TTL as a last-resort safety net
    print("RECOVERY PAYMENT: coordinator-driven — no participant-side action needed", flush=True)
