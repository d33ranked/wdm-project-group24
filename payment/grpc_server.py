import logging
import os
from concurrent import futures

import grpc
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE

import transaction_pb2
import transaction_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _status_name(conn) -> str:
    status = conn.get_transaction_status()
    status_map = {
        0: "IDLE",
        1: "ACTIVE",
        2: "INTRANS",
        3: "INERROR",
        4: "UNKNOWN",
    }
    return status_map.get(status, f"STATUS_{status}")


def _connect_db():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        port=os.environ.get("POSTGRES_PORT", 5432),
    )


def _log_state(conn, phase: str, tx_id: str):
    logger.info(
        "[payment][%s] tx=%s pid=%s autocommit=%s tx_status=%s",
        phase,
        tx_id,
        conn.get_backend_pid(),
        conn.autocommit,
        _status_name(conn),
    )


class PaymentParticipant(transaction_pb2_grpc.TransactionParticipantServicer):
    def Prepare(self, request, context):
        conn = _connect_db()
        tx_id = request.transaction_id
        try:
            _log_state(conn, "prepare:start", tx_id)
            if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                logger.warning("[payment][prepare] tx=%s cleaning non-idle state before prepare", tx_id)
                conn.rollback()
                _log_state(conn, "prepare:after-clean", tx_id)

            conn.autocommit = False
            _log_state(conn, "prepare:autocommit-false", tx_id)

            with conn.cursor() as cur:
                logger.info(
                    "[payment][prepare] tx=%s lock/check user=%s total_cost=%s",
                    tx_id,
                    request.user_id,
                    request.total_cost,
                )
                cur.execute("SELECT credit FROM users WHERE user_id = %s FOR UPDATE", (request.user_id,))
                row = cur.fetchone()

                if not row or row[0] < request.total_cost:
                    logger.warning(
                        "[payment][prepare] tx=%s insufficient funds user=%s credit=%s need=%s",
                        tx_id,
                        request.user_id,
                        row[0] if row else None,
                        request.total_cost,
                    )
                    conn.rollback()
                    _log_state(conn, "prepare:rollback-insufficient", tx_id)
                    return transaction_pb2.PrepareResponse(success=False, message="Insufficient funds")

                cur.execute(
                    "UPDATE users SET credit = credit - %s WHERE user_id = %s",
                    (request.total_cost, request.user_id),
                )
                cur.execute("PREPARE TRANSACTION %s", (tx_id,))

            _log_state(conn, "prepare:prepared", tx_id)
            return transaction_pb2.PrepareResponse(success=True, message="Prepared")
        except Exception as exc:
            logger.exception("Payment prepare failed tx=%s: %s", tx_id, exc)
            try:
                if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                    conn.rollback()
                    _log_state(conn, "prepare:rollback-exception", tx_id)
            except Exception:
                logger.exception("[payment][prepare] tx=%s rollback after exception also failed", tx_id)
            return transaction_pb2.PrepareResponse(success=False, message="Prepare failed")
        finally:
            conn.close()

    def Commit(self, request, context):
        conn = _connect_db()
        tx_id = request.transaction_id
        try:
            _log_state(conn, "commit:start", tx_id)
            if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                conn.rollback()
                _log_state(conn, "commit:after-clean", tx_id)

            conn.autocommit = True
            _log_state(conn, "commit:autocommit-true", tx_id)

            with conn.cursor() as cur:
                cur.execute("COMMIT PREPARED %s", (tx_id,))
            _log_state(conn, "commit:done", tx_id)
            return transaction_pb2.Empty()
        except Exception as exc:
            logger.exception("Payment commit failed tx=%s: %s", tx_id, exc)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Payment commit failed")
            return transaction_pb2.Empty()
        finally:
            conn.close()

    def Rollback(self, request, context):
        conn = _connect_db()
        tx_id = request.transaction_id
        try:
            _log_state(conn, "rollback:start", tx_id)
            if conn.get_transaction_status() != TRANSACTION_STATUS_IDLE:
                conn.rollback()
                _log_state(conn, "rollback:after-clean", tx_id)

            conn.autocommit = True
            _log_state(conn, "rollback:autocommit-true", tx_id)

            with conn.cursor() as cur:
                cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s", (tx_id,))
                exists = cur.fetchone() is not None
                logger.info("[payment][rollback] tx=%s prepared_exists=%s", tx_id, exists)
                if exists:
                    cur.execute("ROLLBACK PREPARED %s", (tx_id,))
            _log_state(conn, "rollback:done", tx_id)
            return transaction_pb2.Empty()
        except Exception as exc:
            logger.exception("Payment rollback failed tx=%s: %s", tx_id, exc)
            return transaction_pb2.Empty()
        finally:
            conn.close()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_pb2_grpc.add_TransactionParticipantServicer_to_server(PaymentParticipant(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    logger.info("Payment gRPC participant listening on :50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
