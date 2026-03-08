

import grpc
from concurrent import futures
import transaction_pb2
import transaction_pb2_grpc
from database import get_db_conn, release_db_conn 

class PaymentParticipant(transaction_pb2_grpc.TransactionParticipantServicer):
    
    def Prepare(self, request, context):
        conn = get_db_conn()
        try:
            # --- CRITICAL FIX START ---
            conn.autocommit = False  # Ensure we are NOT in autocommit mode
            cur = conn.cursor()
            
            # 1. Lock the row (This now starts a transaction)
            cur.execute("SELECT credit FROM users WHERE user_id = %s FOR UPDATE", (request.user_id,))
            row = cur.fetchone()
            
            if not row or row[0] < request.total_cost:
                conn.rollback() 
                return transaction_pb2.PrepareResponse(success=False)

            # 2. Perform work
            cur.execute("UPDATE users SET credit = credit - %s WHERE user_id = %s", 
                        (request.total_cost, request.user_id))
            
            # 3. Hand off to PostgreSQL 2PC
            # Because autocommit is False, the UPDATE above is still 'pending'.
            # PREPARE TRANSACTION will now successfully "freeze" that pending work.
            cur.execute(f"PREPARE TRANSACTION '{request.transaction_id}'")
            # --- CRITICAL FIX END ---
            
            cur.close()
            return transaction_pb2.PrepareResponse(success=True)
        except Exception as e:
            print(f"Prepare internal error: {e}")
            try:
                conn.rollback()
            except:
                pass 
            return transaction_pb2.PrepareResponse(success=False)
        finally:
            # It's good practice to reset this before returning to the pool
            conn.autocommit = True 
            release_db_conn(conn)

    def Commit(self, request, context):
        """Phase 2: Finalize the credit deduction."""
        conn = get_db_conn()
        try:
            conn.set_isolation_level(0) 
            with conn.cursor() as cur:
                cur.execute(f"COMMIT PREPARED '{request.transaction_id}'")
            return transaction_pb2.Empty()
        except Exception as e:
            print(f"Payment Commit failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return transaction_pb2.Empty()
        finally:
            conn.set_isolation_level(1)
            release_db_conn(conn)

    def Rollback(self, request, context):
        conn = get_db_conn()
        try:
            conn.set_isolation_level(0)
            with conn.cursor() as cur:
                # CHECK if it exists first
                cur.execute("SELECT gid FROM pg_prepared_xacts WHERE gid = %s", (request.transaction_id,))
                if cur.fetchone():
                    cur.execute(f"ROLLBACK PREPARED '{request.transaction_id}'")
                    print(f"Rolled back {request.transaction_id}")
                else:
                    print(f"Transaction {request.transaction_id} was never prepared. Skipping.")
            return transaction_pb2.Empty()
        except Exception as e:
            print(f"Rollback error: {e}")
            return transaction_pb2.Empty() # Don't return an error to the coordinator
        finally:
            conn.set_isolation_level(1)
            release_db_conn(conn)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_pb2_grpc.add_TransactionParticipantServicer_to_server(PaymentParticipant(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()