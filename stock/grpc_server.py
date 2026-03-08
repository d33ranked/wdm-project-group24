import grpc
from concurrent import futures
import transaction_pb2
import transaction_pb2_grpc
from database import get_db_conn, release_db_conn # Use the pool helpers
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockParticipant(transaction_pb2_grpc.TransactionParticipantServicer):
    
    def Prepare(self, request, context):
        conn = get_db_conn()
        try:
            # 1. FORCE MANUAL TRANSACTION MODE
            conn.autocommit = False 
            cur = conn.cursor()
            
            # ... your SELECT and UPDATE logic ...

            # 2. CHECK THE ROW COUNT EXPLICITLY
            if row is None:
                logger.warning("ITEM NOT FOUND")
                conn.rollback() # Clean up
                return transaction_pb2.PrepareResponse(success=False)

            # 3. PREPARE THE TRANSACTION
            cur.execute(f"PREPARE TRANSACTION '{request.transaction_id}'")
            
            # 4. DO NOT COMMIT OR ROLLBACK HERE
            # Postgres holds the 'Prepared' state globally once PREPARE is called.
            
            cur.close()
            return transaction_pb2.PrepareResponse(success=True)
        except Exception as e:
            conn.rollback()
            return transaction_pb2.PrepareResponse(success=False)
        finally:
            release_db_conn(conn)

    def Commit(self, request, context):
        conn = get_db_conn()
        try:
            # COMMIT PREPARED must run outside a transaction block
            conn.set_isolation_level(0) 
            with conn.cursor() as cur:
                cur.execute(f"COMMIT PREPARED '{request.transaction_id}'")
            return transaction_pb2.Empty()
        except Exception as e:
            print(f"Stock Commit failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return transaction_pb2.Empty()
        finally:
            conn.set_isolation_level(1) # Reset to default
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

# In stock/grpc_server.py
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_pb2_grpc.add_TransactionParticipantServicer_to_server(StockParticipant(), server)
    server.add_insecure_port('[::]:50052') # Change this to 50052
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()