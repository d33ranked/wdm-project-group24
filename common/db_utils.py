"""
Database utilities for high-availability PostgreSQL with Patroni.

Key features:
- Automatic primary discovery via Patroni REST API.
- Periodic leader polling (every 1 second) via background thread.
- WrappedCursor: detects when failover occurs (leader change detected by poller)
  and raises FailoverDetected to trigger message replay with fresh connection.
- HAConnectionPool: manages connection pool and coordinates leader discovery
  with per-service background poller threads.
"""

import os
import time
import threading
import logging
import requests
import psycopg2
import psycopg2.pool
import psycopg2.errors

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sentinel exception – raised when failover is detected.
# ---------------------------------------------------------------------------

class FailoverDetected(Exception):
    """Raised when leader change is detected by the periodic poller."""

# ---------------------------------------------------------------------------
# WrappedCursor
# ---------------------------------------------------------------------------

class WrappedCursor:
    """
    Wraps a psycopg2 cursor and checks for failover on execute/fetch operations.
    If a FailoverDetected event has been set by the pool's background poller,
    raises FailoverDetected to trigger message replay with a fresh connection.
    """

    def __init__(self, cursor, pool: "HAConnectionPool"):
        self._cur = cursor
        self._pool = pool

    def execute(self, sql, params=None):
        """Execute a query, checking for failover before and after."""
        if self._pool._failover_detected.is_set():
            raise FailoverDetected("Leader change detected")
        if params is not None:
            return self._cur.execute(sql, params)
        return self._cur.execute(sql)

    def fetchone(self):
        if self._pool._failover_detected.is_set():
            raise FailoverDetected("Leader change detected")
        return self._cur.fetchone()

    def fetchall(self):
        if self._pool._failover_detected.is_set():
            raise FailoverDetected("Leader change detected")
        return self._cur.fetchall()

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    # Delegate everything else directly
    def __getattr__(self, name):
        return getattr(self._cur, name)



# ---------------------------------------------------------------------------
# HAConnectionPool
# ---------------------------------------------------------------------------

class HAConnectionPool:
    """
    High-availability connection pool that automatically discovers the primary
    PostgreSQL node via Patroni REST API.

    A background thread polls the primary every 1 second and sets _failover_detected
    when the leader changes. WrappedCursor checks this flag on execute/fetch to
    raise FailoverDetected, triggering message replay.
    """

    def __init__(
        self,
        hosts: list[str],
        port: int,
        dbname: str,
        user: str,
        password: str,
        patroni_port: int = 8008,
        minconn: int = 10,
        maxconn: int = 100,
        service_name: str = "SERVICE",
    ):
        self.hosts = hosts
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.patroni_port = patroni_port
        self.minconn = minconn
        self.maxconn = maxconn
        self.service_name = service_name

        self._pool = None
        self._current_primary = None
        self._lock = threading.Lock()

        # Event set when leader changes
        self._failover_detected = threading.Event()

        # Initial connection
        self._create_pool()

        # Start background poller thread
        self._poller_stop_event = threading.Event()
        poller_thread = threading.Thread(
            target=self._poll_leader_loop,
            daemon=True,
            name=f"{service_name}-leader-poller",
        )
        poller_thread.start()

    # ------------------------------------------------------------------
    # Primary discovery – probes Patroni endpoints
    # ------------------------------------------------------------------

    def _get_primary_host(self) -> str:
        """
        Probe all hosts trying /primary first, fall back to /leader.
        Returns the first host that responds 200.
        """
        for endpoint in ["primary", "leader"]:
            for host in self.hosts:
                try:
                    resp = requests.get(
                        f"http://{host}:{self.patroni_port}/{endpoint}",
                        timeout=1,
                    )
                    if resp.status_code == 200:
                        return host
                except requests.exceptions.RequestException:
                    pass
        raise Exception(f"{self.service_name}: No primary found among hosts: {self.hosts}")

    # ------------------------------------------------------------------
    # Pool creation
    # ------------------------------------------------------------------

    def _create_pool(self, retries: int = 30, delay: float = 1.0):
        """
        Create a new connection pool connected to the current primary.
        Retries until successful or max retries exceeded.
        """
        for attempt in range(retries):
            try:
                primary_host = self._get_primary_host()
                new_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.minconn,
                    maxconn=self.maxconn,
                    host=primary_host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                )

                # Close old pool if it exists
                if self._pool is not None:
                    try:
                        self._pool.closeall()
                    except Exception:
                        pass

                self._pool = new_pool
                self._current_primary = primary_host
                logger.info("%s: Connected to primary: %s", self.service_name, primary_host)
                return

            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(
                        "%s: Failed to connect (attempt %d/%d): %s",
                        self.service_name, attempt + 1, retries, e
                    )
                    time.sleep(delay)
                else:
                    logger.critical(
                        "%s: Failed to create connection pool after %d attempts",
                        self.service_name, retries
                    )
                    raise Exception(
                        f"{self.service_name}: Failed to create connection pool after {retries} attempts"
                    )

    # ------------------------------------------------------------------
    # Background leader poller — runs every 1 second
    # ------------------------------------------------------------------

    def _poll_leader_loop(self):
        """
        Background thread: poll for primary every 1 second.
        If leader changes, set _failover_detected and rebuild pool.
        """
        logger.info("%s: Leader poller started", self.service_name)
        while not self._poller_stop_event.is_set():
            try:
                new_primary = self._get_primary_host()
                if new_primary != self._current_primary:
                    logger.warning(
                        "%s: Leader changed from %s to %s",
                        self.service_name, self._current_primary, new_primary
                    )
                    # Set failover flag so active transactions raise FailoverDetected
                    self._failover_detected.set()
                    # Rebuild pool on new primary
                    self._create_pool(retries=10, delay=0.5)
                    # Clear the flag so next transactions can proceed normally
                    self._failover_detected.clear()
                    logger.info("%s: Failover complete, pool rebuilt", self.service_name)
            except Exception as e:
                logger.debug("%s: Leader poll failed: %s", self.service_name, e)

            # Sleep 1 second before next poll
            self._poller_stop_event.wait(timeout=1.0)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def getconn(self):
        """
        Get a connection from the pool.
        Runs a SELECT 1 probe to ensure it's healthy.
        """
        # Wait for a connection to become available if pool is exhausted
        for attempt in range(300):  # 30 seconds max
            try:
                with self._lock:
                    conn = self._pool.getconn()
                break
            except psycopg2.pool.PoolError:
                if attempt == 299:
                    raise
                time.sleep(0.1)

        # Probe the connection
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning("%s: Connection probe failed, failing fast: %s",
                          self.service_name, e)
            self.putconn(conn, close=True)
            raise

    def cursor(self, conn) -> WrappedCursor:
        """Return a WrappedCursor for the given connection."""
        return WrappedCursor(conn.cursor(), self)

    def putconn(self, conn, close: bool = False):
        """Return a connection to the pool."""
        try:
            self._pool.putconn(conn, close=close)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    def closeall(self):
        """Close all connections and stop poller."""
        self._poller_stop_event.set()
        if self._pool is not None:
            self._pool.closeall()

    @property
    def current_primary(self) -> str | None:
        return self._current_primary

# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_ha_pool(service_name: str = "SERVICE") -> HAConnectionPool:
    """
    Create an HA connection pool from environment variables.

    Expected environment variables:
    - POSTGRES_HOSTS: comma-separated list of host names (e.g. "order-db-1,order-db-2")
    - PATRONI_PORT:   Patroni REST API port (default: 8008)
    - POSTGRES_PORT:  PostgreSQL port (default: 5432)
    - POSTGRES_DB:    Database name
    - POSTGRES_USER:  Database user
    - POSTGRES_PASSWORD: Database password
    """
    hosts = os.environ["POSTGRES_HOSTS"].split(",")
    patroni_port = int(os.environ.get("PATRONI_PORT", 8008))
    postgres_port = int(os.environ.get("POSTGRES_PORT", 5432))
    dbname = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]

    return HAConnectionPool(
        hosts=hosts,
        port=postgres_port,
        dbname=dbname,
        user=user,
        password=password,
        patroni_port=patroni_port,
        service_name=service_name,
    )
