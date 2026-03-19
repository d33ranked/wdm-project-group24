#!/usr/bin/env python3
"""
Switch transaction mode and tune system parameters, then restart.

Usage:
    python tune.py <TPC|SAGA> [-w WORKERS] [-p PARTITIONS] [-g GW_THREADS]

Examples:
    python tune.py TPC                         # TPC with defaults
    python tune.py TPC  -w 24                  # TPC, 24 workers/service
    python tune.py SAGA -w 8 -p 32 -g 64      # SAGA, tuned
"""

import argparse
import re
import subprocess
import sys
import time

COMPOSE_FILE = "docker-compose.yml"

DEFAULTS = {"workers": 1, "partitions": 3, "gw_threads": 16}


def parse_args():
    p = argparse.ArgumentParser(description="Tune and restart the system.")
    p.add_argument("mode", choices=["TPC", "SAGA", "tpc", "saga"], help="Transaction mode")
    p.add_argument("-w", "--workers", type=int, default=DEFAULTS["workers"],
                   help=f"Gunicorn workers per service (default: {DEFAULTS['workers']})")
    p.add_argument("-p", "--partitions", type=int, default=DEFAULTS["partitions"],
                   help=f"Kafka partition count (default: {DEFAULTS['partitions']})")
    p.add_argument("-g", "--gw-threads", type=int, default=DEFAULTS["gw_threads"],
                   help=f"Gateway threads (default: {DEFAULTS['gw_threads']})")
    args = p.parse_args()
    args.mode = args.mode.upper()
    return args


def derive(workers):
    pool_max = max(10, min(100, 400 // workers))
    pool_min = min(5, pool_max)
    pg_max = max(100, workers * pool_max + 20)
    return pool_min, pool_max, pg_max


def current_partitions(text):
    m = re.search(r"KAFKA_NUM_PARTITIONS=(\d+)", text)
    return int(m.group(1)) if m else 3


def apply(text, mode, workers, partitions, gw_threads, pool_min, pool_max, pg_max):
    nginx_conf = "gateway_nginx.conf" if mode == "TPC" else "gateway_nginx_saga.conf"

    text = re.sub(r"TRANSACTION_MODE=(TPC|SAGA)", f"TRANSACTION_MODE={mode}", text)
    text = re.sub(r"gateway_nginx[_a-z]*\.conf", nginx_conf, text)
    text = re.sub(
        r"gunicorn -k gevent -b 0\.0\.0\.0:5000 -w \d+ --timeout 30",
        f"gunicorn -k gevent -b 0.0.0.0:5000 -w {workers} --timeout 30",
        text,
    )
    text = re.sub(r"--threads \d+", f"--threads {gw_threads}", text)
    text = re.sub(r"KAFKA_NUM_PARTITIONS=\d+", f"KAFKA_NUM_PARTITIONS={partitions}", text)
    text = re.sub(r"DB_POOL_MIN=\d+", f"DB_POOL_MIN={pool_min}", text)
    text = re.sub(r"DB_POOL_MAX=\d+", f"DB_POOL_MAX={pool_max}", text)
    text = re.sub(r"max_connections=\d+", f"max_connections={pg_max}", text)
    return text


def run(cmd, check=True):
    print(f"  $ {cmd}")
    subprocess.run(cmd, shell=True, check=check)


def wait_for_health(timeout=90):
    urls = [
        "http://localhost:8000/orders/health",
        "http://localhost:8000/stock/health",
        "http://localhost:8000/payment/health",
    ]
    start = time.time()
    while time.time() - start < timeout:
        try:
            import urllib.request
            all_ok = True
            for url in urls:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    if resp.status != 200:
                        all_ok = False
                        break
            if all_ok:
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def main():
    args = parse_args()
    pool_min, pool_max, pg_max = derive(args.workers)

    with open(COMPOSE_FILE) as f:
        original = f.read()

    old_partitions = current_partitions(original)
    need_volume_wipe = old_partitions != args.partitions

    updated = apply(original, args.mode, args.workers, args.partitions,
                    args.gw_threads, pool_min, pool_max, pg_max)

    print("╔══════════════════════════════════════════════╗")
    print("║            TUNING CONFIGURATION              ║")
    print("╠══════════════════════════════════════════════╣")
    for label, val in [
        ("Mode",             args.mode),
        ("Workers/service",  args.workers),
        ("Kafka partitions", args.partitions),
        ("Gateway threads",  args.gw_threads),
        ("DB pool min/max",  f"{pool_min}/{pool_max}"),
        ("PG max_connections", pg_max),
        ("Volume wipe",      need_volume_wipe),
    ]:
        print(f"║  {label:<20s}  {str(val):<21s} ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    with open(COMPOSE_FILE, "w") as f:
        f.write(updated)

    if need_volume_wipe:
        print(">>> Partitions changed: wiping volumes ...")
        run("docker compose down -v")
    else:
        print(">>> Stopping services ...")
        run("docker compose down")

    print(">>> Building and starting ...")
    run("docker compose up -d --build")

    print("\n>>> Waiting for services to become healthy ...")
    if wait_for_health():
        print()
        print("╔══════════════════════════════════════════════╗")
        print("║          READY — Run your Locust test        ║")
        print("╚══════════════════════════════════════════════╝")
    else:
        print("\nWARNING: Services did not become healthy within 90s.")
        print("Check: docker compose ps / docker compose logs")


if __name__ == "__main__":
    main()
