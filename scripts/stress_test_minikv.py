import requests
import threading
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import Counter

BASE_URL = "http://localhost:8080"

# Configurable ratios
OP_WEIGHTS = {"get": 0.5, "set": 0.3, "delete": 0.2}  # 50% get, 30% set, 20% delete
NUM_THREADS = 200
OPS_PER_THREAD = 250
NUM_KEYS = 1000  # keys in the namespace

# Session with connection pooling & retries
session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=1000,
    pool_maxsize=1000,
    max_retries=Retry(total=3, backoff_factor=0.1)
)
session.mount("http://", adapter)
session.mount("https://", adapter)

success_count = 0
success_lock = threading.Lock()
op_results = Counter()


def set_key(key, value):
    try:
        r = session.post(f"{BASE_URL}/set", json={"key": key, "value": value}, timeout=2)
        with success_lock:
            op_results[( "set", r.status_code )] += 1
            if r.status_code == 200:
                global success_count
                success_count += 1
        return r.status_code
    except Exception as e:
        return f"ERR {e}"

def get_key(key):
    try:
        r = session.get(f"{BASE_URL}/get/{key}", timeout=2)
        with success_lock:
            op_results[( "get", r.status_code )] += 1
            if r.status_code == 200 or r.status_code == 404:
                global success_count
                success_count += 1
        return r.status_code
    except Exception as e:
        return f"ERR {e}"

def delete_key(key):
    try:
        r = session.delete(f"{BASE_URL}/delete/{key}", timeout=2)
        with success_lock:
            op_results[( "delete", r.status_code )] += 1
            if r.status_code == 200 or r.status_code == 404:
                global success_count
                success_count += 1
        return r.status_code
    except Exception as e:
        return f"ERR {e}"


def worker(thread_id, num_ops=100):
    for _ in range(num_ops):
        time.sleep(random.uniform(0.005, 0.02))  # jitter
        op = random.choices(
            population=list(OP_WEIGHTS.keys()),
            weights=list(OP_WEIGHTS.values())
        )[0]

        key = f"key{random.randint(1, NUM_KEYS)}"
        if op == "set":
            value = f"value{random.randint(1, NUM_KEYS)}"
            res = set_key(key, value)
        elif op == "get":
            res = get_key(key)
        else:  # delete
            res = delete_key(key)

        print(f"[Thread-{thread_id}] {op.upper()} {key} -> {res}")


def prepopulate_keys():
    print(f"Pre-populating {NUM_KEYS} keys...")
    for i in range(1, NUM_KEYS + 1):
        set_key(f"key{i}", f"value{i}")
    print("Pre-population complete.\n")


if __name__ == "__main__":
    start = time.time()

    prepopulate_keys()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(worker, i, OPS_PER_THREAD) for i in range(NUM_THREADS)]
        for f in as_completed(futures):
            pass

    duration = time.time() - start
    total_ops = NUM_THREADS * OPS_PER_THREAD

    print(f"\nCompleted {total_ops} ops in {duration:.2f}s "
          f"({total_ops/duration:.2f} ops/sec)")
    print(f"Successful calls: {success_count} out of {total_ops}")

    print("\nOperation breakdown:")
    for (op, code), count in op_results.items():
        print(f"  {op.upper()} {code}: {count}")