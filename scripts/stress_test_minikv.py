import requests
import threading
import random
import time

BASE_URL = "http://localhost:8080"
session = requests.Session()
def set_key(key, value):
    try:
        r = requests.post(f"{BASE_URL}/set", json={"key": key, "value": value}, timeout=2)
        return r.status_code
    except Exception as e:
        return f"ERR {e}"

def get_key(key):
    try:
        r = requests.get(f"{BASE_URL}/get/{key}", timeout=2)
        return r.status_code
    except Exception as e:
        return f"ERR {e}"

def worker(thread_id, num_ops=100):
    for _ in range(num_ops):
        op = random.choice(["get", "set"])
        key = f"key{random.randint(1, 1000)}"

        if op == "set":
            value = f"value{random.randint(1, 1000)}"
            res = set_key(key, value)
        else:
            res = get_key(key)

        print(f"[Thread-{thread_id}] {op.upper()} {key} -> {res}")


if __name__ == "__main__":
    NUM_THREADS = 50
    OPS_PER_THREAD = 200

    start = time.time()

    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(i, OPS_PER_THREAD))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start
    total_ops = NUM_THREADS * OPS_PER_THREAD
    print(f"\n Completed {total_ops} ops in {duration:.2f}s "
          f"({total_ops/duration:.2f} ops/sec)")
