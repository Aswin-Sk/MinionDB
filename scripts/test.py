import requests

BASE_URL = "http://localhost:8080" 


def set_key(key, value):
    r = requests.post(f"{BASE_URL}/set", json={"key": key, "value": value})
    print("SET:", r.json())


def get_key(key):
    r = requests.get(f"{BASE_URL}/get/{key}")
    print("GET:", r.json())


def delete_key(key):
    r = requests.delete(f"{BASE_URL}/delete/{key}")
    print("DELETE:", r.json())


def compact():
    r = requests.post(f"{BASE_URL}/compact")
    print("COMPACT:", r.json())


if __name__ == "__main__":
    # 1. Set keys
    r = requests.post(f"{BASE_URL}/set", json={"key": "foo", "value": "bar"})
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

    r = requests.post(f"{BASE_URL}/set", json={"key": "hello", "value": "world"})
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

    # 2. Get keys
    r = requests.get(f"{BASE_URL}/get/foo")
    assert r.status_code == 200
    assert r.json() == {"key": "foo", "value": "bar"}

    r = requests.get(f"{BASE_URL}/get/hello")
    assert r.status_code == 200
    assert r.json() == {"key": "hello", "value": "world"}

    # 3. Delete key
    r = requests.delete(f"{BASE_URL}/delete/foo")
    assert r.status_code == 200
    assert r.json() == {"status": "deleted"}

    # 4. Verify key deleted
    r = requests.get(f"{BASE_URL}/get/foo")
    assert r.status_code == 404 or "error" in r.json()
    assert r.json() == {"error": "key not found"}

    # 5. Compact
    r = requests.post(f"{BASE_URL}/compact")
    assert r.status_code == 200
    assert r.json() == {"status": "compacted"}
    print("All tests passed!")