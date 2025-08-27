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
    set_key("foo", "bar")
    set_key("hello", "world")

    get_key("foo")
    get_key("hello")

    delete_key("foo")

    get_key("foo")

    compact()
