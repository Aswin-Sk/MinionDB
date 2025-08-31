package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func setKey(url, key, value string) {
	data := map[string]string{
		"key":   key,
		"value": value,
	}
	body, _ := json.Marshal(data)

	resp, err := http.Post(url+"/set", "application/json", bytes.NewBuffer(body))
	if err != nil {
		panic(fmt.Sprintf("SET error: %v", err))
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Println("SET response:", string(out))
}

func getKey(url, key string) {
	resp, err := http.Get(url + "/get/" + key)
	if err != nil {
		panic(fmt.Sprintf("GET error: %v", err))
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Println("GET response:", string(out))
}

func deleteKey(url, key string) {
	req, _ := http.NewRequest(http.MethodDelete, url+"/delete/"+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(fmt.Sprintf("DELETE error: %v", err))
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Println("DELETE response:", string(out))
}

func main() {
	baseURL := "http://localhost:8080"

	time.Sleep(500 * time.Millisecond)

	setKey(baseURL, "foo", "bar")
	setKey(baseURL, "hello", "world")
	getKey(baseURL, "foo")
	deleteKey(baseURL, "foo")
	getKey(baseURL, "foo")
	getKey(baseURL, "hello")
}
