package client

import (
	"testing"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"errors"
	"fmt"
)

func GetRandomString() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}

func testClientThread(t *testing.T, host string, wg *sync.WaitGroup) {
	defer wg.Done()

	client := NewClient(host)
	err := client.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 1000; i++ {
		key := GetRandomString()
		value := GetRandomString()

		err := client.SetKey(key, value)
		if err != nil {
			t.Fatal(err)
		}

		rvalue, err := client.GetKey(key)
		if err != nil {
			t.Fatal(err)
		}

		if value != rvalue {
			t.Fatal(errors.New(fmt.Sprintf("key %s val %s:%d rval %s:%d",
				key, value, len(value), rvalue, len(rvalue))))

		}

		value = GetRandomString()
		err = client.UpdateKey(key, value)
		if err != nil {
			t.Fatal(err)
		}

		rvalue, err = client.GetKey(key)
		if err != nil {
			t.Fatal(err)
		}

		if value != rvalue {
			t.Fatal(errors.New(fmt.Sprintf("key %s val %s rval %s", key, value, rvalue)))
		}
	}
}

func TestClient(t *testing.T) {
	host := "127.0.0.1:8111"
	wg := new(sync.WaitGroup)
	for i := 0; i < 4 ; i++ {
		wg.Add(1)
		go testClientThread(t, host, wg)
	}
	wg.Wait()
}
