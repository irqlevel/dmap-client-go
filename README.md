### [dmap](https://github.com/irqlevel/dmap) go client

####Usage:
```go
import "github.com/irqlevel/dmap-client-go/client"
...
client := new(client.Client)
client.Init("host:port")
err := client.Dial()
if err != nil {
	panic(err)
}
defer client.Close()

err = client.SetKey("mykey", "myvalue")
...
value, err := client.GetKey("mykey")
...
value, err := client.CmpxchgKey("mykey", "newvalue", "myvalue")
...
```

####Test:
```sh
cd client && go test
```

####Play:
```sh
$ go get github.com/irqlevel/dmap-client-go/client

$ go build -o dmap-client tool/main.go

$ ./dmap-client hostname:port set key value #add key-value

$ ./dmap-client hostname:port get key #query value by key

$ ./dmap-client hostname:port upd key value #update key value

$ ./dmap-client hostname:post cmpxchg key exchange comparand #compare exchange key value

$ ./dmap-client hostname:port del key #delete key
```
