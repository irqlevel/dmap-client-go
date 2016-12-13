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
