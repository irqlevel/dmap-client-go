package main

import (
	"os"
	"log"
	"github.com/irqlevel/dmap-client-go/client"
)

func Usage(rc int) {
	log.Printf("Usage:\n")
	log.Printf("dmap-client <host:ip> <set> <key> <value>\n")
	log.Printf("dmap-client <host:ip> <get> <key>\n")
	log.Printf("dmap-client <host:ip> <del> <key>\n")
	log.Printf("dmap-client <host:ip> <upd> <key> <value>\n")
	log.Printf("dmap-client <host:ip> <cmpxchg> <key> <exchange> <comparand>\n")
	os.Exit(rc)
}

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	if len(os.Args) < 3 {
		Usage(1)
		return
	}

	host := os.Args[1]
	cmd := os.Args[2]

	client := client.NewClient(host)
	if cmd == "set" {
		if len(os.Args) != 5 {
			Usage(1)
			return
		}
		key := os.Args[3]
		value := os.Args[4]
		err := client.Dial()
		if err != nil {
			log.Printf("Dial failed: %v\n", err)
			os.Exit(1)
			return
		}
		defer client.Close()
		err = client.SetKey(key, value)
		if err != nil {
			log.Printf("Set key failed: %v\n", err)
			os.Exit(1)
			return
		}
	} else if cmd == "get" {
		if len(os.Args) != 4 {
			Usage(1)
			return
		}
		key := os.Args[3]
		err := client.Dial()
		if err != nil {
			log.Printf("Dial failed: %v\n", err)
			os.Exit(1)
			return
		}
		defer client.Close()
		value, err := client.GetKey(key)
		if err != nil {
			log.Printf("Get key failed: %v\n", err)
			os.Exit(1)
			return
		}
		log.Printf("%s\n", value)
	} else if cmd == "del" {
		if len(os.Args) != 4 {
			Usage(1)
			return
		}
		key := os.Args[3]
		err := client.Dial()
		if err != nil {
			log.Printf("Dial failed: %v\n", err)
			os.Exit(1)
			return
		}
		defer client.Close()
		err = client.DelKey(key)
		if err != nil {
			log.Printf("Delete key failed: %v\n", err)
			os.Exit(1)
			return
		}
	} else if cmd == "upd" {
		if len(os.Args) != 5 {
			Usage(1)
			return
		}
		key := os.Args[3]
		value := os.Args[4]
		err := client.Dial()
		if err != nil {
			log.Printf("Dial failed: %v\n", err)
			os.Exit(1)
			return
		}
		defer client.Close()
		err = client.UpdateKey(key, value)
		if err != nil {
			log.Printf("Update key failed: %v\n", err)
			os.Exit(1)
			return
		}
	} else if cmd == "cmpxchg" {
		if len(os.Args) != 6 {
			Usage(1)
			return
		}
		key := os.Args[3]
		exchange := os.Args[4]
		comparand := os.Args[5]

		err := client.Dial()
		if err != nil {
			log.Printf("Dial failed: %v\n", err)
			os.Exit(1)
			return
		}
		defer client.Close()
		value, err := client.CmpxchgKey(key, exchange, comparand)
		if err != nil {
			log.Printf("Cmpxchg key failed: %v\n", err)
			os.Exit(1)
			return
		}
		log.Printf("%s\n", value)
	} else {
		Usage(1)
		return
	}
	os.Exit(0)
	return
}
