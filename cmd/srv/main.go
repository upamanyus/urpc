package main

import (
	"github.com/upamanyus/urpc/rpc"
	"fmt"
)

func main() {
	fmt.Println("Starting server on port 12345")
	handlers := make(map[uint64]func([]byte, *[]byte))
	handlers[1] = func(args []byte, reply *[]byte) {
		*reply = []byte("This works!")
		return
	}
	s := rpc.MakeRPCServer(handlers)
	s.Serve(":12345")
	select{}
}
