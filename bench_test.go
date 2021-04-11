package main

import (
	"fmt"
	"github.com/upamanyus/urpc/rpc"
	"testing"
	"time"
)

func TestRPC(t *testing.T) {
	fmt.Println("==Basic urpc test")
	client := rpc.MakeRPCClient("localhost:12345")

	var reply []byte
	args := make([]byte, 0)
	reply = make([]byte, 0)
	client.Call(1, args, &reply)
	fmt.Printf("%s\n", string(reply))
}

func TestBenchRPC(t *testing.T) {
	fmt.Println("==Benchmarking urpc")
	client := rpc.MakeRPCClient("localhost:12345")

	start := time.Now()
	N := 200000
	var reply []byte
	args := make([]byte, 0)
	for n := 0; n < N; n++ {
		reply = make([]byte, 0)
		client.Call(1, args, &reply)
	}
	d := time.Since(start)
	fmt.Printf("%v us/op\n", d.Microseconds()/int64(N))
	fmt.Printf("%v ops/sec\n", float64(N) / d.Seconds())
}
