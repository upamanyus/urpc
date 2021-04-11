package rpc

import (
	"github.com/tchajed/marshal"
	"io"
	"net"
	"sync"
)

func (srv *RPCServer) rpcHandle(c net.Conn, rpcid uint64, seqno uint64, data []byte) {
	replyData := make([]byte, 0)

	srv.handlers[rpcid](data, &replyData) // call the function

	e := marshal.NewEnc(8 + 8 + uint64(len(replyData)))
	e.PutInt(seqno)
	e.PutInt(uint64(len(replyData)))
	e.PutBytes(replyData)
	_, err := c.Write(e.Finish()) // TODO: contention? should we buffer these in userspace too?
	if err != nil {
		// client might close the connection by the time that we finish
		// processing their request
		// panic(err)
	}
}

func (srv *RPCServer) handleConn(c net.Conn) {
	for {
		// header format: [rpcid, seqno, dataLen]
		headerData := make([]byte, 8*3)
		_, err := io.ReadFull(c, headerData)

		if err != nil {
			break
		}
		d := marshal.NewDec(headerData)
		rpcid := d.GetInt()
		dataLen := d.GetInt()
		seqno := d.GetInt()

		data := make([]byte, dataLen)
		io.ReadFull(c, data)
		// fmt.Printf("Received RPC %d(seq %d) with %d bytes\n", rpcid, seqno, len(data))
		go srv.rpcHandle(c, rpcid, seqno, data)
	}
	c.Close()
}

type RPCServer struct {
	handlers map[uint64]func([]byte, *[]byte)
}

func MakeRPCServer(handlers map[uint64]func([]byte, *[]byte)) *RPCServer {
	return &RPCServer{handlers}
}

func (srv *RPCServer) Serve(port string) {
	l, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go srv.handleConn(c)
	}
}

type callback struct {
	reply *[]byte
	done  *bool
	cond  *sync.Cond
}

type RPCClient struct {
	mu   *sync.Mutex
	conn net.Conn
	seq  uint64 // next fresh sequence number

	pending map[uint64]*callback
}

func (cl *RPCClient) replyThread() {
	for {
		// reply format: [seqno, dataLen] ++ data
		headerData := make([]byte, 8*2)
		_, err := io.ReadFull(cl.conn, headerData)
		if err != nil {
			panic(err)
		}

		d := marshal.NewDec(headerData)
		seqno := d.GetInt()
		replyLen := d.GetInt()

		reply := make([]byte, replyLen)
		_, err = io.ReadFull(cl.conn, reply)
		if err != nil {
			panic(err)
		}

		cl.mu.Lock()
		cb, ok := cl.pending[seqno]
		if ok {
			delete(cl.pending, seqno)
			*cb.reply = reply
			*cb.done = true
			cb.cond.Signal()
		}
		cl.mu.Unlock()
	}
}

func MakeRPCClient(host string) *RPCClient {
	cl := new(RPCClient)
	conn, err := net.Dial("tcp", host)
	if err != nil {
		panic(err)
	}

	cl.conn = conn
	cl.mu = new(sync.Mutex)
	cl.seq = 1
	cl.pending = make(map[uint64]*callback)

	go cl.replyThread()
	return cl
}

func (cl *RPCClient) Call(rpcid uint64, args []byte, reply *[]byte) bool {
	cb := callback{reply: reply, done: new(bool), cond: sync.NewCond(cl.mu)}
	*cb.done = false
	cl.mu.Lock()
	seqno := cl.seq
	cl.seq = cl.seq + 1
	cl.pending[seqno] = &cb
	cl.mu.Unlock()

	e := marshal.NewEnc(8 + 8 + 8 + uint64(len(args)))
	e.PutInt(rpcid)
	e.PutInt(uint64(len(args)))
	e.PutInt(seqno)
	e.PutBytes(args)
	reqData := e.Finish()

	_, err := cl.conn.Write(reqData)
	if err != nil {
		panic(err)
	}

	// wait for reply
	cl.mu.Lock()
	for !*cb.done {
		cb.cond.Wait()
	}
	cl.mu.Unlock()
	return false
}
