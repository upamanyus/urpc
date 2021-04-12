package rpc

import (
	"github.com/tchajed/marshal"
	"io"
	"net"
	"sync"
	"fmt"
)

type RPCServerWorker struct {
	handlers map[uint64]func([]byte, *[]byte)
	epoller  *Epoller
	state    map[int]reqState
}

func (srv *RPCServerWorker) rpcHandle(c net.Conn, rpcid uint64, seqno uint64, data []byte) {
	/*
	start := time.Now()
	defer func() {
		fmt.Printf("%+v\n", time.Since(start))
	}()
	*/
	replyData := make([]byte, 0)

	srv.handlers[rpcid](data, &replyData) // call the function

	e := marshal.NewEnc(8 + 8 + uint64(len(replyData)))
	e.PutInt(seqno)
	e.PutInt(uint64(len(replyData)))
	e.PutBytes(replyData)
	_, err := c.Write(e.Finish()) // TODO: contention? should we buffer these in userspace too?
	if err != nil {
		panic(err)
		// client might close the connection by the time that we finish
		// processing their request
		// panic(err)
	}
}

type reqState struct {
	data []byte
	off  uint64
}

type RPCServer struct {
	handlers map[uint64]func([]byte, *[]byte)
	workerEpollers []*Epoller
}

func MakeRPCServer(handlers map[uint64]func([]byte, *[]byte), numWorkers uint64) *RPCServer {
	epollers := make([]*Epoller, numWorkers)
	for i := uint64(0); i < numWorkers; i++ {
		epollers[i] = MakeEpoller()
	}
	return &RPCServer{handlers: handlers, workerEpollers: epollers}
}

func (srv *RPCServer) Serve(port string) {
	go srv.acceptThread(port)
	for i := 0; i < len(srv.workerEpollers); i++ {
		s := &RPCServerWorker{handlers:srv.handlers, epoller:srv.workerEpollers[i], state:make(map[int]reqState)}
		go s.readThread()
	}
}

func (srv *RPCServer) acceptThread(port string) {
	l, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for i := 0; ; i++ {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		// fmt.Printf("Adding to %d\n", i % len(srv.workerEpollers))
		srv.workerEpollers[i % len(srv.workerEpollers) ].Add(c) // FIXME: concurrent map access
	}
}

const headerSize = 8 * 3

func (srv *RPCServerWorker) readThread() {
	// fmt.Println("Worker thread")
	for {
		es := srv.epoller.Wait()
		for _, e := range es {
			// fmt.Println("Polled")
			f := int(e.Fd)
			c := srv.epoller.Conns[f]
			s, ok := srv.state[f]
			if !ok {
				s = reqState{data: nil, off: 0}
			}
			if len(s.data) == 0 {
				s.data = make([]byte, 1024)
				s.off = 0
			}

			// fmt.Printf("%+v\n", s)
			if s.off >= uint64(len(s.data)) {
				fmt.Println(s)
			}
			if len(s.data) == 0 {
				fmt.Println(s)
			}
			n, err := c.Read(s.data[s.off:])
			if err != nil {
				srv.epoller.Remove(c)
				delete(srv.state, f)
			}

			if s.off < headerSize && headerSize <= (uint64(n)+s.off) {
				// get length of args data, and grow s.data if needed
				d := marshal.NewDec(s.data[8*2 : 8*3])
				argsLen := d.GetInt()
				// fmt.Println(argsLen)
				if argsLen > uint64((len(s.data) - 8*3)) {
					extraSize := argsLen - uint64((len(s.data) - 8*3))
					s.data = append(s.data, make([]byte, extraSize)...)
				} else {
					s.data = s.data[:headerSize+argsLen]
					// fmt.Printf("trim: %+v\n", s)
				}
			}
			s.off = s.off + uint64(n)
			if s.off == uint64(len(s.data)) {
				// got a full request
				d := marshal.NewDec(s.data)
				rpcid := d.GetInt()
				seqno := d.GetInt()
				d.GetInt() // skip
				reqData := s.data[headerSize:]
				go srv.rpcHandle(c, rpcid, seqno, reqData)
				s = reqState{data: nil, off: 0}
			}

			srv.state[f] = s
		}
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
		// fmt.Printf("Client read header %+v\n", headerData)

		d := marshal.NewDec(headerData)
		seqno := d.GetInt()
		replyLen := d.GetInt()

		reply := make([]byte, replyLen)
		_, err = io.ReadFull(cl.conn, reply)
		// fmt.Printf("Client read %+v\n", reply)
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
	e.PutInt(seqno)
	e.PutInt(uint64(len(args)))
	e.PutBytes(args)
	reqData := e.Finish()
	// fmt.Fprintf(os.Stderr, "%+v\n", reqData)

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
