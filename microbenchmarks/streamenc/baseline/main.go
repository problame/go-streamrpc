package main

import (
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)

type discardWriter struct{}

func (discardWriter) Write(buf []byte) (n int, err error) {
	return len(buf), nil
}

func copybuf(w io.Writer, r io.Reader, buf []byte) (total int64) {
	done := false
	for !done {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			log.Fatalf("n=%v err=%s", n, err)
		}
		done = err == io.EOF
		n2, err := w.Write(buf[:n])
		if n != n2 || err != nil {
			log.Fatalf("n2=%v err=%s", n, err)
		}
		total += int64(n)
	}
	return total
}

var conf struct {
	mode         string
	listenOrDial string
	bufsize      int
	bytecount    int64 // server only
	pprof        string
}

func main() {

	flag.Int64Var(&conf.bytecount, "bytecount", 1<<35, "number of bytes to transfer")
	flag.IntVar(&conf.bufsize, "bufsize", 1<<21, "buffer size")
	flag.StringVar(&conf.mode, "mode", "", "client|server")
	flag.StringVar(&conf.listenOrDial, "listen", "localhost:2342", "")
	flag.StringVar(&conf.listenOrDial, "dial", "localhost:2342", "")
	flag.StringVar(&conf.pprof, "pprof", "localhost:8080", "pprof http listener")
	flag.Parse()

	go http.ListenAndServe(conf.pprof, nil)

	switch conf.mode {
	case "server":
		server()
	case "client":
		client()
	default:
		log.Printf("unknown mode '%s'", conf.mode)
		log.Printf(`usage:
    In one terminal:
        taskset -c 0 ./baseline -mode server
    In another terminal:
        taskset -c 1 ./baseline -mode client < /dev/zero`)
		os.Exit(1)
	}

}

func server() {
	l, err := net.Listen("tcp", conf.listenOrDial)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		func() {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			buf := make([]byte, conf.bufsize)
			d := discardWriter{}
			begin := time.Now()
			n := copybuf(d, conn, buf[:])
			delta := time.Now().Sub(begin)
			txed := float64(n) / (1e6 * delta.Seconds())
			txedBits := txed * 8
			log.Printf("connection done: %v => %v MB/s %v Mbit/s err: %v", n, txed, txedBits, err)
		}()
	}
}

func client() {
	c, err := net.Dial("tcp", conf.listenOrDial)
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, conf.bufsize)

	r := io.LimitReader(os.Stdin, conf.bytecount)
	before := time.Now()
	n := copybuf(c, r, buf)
	delta := time.Now().Sub(before)
	log.Printf("connection done: %v bytes => %v Mbit/s", n, 8*float64(n)/(1e6*delta.Seconds()))
}
