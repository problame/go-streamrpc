package main

import (
	flag "github.com/spf13/pflag"
	"log"

	"regexp"
	"net"
	"github.com/problame/go-streamrpc"
	"bytes"
	"io"
	"strconv"
	"os"
	"context"
	"fmt"
	"time"
)

type limitReadCloser struct {
	io.Reader
}

func (limitReadCloser) Close() error { return nil }

func handleDevZeroStream(endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
	streamLen, err := strconv.ParseInt(reqStructured.String(), 0, 64)
	if err != nil {
		return nil, nil, err
	}
	z, err := os.Open("/dev/zero")
	if err != nil {
		return nil, nil, err
	}
	return bytes.NewBufferString("OK"), limitReadCloser{io.LimitReader(z, streamLen)}, nil
}

func main() {

	var mode string
	connConfig := &streamrpc.ConnConfig{
		RxStreamMaxChunkSize: 0,
		RxHeaderMaxLen: 1024,
		RxStructuredMaxLen: 1 << 16,
		TxChunkSize: 0,
	}
	clientConfig := &streamrpc.ClientConfig{
		ConnConfig:             connConfig,
	}

	flag.StringVar(&mode, "mode", "client|server", "")
	flag.Uint32Var(&connConfig.TxChunkSize, "c.txcsiz", 1 << 21 , "")
	flag.Uint32Var(&connConfig.RxStreamMaxChunkSize, "c.rxmaxcsiz", 1 << 21, "")
	flag.Parse()

	serverRE := regexp.MustCompile(`^server:(tcp):(.*:.*)$`)
	clientRE := regexp.MustCompile(`^client:(tcp):(.*:.*):(.*)$`)
	switch {
	case clientRE.MatchString(mode):

		m := clientRE.FindStringSubmatch(mode)
		nbytes, err := strconv.ParseInt(m[3], 0, 64)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("begin Dial")
		conn, err := net.Dial(m[1], m[2])
		if err != nil {
			log.Fatal(err)
		}
		func () {
			defer conn.Close()
			log.Printf("begin request")
			begin := time.Now()

			client, err := streamrpc.NewClientOnConn(conn, clientConfig)
			if err != nil {
				log.Fatal(err)
			}
			defer client.Close()

			rstru, rstre, err := client.RequestReply(context.Background(), "foo", bytes.NewBufferString(fmt.Sprintf("%v", nbytes)), nil)
			if err != nil {
				log.Fatal(err)
			}
			if rstru.String() != "OK" {
				log.Fatalf("unexpected response: %#v", rstru)
			}

			n, err := io.Copy(os.Stdout, rstre)
			delta := time.Now().Sub(begin)
			byteps := float64(n) / delta.Seconds()
			log.Printf("transferred %d bytes in %s, this amounts to %v MB/s or %v Mb/s", n, delta, byteps/1e6, (byteps/1e6)*8)
			if err != nil {
				log.Fatal(err)
			}
			if n != nbytes {
				log.Fatalf("n = %v != %v = nbytes", n, nbytes)
			}
			log.Printf("request done")
		}()

	case serverRE.MatchString(mode):
		m := serverRE.FindStringSubmatch(mode)
		l, err := net.Listen(m[1], m[2])
		if err != nil {
			log.Fatal(err)
		}
		defer l.Close()

		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal(err)
			}
			func() {
				defer conn.Close()
				log.Printf("begin ServeConn: %s", conn)
				if err := streamrpc.ServeConn(conn, connConfig, handleDevZeroStream); err != nil {
					log.Printf("error ServeConn: %s", err)
				} else {
					log.Printf("finished ServeConn")
				}
			}()
		}


	default:
		log.Fatal("specify -mode client|server")
	}

}
