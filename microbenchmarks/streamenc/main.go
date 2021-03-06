package main

import (
	flag "github.com/spf13/pflag"
	"log"

	"bytes"
	"context"
	"fmt"
	"github.com/problame/go-streamrpc"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strconv"
	"time"
)

type limitReadCloser struct {
	io.Reader
}

func (limitReadCloser) Close() error { return nil }

func handleDevZeroStream(ctx context.Context, endpoint string, reqStructured *bytes.Buffer, reqStream io.ReadCloser) (resStructured *bytes.Buffer, resStream io.ReadCloser, err error) {
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
		RxHeaderMaxLen:       1024,
		RxStructuredMaxLen:   1 << 16,
		TxChunkSize:          0,
		Timeout: 24*time.Hour, // inf
		SendHeartbeatInterval: 1*time.Hour, // inf
	}
	clientConfig := &streamrpc.ClientConfig{
		ConnConfig: connConfig,
	}

	flag.StringVar(&mode, "mode", "client|server", "")
	flag.Uint32Var(&connConfig.TxChunkSize, "c.txcsiz", 1<<21, "")
	flag.Uint32Var(&connConfig.RxStreamMaxChunkSize, "c.rxmaxcsiz", 1<<21, "")
	flag.DurationVar(&connConfig.Timeout, "c.timeout", 24*time.Hour, "")
	flag.DurationVar(&connConfig.Timeout, "c.heartbeat", 1*time.Hour, "")
	flag.Parse()

	go http.ListenAndServe(":8080", nil)

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
		func() {
			defer conn.Close()
			log.Printf("begin request")
			begin := time.Now()

			client, err := streamrpc.NewClientOnConn(conn, clientConfig)
			if err != nil {
				log.Fatal(err)
			}
			defer client.Close(context.Background())

			rstru, rstre, err := client.RequestReply(context.Background(), "foo", bytes.NewBufferString(fmt.Sprintf("%v", nbytes)), nil)
			if err != nil {
				log.Fatal(err)
			}
			if rstru.String() != "OK" {
				log.Fatalf("unexpected response: %#v", rstru)
			}

			n, err := io.Copy(os.Stdout, rstre)
			delta := time.Now().Sub(begin)
			mbyteps := float64(n) / (1e6 * delta.Seconds())
			log.Printf("transferred %d bytes in %s, this amounts to %v MB/s or %v Mb/s", n, delta, mbyteps, mbyteps*8)
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
				if err := streamrpc.ServeConn(context.Background(), conn, connConfig, handleDevZeroStream); err != nil {
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
