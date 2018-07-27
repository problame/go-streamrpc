package main

import (
	flag "github.com/spf13/pflag"
	"log"

	"regexp"
	"net"
	"io"
	"strconv"
	"os"
	"time"
	"encoding/binary"
	"net/http"
	_ "net/http/pprof"
)

func main() {

	var mode string
	var bufsiz int

	flag.StringVar(&mode, "mode", "client|server", "")
	flag.IntVar(&bufsiz, "c.bufsiz", 1 << 21, "")
	flag.Parse()

	go http.ListenAndServe(":8080", nil)

	buf := make([]byte, bufsiz)

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

			if err := binary.Write(conn, binary.BigEndian, nbytes); err != nil {
				log.Fatal(err)
			}

			var n int64
			for n < nbytes {
				readc := int64(len(buf))
				if nbytes - n < readc {
					readc = nbytes - n
				}

				n1, err := conn.Read(buf[0:readc])
				if n1 > 0 {
					n += int64(n1)
					n2, err := os.Stdout.Write(buf[0:n1])
					if err != nil || n2 != n1 {
						log.Fatal(err)
					}
				}
				if err != nil {
					if err != io.EOF {
						log.Fatal(err)
					}
					break
				}
			}

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
				defer log.Printf("finished ServeConn")


				var nbytes int64
				if err := binary.Read(conn, binary.BigEndian, &nbytes); err != nil {
					log.Fatal(err)
				}
				z, err := os.Open("/dev/zero")
				if err != nil {
					log.Fatal(err)
				}
				defer z.Close()

				r := io.LimitReader(z, nbytes)
				for {
					readc := int64(len(buf))
					n1, err := r.Read(buf[0:readc])
					if n1 > 0 {
						n2, err := conn.Write(buf[0:n1])
						if err != nil || n2 != n1 {
							log.Fatal(err)
						}
					}
					if err != nil {
						if err != io.EOF {
							log.Fatal(err)
						}
						break
					}
				}
			}()
		}


	default:
		log.Fatal("specify -mode client|server")
	}

}
