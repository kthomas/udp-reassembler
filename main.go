package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgryski/go-bitstream"
)

var wg sync.WaitGroup

func main() {
	protocol := "udp"
	port := ":6789"

	conn, err := bind(protocol, port)

	if err == nil {
		var concurrency int = 1
		if os.Getenv("CONCURRENCY") != "" {
			concurrency64, _ := strconv.ParseInt(os.Getenv("CONCURRENCY"), 10, 8)
			concurrency = int(concurrency64)
		}

		msg := fmt.Sprintf("Reading shared UDP port %s; concurrency: %v", port, concurrency)
		fmt.Println(msg)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go read(conn)
		}

		wg.Wait()
	}
}

func bind(protocol string, port string) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr(protocol, port)
	if err != nil {
		msg := fmt.Sprintf("Unable to resolve UDP address on %s", port)
		fmt.Println(msg)
		return nil, err
	} else {
		conn, err := net.ListenUDP(protocol, addr)
		if err != nil {
			msg := fmt.Sprintf("Unable to bind UDP listener on %s", addr)
			fmt.Println(msg)
			return nil, err
		} else {
			return conn, nil
		}
	}
}

func read(conn *net.UDPConn) {
	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		var buf [256]byte
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			msg := fmt.Sprintf("Failed to read on UDP connection; %s", err)
			fmt.Println(msg)
			break
		} else if n > 0 {
			msg := fmt.Sprintf("Read %v-byte UDP packet from %s", n, addr)
			fmt.Println(msg)
			process(buf[0:n])
		}
	}
	defer wg.Done()
}

func process(packet []byte) {
	// see https://tools.ietf.org/html/rfc815
	reader := bitstream.NewReader(bytes.NewReader(packet))
	flags, _ := reader.ReadBits(15)
	size, _ := reader.ReadBits(16)
	offset, _ := reader.ReadBits(32)
	transactionId, _ := reader.ReadBits(32)
	data, _ := reader.ReadBits(int(size))

	msg := fmt.Sprintf("flags = %v; size = %v; offset = %v, transactionId = %v, data = %s", flags, size, offset, transactionId, data)
	fmt.Println(msg)
}
