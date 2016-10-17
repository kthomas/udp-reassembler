package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-bitstream"
)

var wg sync.WaitGroup
var messages map[uint64][]byte
var mutex = &sync.Mutex{}

func ReassembleMessages() {
	main()
}

func main() {
	protocol := "udp"
	port := ":6789"

	conn, err := bind(protocol, port)

	if err == nil {
		messages = make(map[uint64][]byte)

		concurrency := getConcurrency()

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go read(conn)
		}

		wg.Add(1)
		go watchMessages()

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
		var buf [512]byte
		n, _, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			msg := fmt.Sprintf("Failed to read on UDP connection; %s", err)
			fmt.Println(msg)
			break
		} else if n > 0 {
			process(buf[0:n])
		}
	}
	defer wg.Done()
}

func process(packet []byte) {
	reader := bitstream.NewReader(bytes.NewReader(packet))

	flags, err := reader.ReadBits(16)
	checkPacketError(err)
	eof := flags == 0x8000

	dataSize, err := reader.ReadBits(16)
	checkPacketError(err)

	offset, err := reader.ReadBits(32)
	checkPacketError(err)

	transactionId, err := reader.ReadBits(32)
	checkPacketError(err)

	var buf []byte

	mutex.Lock()
	defer mutex.Unlock()

	if eof {
		buf = getMessageBuffer(transactionId, uint32(dataSize))
	} else {
		buf = getMessageBuffer(transactionId, 1024 * 1024)
	}

	packetSize := len(packet) - 12

	if packetSize > 0 {
		pkt := make([]byte, packetSize)
		copy(pkt, packet[12:])
		if int(offset) + packetSize < len(buf) {
			copy(buf[offset:], pkt)
		}
	}
}

func checkPacketError(err error) {
	if err != nil {
		msg := fmt.Sprintf("Error processing packet: %s", err)
		fmt.Println(msg)
	}
}

func getConcurrency() int {
	var concurrency int = 1
	if os.Getenv("CONCURRENCY") != "" {
		concurrency64, _ := strconv.ParseInt(os.Getenv("CONCURRENCY"), 10, 8)
		concurrency = int(concurrency64)
	}
	return concurrency
}

func getMessageBuffer(transactionId uint64, size uint32) []byte {
	msg, ok := messages[transactionId]
	if !ok {
		msg = make([]byte, size)
		messages[transactionId] = msg
	}
	return msg
}

func getMessage(transactionId uint64) string {
	msg := messages[transactionId]
	msg = bytes.Trim(msg, "\x00")
	return string(msg)
}

func getMessageSHA(transactionId uint64) string {
	hash := sha256.New()
	n, err := hash.Write([]byte(getMessage(transactionId)))
	if err != nil {
		msg := fmt.Sprintf("Error calculating sha256 for message; transactionId = %v", n)
		fmt.Println(msg)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func watchMessages() {
	for {
		time.Sleep(time.Second * 5)
		if len(messages) == 10 {
			i := 1
			for k, v := range messages {
				buf := bytes.Trim(v, "\x00")

				//holes := findHoles(buf)
				//for hole := range holes {
				//	msg := fmt.Sprintf("Message #%v hole at: %v", k, holes[hole])
				//	fmt.Println(msg)
				//}

				msg := fmt.Sprintf("Message #%v length: %v sha256:%s", i, len(buf), getMessageSHA(k))
				fmt.Println(msg)

				i++
			}

			wg.Add(-getConcurrency())
			break
		}
	}
	defer wg.Done()
}

func findHoles(buf []byte) []int {
	holes := []int{}
	str := string(buf)
	index := strings.Index(str, "\x00")
	if index != -1 {
		i := index
		holes = []int{ i }
		i++
		str = string(str[i:])
		holeSlice := findHoles([]byte(str))
		for idx := range holeSlice {
			unique := true
			for x := range holes {
				if holes[x] == holeSlice[idx] {
					unique = false
					break
				}
			}
			if unique {
				holes = append(holes, holeSlice[idx])
			}
		}
	}
	sort.Ints(holes)
	return holes
}
