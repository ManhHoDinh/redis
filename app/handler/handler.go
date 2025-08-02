package handler

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"redis/app/types"
	"strconv"
	"strings"
	"sync"
	"time"
)

var store = make(map[string]types.Entry)
var rPlush = make(map[string][]string)

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := parseArgs(conn, reader)
		if err != nil {
			writeError(conn, err.Error())
			continue
		}
		if len(args) == 0 {
			writeError(conn, "empty command")
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			handlePing(conn)
		case "ECHO":
			handleEcho(conn, args)
		case "SET":
			handleSet(conn, args)
		case "GET":
			handleGet(conn, args)
		case "LPUSH":
			handleLPush(conn, args)
		case "RPUSH":
			handleRPush(conn, args)
		case "LRANGE":
			handleLRange(conn, args)
		case "LLEN":
			handleLLen(conn, args)
		case "LPOP":
			handleLPop(conn, args)
		case "BLPOP":
			handleBLPop(conn, args)
		default:
			writeError(conn, fmt.Sprintf("unknown command '%s'", args[0]))
		}
	}
}

func handlePing(conn net.Conn) {
	writeSimpleString(conn, "PONG")
}

func handleEcho(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'ECHO'")
		return
	}
	writeSimpleString(conn, args[1])
}

func handleSet(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'SET'")
		return
	}
	key := args[1]
	val := args[2]
	var expiry time.Time

	if len(args) >= 5 && strings.ToUpper(args[3]) == "PX" {
		ms, err := strconv.Atoi(args[4])
		if err != nil {
			writeError(conn, "PX value must be integer")
			return
		}
		expiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}

	store[key] = types.Entry{Value: val, ExpiryTime: expiry}
	writeSimpleString(conn, "OK")
}

func handleGet(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'GET'")
		return
	}
	key := args[1]
	entry, ok := store[key]
	if !ok || (entry.ExpiryTime != (time.Time{}) && time.Now().After(entry.ExpiryTime)) {
		delete(store, key)
		writeNull(conn)
		return
	}
	writeBulkString(conn, entry.Value)
}
func handleLPush(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'LPUSH'")
		return
	}

	key := args[1]

	mu.Lock()
	defer mu.Unlock()

	for i := 2; i < len(args); i++ {
		rPlush[key] = append([]string{args[i]}, rPlush[key]...)
	}

	// Wake up blocked BLPOP clients if any
	wakeUpFirstBlocking(key)

	writeInteger(conn, len(rPlush[key]))
}

func handleRPush(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'RPUSH'")
		return
	}

	key := args[1]

	mu.Lock()
	defer mu.Unlock()

	for i := 2; i < len(args); i++ {
		rPlush[key] = append(rPlush[key], args[i])
	}

	wakeUpFirstBlocking(key)

	writeInteger(conn, len(rPlush[key]))
}


func handleLRange(conn net.Conn, args []string) {
	if len(args) != 4 {
		writeError(conn, "wrong number of arguments for 'LRANGE'")
		return
	}
	key := args[1]
	start, err1 := strconv.Atoi(args[2])
	end, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		writeError(conn, "invalid start or end index")
		return
	}
	list := rPlush[key]

	if start < 0 {
		start = len(list) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(list) + end
		if end < 0 {
			end = 0
		}
	}
	if start >= len(list) || start > end {
		conn.Write([]byte("*0\r\n"))
		return
	}
	if end >= len(list) {
		end = len(list) - 1
	}
	sublist := list[start : end+1]
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(sublist))))
	for _, item := range sublist {
		writeBulkString(conn, item)
	}
}

func handleLLen(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'LLEN'")
		return
	}
	writeInteger(conn, len(rPlush[args[1]]))
}

func handleLPop(conn net.Conn, args []string) {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'LPOP'")
		return
	}
	key := args[1]
	list := rPlush[key]
	if len(list) == 0 {
		writeNull(conn)
		return
	}
	if len(args) == 3 {
		count, err := strconv.Atoi(args[2])
		if err != nil || count <= 0 {
			count = 1
		}
		if count > len(list) {
			count = len(list)
		}
		rPlush[key] = list[count:]
		conn.Write([]byte(fmt.Sprintf("*%d\r\n", count)))
		for i := 0; i < count; i++ {
			writeBulkString(conn, list[i])
		}
	} else {
		rPlush[key] = list[1:]
		writeBulkString(conn, list[0])
	}
}

var (
	blockings = make(map[string][]types.BlockingRequest)
	mu        = sync.Mutex{}
)
func handleBLPop(conn net.Conn, args []string) {
	if len(args) != 3 {
		writeError(conn, "wrong number of arguments for 'BLPOP'")
		return
	}

	mu.Lock()
	key := args[1]
	if list, ok := rPlush[key]; ok && len(list) > 0 {
		value := list[0]
		rPlush[key] = list[1:]
		mu.Unlock()

		conn.Write([]byte("*2\r\n"))
		writeBulkString(conn, key)
		writeBulkString(conn, value)
		return
	}


	timeoutStr := args[2]
	timeout, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil {
		writeError(conn, "timeout must be a number")
		return
	}

	ch := make(chan string, 1)
	blocking := types.BlockingRequest{
		Key:     key,
		Ch:      ch,
		Timeout: time.Duration(timeout * float64(time.Second)),
	}
	blockings[key] = append(blockings[key], blocking)
	mu.Unlock()

	if timeout == 0 {
		_, ok := <-ch
		if !ok {
			writeBulkString(conn, "")
			return
		}
		list := rPlush[key]
		if len(list) > 0 {
			value := list[0]
			rPlush[key] = list[1:]
			conn.Write([]byte("*2\r\n"))
			writeBulkString(conn, key)
			writeBulkString(conn, value)
			return
		}
	} else {
		select {
		case <-time.After(blocking.Timeout):
			mu.Lock()
			list := blockings[key]
			newList := []types.BlockingRequest{}
			for _, r := range list {
				if r.Ch != ch {
					newList = append(newList, r)
				}
			}
			blockings[key] = newList
			mu.Unlock()
			writeNull(conn)
			return
		case key := <-ch:
			list := rPlush[key]
			if len(list) > 0 {
				value := list[0]
				rPlush[key] = list[1:]
				conn.Write([]byte("*2\r\n"))
				writeBulkString(conn, key)
				writeBulkString(conn, value)
				return
			}
		}
	}
}



// Helpers

func parseArgs(conn net.Conn, reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" || !strings.HasPrefix(line, "*") {
		writeError(conn, "invalid format")
		return nil, errors.New("invalid format")
	}
	n := parseLength(line)
	args := []string{}
	for i := 0; i < n; i++ {
		_, err = reader.ReadString('\n') // skip $len
		if err != nil {
			return nil, err
		}
		arg, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		args = append(args, strings.TrimSpace(arg))
	}
	return args, nil
}

func parseLength(s string) int {
	var n int
	fmt.Sscanf(s, "*%d", &n)
	return n
}

func writeError(conn net.Conn, msg string) {
	conn.Write([]byte("-ERR " + msg + "\r\n"))
}

func writeSimpleString(conn net.Conn, msg string) {
	conn.Write([]byte("+" + msg + "\r\n"))
}

func writeBulkString(conn net.Conn, s string) {
	if s == "" {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
}


func writeInteger(conn net.Conn, n int) {
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
}

func writeNull(conn net.Conn) {
	conn.Write([]byte("$-1\r\n"))
}

func wakeUpFirstBlocking(key string) {
	if list, ok := blockings[key]; ok && len(list) > 0 {
		req := list[0]
		blockings[key] = list[1:]
		select {
		case req.Ch <- key:
		default:
		}
	}
}