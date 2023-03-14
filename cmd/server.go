package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	simplekv "github.com/SENC275/simplekv"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = "8888"
	CONN_TYPE = "tcp"

	DBFolder = "simplekvstore"
)

const (
	PUT = "PUT"
	GET = "GET"
)

var db *simplekv.SimpleKV

var (
	ErrArgsNumberNotMatch = errors.New("the number of arguments not match")
	ErrNotSupportedCmd    = errors.New("the command is not supported")
)

func init() {
	var err error
	db, err = simplekv.Open(DBFolder)
	if err != nil {
		panic(err)
	}
}

func main() {
	listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-sig
		os.Exit(1)
	}()

	defer listener.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		cmds := strings.Split(string(buf[:n]), " ")

		var response string
		response, err = ExecuteCmds(cmds)

		if err != nil {
			response = "ERROR:" + err.Error()
		} else {
			response = fmt.Sprintf("%s", string(response))
		}

		conn.Write([]byte(response))
	}
}

func ExecuteCmds(cmds []string) (string, error) {
	var result string
	var err error

	switch strings.ToUpper(cmds[0]) {
	case PUT:
		if err = check(cmds[1:], 2); err != nil {
			return "", err
		}
		db.Put(cmds[1], cmds[2])
	case GET:
		if err = check(cmds[1:], 1); err != nil {
			return "", err
		}
		result, err = db.Get(cmds[1])
	default:
		err = ErrNotSupportedCmd
	}
	return result, err
}

func check(args []string, num int) error {
	if len(args) != num {
		return ErrArgsNumberNotMatch
	}
	return nil
}
