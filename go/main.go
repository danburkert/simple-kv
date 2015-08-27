package main

import "bufio"
import "log"
import "net"
import "strings"
import "fmt"
import "sync"

type DB struct {
	mu      sync.Mutex
	entries map[string]string
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.entries[key]
	return val, ok
}

func (db *DB) Put(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.entries[key] = value
}

func main() {
	log.Printf("Starting simple-kv Go server with listening port 5556")
	ln, err := net.Listen("tcp", ":5556")
	if err != nil {
		log.Fatal(err)
	}
	db := &DB{
		entries: make(map[string]string),
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("unable to accept connection: %s", err)
		} else {
			go handleConnection(conn, db)
		}
	}
}

func handleConnection(conn net.Conn, db *DB) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		words := strings.Split(scanner.Text(), " ")

		if len(words) < 2 {
			fmt.Fprintf(conn, "ERR\n")
			continue
		}

		command, args := words[0], words[1:]

		switch command {
		case "GET":
			if len(args) != 1 {
				fmt.Fprintf(conn, "ERR\n")
				continue
			}
			val, ok := db.Get(args[0])
			if ok {
				fmt.Fprintf(conn, "%s\n", val)
			} else {
				fmt.Fprintf(conn, "NONE\n")
			}
		case "PUT":
			if len(args) != 2 {
				fmt.Fprintf(conn, "ERR\n")
				continue
			}
			db.Put(args[0], args[1])
			fmt.Fprintf(conn, "OK\n")
		default:
			fmt.Fprintf(conn, "ERR\n")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
}
