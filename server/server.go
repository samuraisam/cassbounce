package server

import (
	"net"
	"log"
	"os"
	// "buffer"
)

type CassBouncer struct {
	conn net.Conn
	cassConn *CassConnection
}

func Listen(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("error listening: ", err)
		os.Exit(1)
	}
	log.Print("listening on ", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print("error accepting: ", err)
			return
		}
		log.Print("got client: ", conn)
		go func() {
			cassConn, err := Dial("0.0.0.0:9160", "farty", 1000)
			if err != nil {
				log.Print("could not connect to cassandra node: ", err)
				conn.Close()
				return
			}
			bouncer := &CassBouncer{conn, cassConn}
			bouncer.Run()
		}()
	}
}

func (b *CassBouncer) Run() {
	defer func() {
		b.conn.Close()
		b.cassConn.Close()
	}()

	dead := false

	for !dead {
		readBuf := make([]byte, 1024)
		n, err := b.conn.Read(readBuf)
		if err != nil {
			log.Print("error reading from client: ", err)
			return
		}
		log.Print("received ", n, " bytes of data")

		// var packet Buffer
		// packet.Write()

		n, cassWriteErr := b.cassConn.Write(readBuf[:n])
		if cassWriteErr != nil {
			log.Print("error writing to cassandra: ", cassWriteErr)
			return
		}
		log.Print("wrote ", n, " bytes of data to cassandra")

		cassReadBuf := make([]byte, 1024)
		n, cassReadErr := b.cassConn.Read(cassReadBuf)
		if cassReadErr != nil {
			log.Print("error reading from cassandra: ", cassReadErr)
			return
		}
		log.Print("read ", n, " bytes from cassandra")

		n, clientWriteErr := b.conn.Write(cassReadBuf[:n])
		if clientWriteErr != nil {
			log.Print("error sending reply: ", clientWriteErr)
		}
		log.Print("wrote ", n, " bytes of data")
	}
}