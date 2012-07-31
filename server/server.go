package server

import (
	"net"
	"log"
	"os"
)

type CassBouncer struct {
	conn net.Conn
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
			log.Fatal("error accepting: ", err)
			return
		}
		bouncer := &CassBouncer{conn}
		bouncer.Run()
	}
}

func (b *CassBouncer) Run() {
	buf := make([]byte, 1024)
	n, err := b.conn.Read(buf)
	if err != nil {
		log.Fatal("error reading: ", err)
		return
	}
	log.Print("received ", n, " bytes of data --> ", string(buf))
	_, err = b.conn.Write(buf)
	if err != nil {
		log.Print("error sending reply: ", err)
	}
}