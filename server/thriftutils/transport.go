package thriftutils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
)

// An alternate TFramedTrasport that does not puke on over-reads. Specifically, it is more lenient
// when trying to read 4-byte frame headers.
// 
// this is mostly lifted from github.com/pomack/thrift4go's implementation of TFramedTransport
type TFramedTransport struct {
	transport   thrift.TTransport
	writeBuffer *bytes.Buffer
	readBuffer  *bytes.Buffer
}

type tFramedTransportFactory struct {
	factory thrift.TTransportFactory
}

func NewTFramedTransportFactory(factory thrift.TTransportFactory) thrift.TTransportFactory {
	return &tFramedTransportFactory{factory: factory}
}

func (p *tFramedTransportFactory) GetTransport(base thrift.TTransport) thrift.TTransport {
	return NewTFramedTransport(p.factory.GetTransport(base))
}

func NewTFramedTransport(transport thrift.TTransport) *TFramedTransport {
	writeBuf := make([]byte, 0, 1024)
	readBuf := make([]byte, 0, 1024)
	return &TFramedTransport{transport: transport, writeBuffer: bytes.NewBuffer(writeBuf), readBuffer: bytes.NewBuffer(readBuf)}
}

func (p *TFramedTransport) Open() error {
	return p.transport.Open()
}

func (p *TFramedTransport) IsOpen() bool {
	return p.transport.IsOpen()
}

func (p *TFramedTransport) Peek() bool {
	return p.transport.Peek()
}

func (p *TFramedTransport) Close() error {
	return p.transport.Close()
}

func (p *TFramedTransport) Read(buf []byte) (int, error) {
	if p.readBuffer.Len() > 0 {
		got, err := p.readBuffer.Read(buf)
		if got > 0 {
			return got, thrift.NewTTransportExceptionFromOsError(err)
		}
	}

	// Read another frame of data
	f, e := p.readFrame()
	if e != nil {
		return f, e
	}
	if f == 0 { // no more to read right now
		return 0, nil
	}

	got, err := p.readBuffer.Read(buf)
	return got, thrift.NewTTransportExceptionFromOsError(err)
}

func (p *TFramedTransport) ReadAll(buf []byte) (int, error) {
	return thrift.ReadAllTransport(p, buf)
}

func (p *TFramedTransport) Write(buf []byte) (int, error) {
	n, err := p.writeBuffer.Write(buf)
	return n, thrift.NewTTransportExceptionFromOsError(err)
}

func (p *TFramedTransport) Flush() error {
	size := p.writeBuffer.Len()
	buf := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(buf, uint32(size))
	_, err := p.transport.Write(buf)
	if err != nil {
		return thrift.NewTTransportExceptionFromOsError(err)
	}
	if size > 0 {
		n, err := p.writeBuffer.WriteTo(p.transport)
		if err != nil {
			println("Error while flushing write buffer of size ", size, " to transport, only wrote ", n, " bytes: ", err.Error(), "\n")
			return thrift.NewTTransportExceptionFromOsError(err)
		}
	}
	err = p.transport.Flush()
	return thrift.NewTTransportExceptionFromOsError(err)
}

func (p *TFramedTransport) readFrame() (int, error) {
	buf := []byte{0, 0, 0, 0}
	x, err := p.transport.Read(buf)
	//_, err := p.transport.ReadAll(buf)

	// if err != nil || x != 4 {
	//   fmt.Println("ERRRRRR ", err, " len: ", x)
	//   return 0, err
	// }
	if x == 0 {
		fmt.Println("read incorrect size:", x, "!=", 4)
		return 0, nil //errors.New("Read incorrect frame size.") -- no more to read
	}
	if err != nil {
		fmt.Println("err reading from socket: ", err)
		return 0, err
	}

	size := int(binary.BigEndian.Uint32(buf))
	if size < 0 {
		// TODO(pomack) log error
		return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, "Read a negative frame size ("+string(size)+")")
	}
	if size == 0 {
		return 0, nil
	}
	buf2 := make([]byte, size)
	n, err := p.transport.ReadAll(buf2)
	if err != nil {
		return n, err
	}
	p.readBuffer = bytes.NewBuffer(buf2)
	return size, nil
}
