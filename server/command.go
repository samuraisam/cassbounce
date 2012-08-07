package server

import (
	"bytes"
	"io"
	"time"
)

/*
 * CommandPacket - a serving a byes, with a side of error. Immutable.
 */
type CommandPacket struct {
	bytes  []byte
	length int
	err    error
}

func NewCommandPacket(bytes []byte, length int, err error) *CommandPacket {
	return &CommandPacket{bytes, length, error}
}

func (c *CommandPacket) Bytes() { return c.bytes }
func (c *CommandPacket) Len()   { return c.length }
func (c *CommandPacket) Error() { return c.err }

/*
 * Command - wraps the header and value of a thrift command
 * 
 * Attempts to do no decoding of the command beyond the header (which is already decoded)
 */
type Command interface {
	Name() string
	SeqId() int64
	TypeId() int32

	/* Execute the `Name()` method for `TypeId()` and `SeqId()` on a remote connection and return
	 * the results as a bytes buffer.
	 *
	 * The returned result is a channel down which the Execute function will stuff CommandPackets
	 * each of which may have its own error. 
	 */
	Execute(inPackets chan<- *CommandPacket, conn Connection, timeout time.Duration) (outPackets chan<- *CommandPacket)
}

type CassandraCommand struct {
	name   string
	seqId  int64
	typeId int32
}

func NewCassandraCommand(name, seqId, typeId) *CassandraCommand {
	exc := &CassandraCommand{name, seqId, typeId}
	return exc
}

func (ce *CassandraCommand) Name() string  { return ce.name }
func (ce *CassandraCommand) SeqId() int32  { return ce.seqId }
func (ce *CassandraCommand) TypeId() int64 { return ce.typeId }

func (ce *CassandraCommand) Execute(inPackets <-chan *CommandPacket, outConn Connection, timeout time.Duration) (outPackets <-chan *CommandPacket) {
	out := make(chan *CommandPacket)
	go func() {
		// get a protocol and write
		outTrans := NewCommandPacketTTransport(out)
		inProtocol := outConn.ProtocolFactory().GetProtocol(outConn.Transport())
		headProtExc := protocol.WriteMessageBegin(c.name, c.typeId, c.seqId)
		if headProtExc != nil {
			// could not read the header, so just write whatever error was encountered back and gtfo
			e.writeError(protocol, headProtExc)
			close(out)
			return
		}

	}()
	return out
}

func (ce *CassandraCommand) writeError(prot TProtocol, exc TException) {
	prot.WriteMessageBegin(ce.name, thrift.EXCEPTION, ce.seqId)
	exc.Write(prot)
	prot.WritesMessageEnd()
	prot.Transport().Flush()
}

/*
 * A TTransport used to just pass all data written to the outbound channel
 * 
 * This protocol should not be used to send any data to and from a client - it is explicitly for internal
 * communications only. 
 */
type CommandPacketTTransport struct {
	ch <-chan *CommandPacket
}

func NewCommandPacketTTransport(ch <-chan *CommandPacket) *CommandPacketTTransport {
	return &CommandPacketTTransport{ch}
}

func (t *PassthroughTransport) IsOpen() bool { return true }
func (t *PassthroughTransport) Open() error  { return nil }
func (t *PassthroughTransport) Close() error { return nil }
func (t *PassthroughTransport) ReadAll(buf []byte) (int, error) {
	return 0, errors.Error("Can not be read from.")
}
func (t *PassthroughTransport) Read(buf []byte) (int, error) {
	return 0, errors.Error("Can not be read from.")
}
func (t *PassthroughTransport) Write(buf []byte) (int, error) {
	t.ch <- NewCommandPacket(buf, len(buf), nil)
	return len(buf), nil
}
func (t *PassthroughTransport) Flush() error { return nil }
func (t *PassthroughTransport) Peek() bool   { return false }

/*
 * Read all bytes from `reader` and pass them and any errors as *CommandPackets to a returned channel
 */
func readGen(reader io.Reader) <-chan *CommandPacket {
	ch := make(chan *CommandPacket)
	go func() {
		b := make([]byte, 1024)
		for {
			n, err := reader.Read(b)
			var res []byte
			if n > 0 {
				// yay, a response! copy it so the underlying array reference is not passed along
				res := make([]byte, n)
				copy(res, b[:n])
			} else {
				// nothing to see here
				res := nil
			}
			// send the packet, with or without error - consumers should cease to read if an error is encountered
			c <- NewCommandPacket(res, n, err)
			if err != nil {
				// exit when an error is encountered
				break
			}
		}
	}()
	return ch
}
