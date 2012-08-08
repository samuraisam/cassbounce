package server

import (
	"errors"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"io"
	"time"
	"log"
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
	return &CommandPacket{bytes, length, err}
}

func (c *CommandPacket) Bytes() []byte { return c.bytes }
func (c *CommandPacket) Len() int      { return c.length }
func (c *CommandPacket) Error() error  { return c.err }

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
	name            string
	typeId          thrift.TMessageType
	seqId           int32
	protocolFactory *thrift.TBinaryProtocolFactory
}

func NewCassandraCommand(name string, typeId thrift.TMessageType, seqId int32) *CassandraCommand {
	protoFac := thrift.NewTBinaryProtocolFactoryDefault()
	exc := &CassandraCommand{name, typeId, seqId, protoFac}
	return exc
}

func (ce *CassandraCommand) Name() string                { return ce.name }
func (ce *CassandraCommand) TypeId() thrift.TMessageType { return ce.typeId }
func (ce *CassandraCommand) SeqId() int32                { return ce.seqId }

/*
 * 1. Begin executing the configured command (name/typeid/seqid) on the upstream server (some random cassandra instance)
 * 2. Stream all remaining inbound data (the rest of the command from some client) to the upstream server
 * 3. Read the response header from the upstream server
 * 4. Read all remaining upstream data and write it to the client via the `inPackets` chan
 */
func (ce *CassandraCommand) Execute(inPackets <-chan *CommandPacket, outConn Connection, timeout time.Duration) (outPackets <-chan *CommandPacket) {
	inCh := make(chan *CommandPacket)
	go func() {
		log.Print("CassandraCommand:Execute executing ", ce.name)
		// get transport and protocol for the inbound connection
		inTrans := NewCommandPacketTTransport(inCh)
		inProt := ce.protocolFactory.GetProtocol(inTrans)

		// get transport and protocol for the upstream connection
		outTrans := outConn.Transport()
		outProt := outConn.ProtocolFactory().GetProtocol(outTrans)

		// write our inbound command header to the upstream connection
		headProtExc := outProt.WriteMessageBegin(ce.name, ce.typeId, ce.seqId)

		// respond to errors from writing the command header
		if headProtExc != nil {
			// headProtExc is a thrift.TTException
			log.Print("CassandraCommand:Execute error sending header to upsteram server: ", headProtExc)
			ce.writeError(inProt, headProtExc)
			close(inCh)
			return
		}

		// write all of our inbound packets to the upstream server
		for pkt := range inPackets {
			if pkt.Error() != nil {
				// if we got an error, then bail
				log.Print("CassandraCommand:Execute error reading data from client: ", pkt.Error())
				ce.writeError(inProt, thrift.NewTProtocolExceptionFromOsError(pkt.Error()))
				close(inCh)
				return
			}

			// pass on our inbound input to the upstream server
			wnWritten, wErr := outTrans.Write(pkt.Bytes())
			if wErr != nil {
				log.Print("CassandraCommand:Execute error writing ", wnWritten, " bytes  to upstream server: ", wErr)
				ce.writeError(inProt, thrift.NewTTransportExceptionFromOsError(wErr))
				close(inCh)
				return
			}
		}

		// start reading a connection, block until it's ready
		oName, oTypId, oSeqId, oErr := outProt.ReadMessageBegin()

		// respond to errors from reading the command header
		if oErr != nil {
			log.Print("CassandraCommand:Execute error reading response header from upstream server: ", oErr)
			ce.writeError(inProt, oErr)
			close(inCh)
			return
		}

		// write our header received from the upstream to the inbound connection
		iErr := inProt.WriteMessageBegin(oName, oTypId, oSeqId)
		if iErr != nil {
			// received an error writing response header, try and send an error
			log.Print("CassandraCommand:Execute error writing response header to client: ", iErr)
			ce.writeError(inProt, iErr)
			close(inCh)
			return
		}

		// read all data from the upstream server, streaming them to the inbound connection
		// note: at this point we kind of have our asses hanging out: we can not successfully send error header should we encounter 
		// an error reading, as we've already written the header. we'll still send an error just in case
		for pkt := range readGen(outTrans, "upstreamResp") {
			if pkt.Error() != nil {
				log.Print("CassandraCommand:Execute error writing response data to client: ", pkt.Error())
				ce.writeError(inProt, thrift.NewTProtocolExceptionFromOsError(pkt.Error()))
				close(inCh)
				return
			}
			inTrans.Write(pkt.Bytes()) // this will ultimately translate into inCh <-pkt
		}
	}()
	return inCh
}

func (ce *CassandraCommand) writeError(prot thrift.TProtocol, exc thrift.TException) {
	// see whihch of the known thrift exception types it was
	_, isProtExc := exc.(thrift.TProtocolException)
	appExc, isAppExc := exc.(thrift.TApplicationException)
	_, isTransExc := exc.(thrift.TTransportException)

	// write the exception header
	prot.WriteMessageBegin(ce.name, thrift.EXCEPTION, ce.seqId)

	// convert it into an application exception if not already
	if !isAppExc {
		if isProtExc {
			appExc = thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, exc.Error())
		} else if isTransExc {
			appExc = thrift.NewTApplicationException(thrift.INTERNAL_ERROR, exc.Error())
		} else {
			appExc = thrift.NewTApplicationExceptionMessage(exc.Error())
		}
	}

	appExc.Write(prot)

	// finish it 
	prot.WriteMessageEnd()
	prot.Transport().Flush()
}

/*
 * A TTransport used to just pass all data written to the outbound channel
 * 
 * This protocol should not be used to send any data to and from a client - it is explicitly for internal
 * communications only. 
 */
type CommandPacketTTransport struct {
	ch chan<- *CommandPacket // write only yo
}

func NewCommandPacketTTransport(ch chan<- *CommandPacket) *CommandPacketTTransport {
	return &CommandPacketTTransport{ch}
}

func (t *CommandPacketTTransport) IsOpen() bool { return true }
func (t *CommandPacketTTransport) Open() error  { return nil }
func (t *CommandPacketTTransport) Close() error { return nil }
func (t *CommandPacketTTransport) ReadAll(buf []byte) (int, error) {
	return 0, errors.New("Can not be read from.")
}
func (t *CommandPacketTTransport) Read(buf []byte) (int, error) {
	return 0, errors.New("Can not be read from.")
}
func (t *CommandPacketTTransport) Write(buf []byte) (int, error) {
	t.ch <-NewCommandPacket(buf, len(buf), nil)
	return len(buf), nil
}
func (t *CommandPacketTTransport) Flush() error { return nil }
func (t *CommandPacketTTransport) Peek() bool   { return false }

/*
 * Read all bytes from `reader` and pass them and any errors as *CommandPackets to a returned channel
 */
func readGen(reader io.Reader, name string) <-chan *CommandPacket { // TODO: add timeout
	ch := make(chan *CommandPacket)
	log.Print("XXX: readGen:", name, " creating!")
	go func() {
		b := make([]byte, 1024)
		totalBytesRead := 0
		for {
			n, err := reader.Read(b)
			var res []byte
			totalBytesRead += n
			if n > 0 {
				// yay, a response! copy it so the underlying array reference is not passed along
				res := make([]byte, n)
				copy(res, b[:n])
			} else {
				// nothing to see here
				// res := nil
			}
			doBreak := err != nil
			if err == io.EOF {
				log.Print("XXX: readGen:", name, " got EOF", err)
				err = nil // set err to nil, because we just want to close on EOF
			} else {
				log.Print("XXX: readGen:", name, " read ", totalBytesRead, " bytes")
				log.Print("XXX: readGen:", name, " did not get EOF: ", err)
			}
			// send the packet, with or without error - consumers should cease to read if an error is encountered
			ch <- NewCommandPacket(res, n, err)
			if doBreak {
				// exit when an error is encountered
				close(ch)
				break
			}
		}
	}()
	return ch
}
