package server

import (
	"bytes"
	"cassbounce/server/cassutils"
	"errors"
	"fmt"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"io"
	"log"
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
	SeqId() int32
	TypeId() thrift.TMessageType
	TokenHint() (*cassutils.Token, error)
	SetStream(inPackets chan *CommandPacket) error

	/* Execute the `Name()` method for `TypeId()` and `SeqId()` on a remote connection and return
	 * the results as a bytes buffer.
	 *
	 * The returned result is a channel down which the Execute function will stuff CommandPackets
	 * each of which may have its own error. 
	 */
	Execute(outConn Connection, timeout time.Duration) (outPackets <-chan *CommandPacket)
}

type CassandraCommand struct {
	name            string
	typeId          thrift.TMessageType
	seqId           int32
	protocolFactory *thrift.TBinaryProtocolFactory
	// didStealBytes     bool
	// stolenBytesTrans  *thrift.TTransport
	// stolenPacketsProt *thrift.TProtocol
	// stolenBytesCh     chan *CommandPacket
	streamWasSet bool
	inPackets    chan *CommandPacket
	argsProc     *cassutils.ArgsRetainingProcessor
}

func NewCassandraCommand(name string, typeId thrift.TMessageType, seqId int32) *CassandraCommand {
	protoFac := thrift.NewTBinaryProtocolFactoryDefault()
	exc := &CassandraCommand{name, typeId, seqId, protoFac, false, nil, nil}
	return exc
}

func (ce *CassandraCommand) Name() string                { return ce.name }
func (ce *CassandraCommand) TypeId() thrift.TMessageType { return ce.typeId }
func (ce *CassandraCommand) SeqId() int32                { return ce.seqId }

// read forward and determine if this command is something that will have a token
// eg. `get` or `batch_mutate` with some specific row key
func (ce *CassandraCommand) TokenHint() (*cassutils.Token, error) {
	if !ce.streamWasSet {
		return nil, errors.New("Can not get token hint if stream has not been set (nothing to read from)")
	}
	err := ce.captureArgsMessage()
	if err != nil {
		return nil, err
	}

	availableSingle := []string{"get", "get_slice", "get_count", "insert", "add", "remove", "remove_counter"}
	availableMulti := []string{"multiget_slice", "multiget_count"}

	contains := func(haystack []string, needle string) bool {
		for i := 0; i < len(haystack); i++ {
			if haystack[i] == needle {
				return true
			}
		}
		return false
	}

	switch {
	case contains(availableSingle, ce.name):
		// we will get the key
		k, kerr := ce.argsProc.GetArgBytes("key")
		if kerr != nil {
			return nil, kerr
		}
		return cassutils.NewToken(k), nil
		break
	case contains(availableMulti, ce.name):
		// we will get the *first* key
		//return cassutils.NewToken(ce.scrapeFirstKey(1))
		return nil, errors.New("Fart!")
		break
	default:
		break
	}
	return nil, errors.New(fmt.Sprintf("No token can be inferred from the command: ", ce.name))
}

// sets the stream
func (ce *CassandraCommand) SetStream(inPackets chan *CommandPacket) error {
	if ce.streamWasSet {
		return errors.New("Can not set stream twice")
	}
	ce.inPackets = inPackets
	ce.streamWasSet = true
	return nil
}

// Execute the current command on the given connection. If execution takes longer than `timeout` then
// the returned channel will be closed just after sending a CommandPacket contianing an error
func (ce *CassandraCommand) Execute(outConn Connection, timeout time.Duration) (outPackets <-chan *CommandPacket) {
	return ce.doExecute(outConn, timeout)
}

func (ce *CassandraCommand) captureArgsMessage() error {
	if !ce.streamWasSet {
		return errors.New("Can not capture args message if stream is not yet set.")
	}
	args, err := cassutils.NewArgsRetainingProcessor(ce.name)
	if err != nil {
		return err
	}
	inProt := ce.protocolFactory.GetProtocol(NewCommandPacketTTransport(ce.inPackets))
	success, err := args.ReadArgs(inProt)
	if success {
		ce.argsProc = args
	}
	return err
}

func (ce *CassandraCommand) writeArgsMessage(oprot thrift.TProtocol) thrift.TException {
	success, texec, err := ce.argsProc.WriteArgs(oprot)
	if success {
		return nil
	}
	if texec != nil {
		return texec
	}
	return thrift.NewTApplicationExceptionMessage(fmt.Sprintf("%s", err))
}

func (ce *CassandraCommand) doExecute(outConn Connection, timeout time.Duration) (outPackets <-chan *CommandPacket) {
	inCh := make(chan *CommandPacket)
	go func() {
		defer close(inCh)
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
			return
		}

		if ce.argsProc != nil && ce.argsProc.GotArgs() {
			// if we already read the args and processed it, write the processed args to the upstream server
			writeExc := ce.writeArgsMessage(outProt)
			outProt.WriteMessageEnd()
			if writeExc != nil {
				log.Print("CassandraCommand:Execute error writing processed args: ", writeExc)
				ce.writeError(inProt, writeExc)
				return
			}
		} else {
			// otherwise just write all inbound packets to the upstream server
			for pkt := range ce.inPackets {
				if pkt.Error() != nil {
					// if we got an error, then bail
					log.Print("CassandraCommand:Execute error reading data from client: ", pkt.Error())
					ce.writeError(inProt, thrift.NewTProtocolExceptionFromOsError(pkt.Error()))
					return
				}

				// pass on our inbound input to the upstream server
				wnWritten, wErr := outTrans.Write(pkt.Bytes())
				if wErr != nil {
					log.Print("CassandraCommand:Execute error writing ", wnWritten, " bytes  to upstream server: ", wErr)
					ce.writeError(inProt, thrift.NewTTransportExceptionFromOsError(wErr))
					return
				}
			}
		}

		outTrans.Flush() // get rid of the command

		// start reading a connection, block until it's ready
		oName, oTypId, oSeqId, oErr := outProt.ReadMessageBegin()

		// respond to errors from reading the command header
		if oErr != nil {
			log.Print("CassandraCommand:Execute error reading response header from upstream server: ", oErr)
			ce.writeError(inProt, oErr)
			return
		}

		// write our header received from the upstream to the inbound connection
		iErr := inProt.WriteMessageBegin(oName, oTypId, oSeqId)
		if iErr != nil {
			// received an error writing response header, try and send an error
			log.Print("CassandraCommand:Execute error writing response header to client: ", iErr)
			ce.writeError(inProt, iErr)
			return
		}

		// read all data from the upstream server, streaming them to the inbound connection
		// note: at this point we kind of have our asses hanging out: we can not successfully send error header should we encounter 
		// an error reading, as we've already written the header. we'll still send an error just in case
		for pkt := range TTransportReadGen(outTrans, "upstreamResp") {
			if pkt.Error() != nil {
				log.Print("CassandraCommand:Execute error writing response data to client: ", pkt.Error())
				ce.writeError(inProt, thrift.NewTProtocolExceptionFromOsError(pkt.Error()))
				return
			}
			_, writeErr := inProt.Transport().Write(pkt.Bytes()) // this will ultimately translate into inCh <-pkt
			if writeErr != nil {
				log.Print("CassandraCommand:Execute error writing back to client protocol: ", writeErr)
			}
		}
	}()
	return inCh
}

func (ce *CassandraCommand) writeError(prot thrift.TProtocol, exc thrift.TException) {
	TWriteException(ce.name, ce.seqId, prot, exc)
}

/*
A TTransport used to just pass all data written to the outbound channel

This protocol should not be used to send any data to and from a client - it is explicitly for internal
communications only. 
*/
type CommandPacketTTransport struct {
	ch         chan *CommandPacket
	readBuffer *bytes.Buffer
}

func NewCommandPacketTTransport(ch chan *CommandPacket) *CommandPacketTTransport {
	return &CommandPacketTTransport{ch, &bytes.Buffer{}}
}

func (t *CommandPacketTTransport) IsOpen() bool { return true }
func (t *CommandPacketTTransport) Open() error  { return nil }
func (t *CommandPacketTTransport) Close() error { return nil }
func (t *CommandPacketTTransport) Read(buf []byte) (int, error) {
	if t.readBuffer.Len() > 0 {
		got, err := t.readBuffer.Read(buf)
		if got > 0 {
			return got, err
		}
	}
	if cp, ok := <-t.ch; !ok {
		t.readBuffer = &bytes.Buffer{}
	} else {
		t.readBuffer = bytes.NewBuffer(cp.Bytes())
	}
	got, err := t.readBuffer.Read(buf)
	return got, err
}

func (t *CommandPacketTTransport) ReadAll(buf []byte) (int, error) {
	return thrift.ReadAllTransport(t, buf)
}
func (t *CommandPacketTTransport) Write(buf []byte) (int, error) {
	t.ch <- NewCommandPacket(buf, len(buf), nil)
	return len(buf), nil
}
func (t *CommandPacketTTransport) Flush() error { return nil }
func (t *CommandPacketTTransport) Peek() bool   { return false }

/*
 * Read all bytes from `reader` and pass them and any errors as *CommandPackets to a returned channel
 */
func TTransportReadGen(reader io.Reader, name string) chan *CommandPacket { // TODO: add timeout
	ch := make(chan *CommandPacket)
	go func() {
		b := make([]byte, 1024)
		totalBytesRead := 0
		for {
			n, err := reader.Read(b)
			res := make([]byte, n)
			doBreak := err != nil
			totalBytesRead += n
			log.Print("XXX: transportreadgen: ", name, " ", n, totalBytesRead, " err: ", err)
			if n > 0 {
				// yay, a response! copy it so the underlying array reference is not passed along
				copy(res, b[:n])
				idx := len(res) - 1
				lastIs := thrift.TType(res[idx])
				//log.Print("XXX: contains ", bytes.Contains(res, []byte{0}))
				//log.Print("XXX: lastis ", lastIs, " ", string(res[idx]))
				if lastIs == thrift.STOP {
					doBreak = true
				}
				// check if thrift is done sending us data
				//if thrift.TType(b[len(b)-1]).Compare(thrift.STOP) {
				//	doBreak = true // thrift wants us to bail
				//}
			} else {
				doBreak = true
			}
			// send the packet, with or without error - consumers should cease to read if an error is encountered

			ch <- NewCommandPacket(res, n, err)
			if doBreak {
				if n == len(b) {
					// we read to the end of the buffer, try reading another
					continue
				}
				// exit when an error is encountered
				close(ch)
				break
			}
		}
	}()
	return ch
}

/*
 * Write the exception to the protocol.
 *
 * Does not assume the protocol's transport is ready for writing (you must prepare this yourself.)
 * Begins writing, writes the exception, and flushes the protocol's transport.
 */
func TWriteException(name string, seqId int32, prot thrift.TProtocol, exc thrift.TException) {
	// see whihch of the known thrift exception types it was
	_, isProtExc := exc.(thrift.TProtocolException)
	appExc, isAppExc := exc.(thrift.TApplicationException)
	_, isTransExc := exc.(thrift.TTransportException)

	// write the exception header
	prot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)

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
