package cassutils

import (
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"errors"
	"reflect"
	"log"
)

var (
	// A generalized map of constructors that build argument structs for the cassandra thrift bindings eg NewGetArgs
	// it maps `"thrift_name"=>func() interface{}`
	ConstructorMap map[string]func()interface{}
	// Returned by NewArgsRetainingProcessor when trying to initialize it for an unknown constructor
	ErrUnknownArgsConstructor = errors.New("That args constructor is not known.")
	// Returned by methods on ArgsRetainingProcessor that expect args but it doesn't have them yet
	ErrNoArgs = errors.New("Need to have successfully executed ReadArgs before getting here")
)

func init() {
	ConstructorMap = make(map[string]func()interface{})
	ConstructorMap["get"] = func() interface{} { return cassandra.NewGetArgs() }
	ConstructorMap["get_slice"] = func() interface{} { return cassandra.NewGetSliceArgs() }
}

type ArgsRetainingProcessor struct {
	name string
	constructor func() interface{}
	gotArgs bool
	args interface{}
	argsTypCache reflect.Type
}

func NewArgsRetainingProcessor(name string) (*ArgsRetainingProcessor, error) {
	f, exists := ConstructorMap[name]
	if !exists {
		return nil, ErrUnknownArgsConstructor
	}
	return &ArgsRetainingProcessor{name, f, false, nil, nil}, nil
}

// Process the arguments of a thrift call off the TProtocol and store it for future access
// NOTE: this actually reads from the inbound protocol, so you will have to call the WriteArgs(TProtocol) method to
// write those back to an upstream server or whatever those args were originally meant for
func (p *ArgsRetainingProcessor) ReadArgs(iprot thrift.TProtocol) (success bool, err thrift.TException) {
	// ok, so this may look a little silly/stupid/insane, but, we're doing it this way (using reflect) to
	// generalize over the generated thrift bindings, which are pretty rigid and offer little extensibility

	// run the constructor which will return for us an interface{}
	args := p.constructor()
	// reflect the type of the interface{} (which is some args instance)
	typ := reflect.TypeOf(args)
	// get its "Read" method
	meth, isValid := typ.MethodByName("Read")
	if !isValid {
		// should never fail, unless the thrift bindings have changed drastically
		log.Print("ArgsRetainingProcessor:ReadArgs error getting valid Read method from ", typ)
		return
	}
	// build args for the "Read" method
	argsArgs := []reflect.Value{reflect.ValueOf(args), reflect.ValueOf(iprot)}
	// call that "Read" method and get its first return value (Read only returns one Value)
	ret := meth.Func.Call(argsArgs)
	// if it is non-nil, then it is (should be) some TProtocolException
	if !ret[0].IsNil() { // there was an error reading the arguments
		// try casting it to the actual TProtocolException
		concreteExc, ok := ret[0].Interface().(thrift.TProtocolException)
		if !ok {
			// should never fail... unless the thrift bindings have changed drastically
			log.Print("ArgsRetainingProcessor:ReadArgs error getting valid TProtocolException from Read method")
			return
		}
		err = concreteExc
	} else {
		p.gotArgs = true
		p.argsTypCache = typ
		p.args = args
	}
	return p.gotArgs, err
}

func (p *ArgsRetainingProcessor) argsTyp() reflect.Type {
	if p.argsTypCache == nil {
		p.argsTypCache = reflect.TypeOf(p.args)
	}
	return p.argsTypCache
}

// Write the arguments that were sniffed using ReadArgs back to the provided thrift.TProtocol
func (p *ArgsRetainingProcessor) WriteArgs(oprot thrift.TProtocol) (success bool, exc thrift.TException, err error) {
	if !p.gotArgs {
		err = ErrNoArgs
		return
	}
	// you saw this up in ReadArgs, first, get the type
	typ := p.argsTyp()
	meth, isValid := typ.MethodByName("Write")
	if !isValid {
		log.Print("ArgsRetainingProcessor:WriteArgs")
		return
	}
	writeArgs := []reflect.Value{reflect.ValueOf(p.args), reflect.ValueOf(oprot)}
	ret := meth.Func.Call(writeArgs)
	if !ret[0].IsNil() {
		// try casting it to the actual TProtocolException
		concreteExc, ok := ret[0].Interface().(thrift.TProtocolException)
		if !ok {
			// should never fail... unless the thrift bindings have changed drastically
			log.Print("ArgsRetainingProcessor:writeArgs error getting valid TProtocolException from Write method")
			return
		}
		exc = concreteExc
		return
	}
	success = true
	return
}

// Get a value from the processed args

