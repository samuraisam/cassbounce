package cassutils

import (
	"errors"
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"reflect"
)

var (
	// A generalized map of constructors that build argument structs for the cassandra thrift bindings eg NewGetArgs
	// it maps `"thrift_name"=>func() interface{}`
	constructorMap map[string]func() interface{}

	// Returned by NewArgsRetainingProcessor when trying to initialize it for an unknown constructor
	ErrUnknownArgsConstructor = errors.New("That args constructor is not known.")

	// Returned by methods on ArgsRetainingProcessor that expect args but it doesn't have them yet
	ErrNoArgs = errors.New("Need to have successfully executed ReadArgs before getting here")

	// Happens when asking for a field that could not be found
	ErrUnknownField = errors.New("Unknown field name")

	// Happens when asking for a field but using the wrong getter (wrong type)
	ErrWrongType = errors.New("Wrong type for field name")
)

func init() {
	constructorMap = make(map[string]func() interface{})
	constructorMap["get"] = func() interface{} { return cassandra.NewGetArgs() }
	constructorMap["get_slice"] = func() interface{} { return cassandra.NewGetSliceArgs() }
}

type ArgsRetainingProcessor struct {
	name         string
	constructor  func() interface{}
	gotArgs      bool
	args         interface{}
	argsTypCache reflect.Type
}

func NewArgsRetainingProcessor(name string) (*ArgsRetainingProcessor, error) {
	f, exists := constructorMap[name]
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
		p.argsTypCache = reflect.TypeOf(p.args).Elem()
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
		log.Print("ArgsRetainingProcessor:WriteArgs invalid method by name: `Write`")
		return
	}
	writeArgs := []reflect.Value{reflect.ValueOf(p.args), reflect.ValueOf(oprot)}
	ret := meth.Func.Call(writeArgs)
	log.Print("XXX: args Write ret: ", ret)
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

// Get the value of a named argument from the thrift args. takes the name of the thrift field,
// not the name of the generated field
func (p *ArgsRetainingProcessor) GetArgBytes(name string) ([]byte, error) {
	if !p.gotArgs {
		return nil, ErrNoArgs
	}
	argTyp := p.argsTyp().Elem() // turn type from "pointer to struct" into "struct"
	nFields := argTyp.NumField()
	var fieldIdx int
	var field reflect.StructField
	found := false

	// get the index of the field asked for by looking at the tag for each member of the struct
	for fieldIdx = 0; fieldIdx < nFields; fieldIdx++ {
		field = argTyp.Field(fieldIdx)
		if string(field.Tag) == name { // thrift structures are tagged with their thrift field name
			found = true
			break
		}
	}
	if !found {
		return nil, ErrUnknownField
	}
	// get the value of the field and try to turn it into []byte
	argsVal := reflect.ValueOf(p.args).Elem()
	val := argsVal.Field(fieldIdx)
	if !val.CanInterface() {
		return nil, ErrWrongType
	}
	valFace := val.Interface()
	valConcrete, ok := valFace.([]byte)
	if !ok {
		return nil, ErrWrongType
	}
	return valConcrete, nil
}
