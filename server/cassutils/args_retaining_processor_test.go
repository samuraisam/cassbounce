package cassutils

import (
	"testing"
	"github.com/carloscm/gossie/src/cassandra"
	"reflect"
)

func Test_CassThing(t *testing.T) {
	gargs := cassandra.NewGetArgs()
	t.Log("shit: ", reflect.TypeOf(gargs))
}

func Test_ConstructorMap(t *testing.T) {
	fnMap := make(map[string]func()interface{})
	fnMap["get"] = func() interface{} { return cassandra.NewGetArgs() }

	rv := fnMap["get"]()
	t.Log("rv: ", reflect.TypeOf(rv))

	tt := reflect.TypeOf(rv)
	rm, valid := tt.MethodByName("Read")
	if valid {
		t.Log("rm: ", rm)
		v := rm.Func
		t.Log("fun: ", v)
	}
}

// func Test_PutConstructor(t *testing.T) {
// 	argsretaining := NewArgsRetainingProcessor()
// 	argsretaining.PutConstructor("get", cassandra.NewGetArgs)
// }

// func Test_GetConstructor(t *testing.T) {
// 	a := NewArgsRetainingProcessor()
// 	a.PutConstructor("get", cassandra.NewGetArgs)

// 	derp := a.GetConstructor("get").(func()interface{})
// 	// t.Log("succ: ", succ)

// 	fart := derp()
// 	t.Log("fart: ", fart)
// }