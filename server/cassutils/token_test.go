package cassutils

import (
	"testing"
	"fmt"
)

func Test_Token_Basic(t *testing.T) {
	b := []byte("fartymcfartyfart")
	tok := NewToken(b)
	t.Log(fmt.Sprintf("token bigint: %s", tok.Token()))
	t.Log(fmt.Sprintf("token stringify: %s", tok))
	t.Log(fmt.Sprintf("token hash: %x", tok.Hash()))
	t.Log(fmt.Sprintf("token key (to string): %s", string(tok.Key())))
}
