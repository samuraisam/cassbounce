package cassutils

import (
	"math/big"
	"crypto/md5"
	"fmt"
)

// Token is some token on a ring
type Token struct {
	key []byte
	tok *big.Int
	hcache []byte
}

// Create a new Token - tokens are immutable so you must use this method.
func NewToken(key []byte) *Token {
	ret := &Token{key, nil, nil}
	i := big.NewInt(0)
	i.SetBytes(ret.Hash()) // also calculates and caches the hash
	ret.tok = i
	return ret
}

// Get the Token on the ring
func (t *Token) Token() *big.Int { return t.tok }

// Get the key that this Token was created with
func (t *Token) Key() []byte { return t.key }

// Get the hashed version of the key - same a org.apache.cassandra.dht.RandomPartitioner would do
// - RandomPartitioner uses the MD5 of the bytes of the key to place the token somewhere on the ring
func (t *Token) Hash() []byte {
	if t.hcache == nil { // cache the value of this
		h := md5.New()
		// io.WriteBytes(h, t.key)
		h.Write(t.key)
		t.hcache = h.Sum(nil)
	}
	return t.hcache
}

func (t *Token) String() string {
	return fmt.Sprintf("<Token: %x>", t.Hash())
}
