package cassandra

// Token is some token on a ring
type Token struct {
	key []byte
	tok int64
}

// Create a new Token - tokens are immutable so you must use this method.
func NewToken(key []byte, tok int64) *Token {
	return &Token{key, tok};
}

// Get the Token on the ring
func (t *Token) Token() int64 { return t.tok }
func (t *Token) Key() []byte { return t.key }