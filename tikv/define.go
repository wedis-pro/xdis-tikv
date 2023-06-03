package tikv

import "errors"

var (
	ErrKeyExist     = errors.New("key exist")
	ErrCASExhausted = errors.New("compare-and-swap exhausted")
)

const (
	// for incr
	MaxCASLoopCn = 2000
)
