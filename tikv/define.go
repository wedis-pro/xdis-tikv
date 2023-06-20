package tikv

import (
	"errors"

	"github.com/tikv/client-go/v2/rawkv"
)

var (
	ErrValueIntOutOfRange = errors.New("ERR value is not an integer or out of range")
	ErrKeyExist           = errors.New("ERR key exist")
	ErrCASExhausted       = errors.New("ERR compare-and-swap exhausted")
	ErrCAS       = errors.New("ERR compare-and-swap")
)

var (
	MaxRawKVScanLimit = rawkv.MaxRawKVScanLimit
)

const (
	// for incr
	MaxCASLoopCn = 2000
)
