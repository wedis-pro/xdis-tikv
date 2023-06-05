package tikv

import (
	"errors"

	"github.com/tikv/client-go/v2/rawkv"
)

var (
	ErrKeyExist     = errors.New("key exist")
	ErrCASExhausted = errors.New("compare-and-swap exhausted")
)

var (
	MaxRawKVScanLimit = rawkv.MaxRawKVScanLimit
)

const (
	// for incr
	MaxCASLoopCn = 2000
)
