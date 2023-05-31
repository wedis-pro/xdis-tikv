package tikv

import (
	"github.com/tikv/client-go/v2/rawkv"
)

type RawKVClientWrapper struct {
	client  *rawkv.Client
	retries int
}
