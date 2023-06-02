package tikv

import (
	"context"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/weedge/xdis-tikv/config"
)

type RawKVClientWrapper struct {
	client  *rawkv.Client
	retries int
}

func NewRawKV(opt *config.TikvClientOptions)

func (m *RawKVClientWrapper) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	return nil
}
