package tikv

import (
	"context"
	"strings"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/weedge/xdis-tikv/v1/config"
)

type RawKVClientWrapper struct {
	client  *rawkv.Client
	retries int
}

func NewRawKVClient(opts *config.TikvClientOptions) (*RawKVClientWrapper, error) {
	rawCli, err := rawkv.NewClientWithOpts(context.Background(), strings.Split(opts.PDAddrs, ","))
	if err != nil {
		return nil, err
	}

	cli := &RawKVClientWrapper{
		client:  rawCli,
		retries: opts.TxnRetryCn,
	}

	return cli, nil
}

func (m *RawKVClientWrapper) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	err := m.client.PutWithTTL(ctx, key, value, ttl)
	return err
}
func (m *RawKVClientWrapper) PutNotExists(ctx context.Context, key, value []byte) error {
	return nil
}

func (m *RawKVClientWrapper) Get(ctx context.Context, key []byte) (val []byte, err error)
func (m *RawKVClientWrapper) BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error)
func (m *RawKVClientWrapper) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error
func (m *RawKVClientWrapper) Delete(ctx context.Context, key []byte) error
func (m *RawKVClientWrapper) BatchDelete(ctx context.Context, keys [][]byte) error
func (m *RawKVClientWrapper) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *RawKVClientWrapper) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *RawKVClientWrapper) Close() error
