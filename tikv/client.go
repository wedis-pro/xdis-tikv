package tikv

import (
	"context"

	"github.com/weedge/xdis-tikv/config"
)

type Client struct {
	opts *config.TikvClientOptions
}

func NewClient(opts *config.TikvClientOptions) (client *Client, err error) {
	return
}

func (m *Client) Get(ctx context.Context, key []byte) (val []byte, err error) {
	return
}
func (m *Client) BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error)

func (m *Client) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	return nil
}
func (m *Client) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error

func (m *Client) Delete(ctx context.Context, key []byte) error
func (m *Client) BatchDelete(ctx context.Context, keys [][]byte) error

func (m *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

func (m *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

func (m *Client) Close() (err error) {
	return
}
