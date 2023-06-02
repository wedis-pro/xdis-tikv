package driver

import "golang.org/x/net/context"

type IKV interface {
	Get(ctx context.Context, key []byte) (val []byte, err error)
	BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error)

	PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error
	BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error

	Delete(ctx context.Context, key []byte) error
	BatchDelete(ctx context.Context, keys [][]byte) error

	Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

	ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

	Close() error
}
