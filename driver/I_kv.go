package driver

import "context"

type IKV interface {
	Get(ctx context.Context, key []byte) (val []byte, err error)
	BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error)

	PutNotExists(ctx context.Context, key, value []byte) error
	PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error
	BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error

	Delete(ctx context.Context, key []byte) error
	BatchDelete(ctx context.Context, keys [][]byte) error

	// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
	// The returned keys are in lexicographical order.
	// If endKey is empty, it means unbounded.
	// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
	// (startKey, endKey], you can write:
	// `Scan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
	Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

	// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
	// The returned keys are in reversed lexicographical order.
	// If endKey is empty, it means unbounded.
	// If you want to include the startKey or exclude the endKey, push a '\0' to the key. For example, to scan
	// (endKey, startKey], you can write:
	// `ReverseScan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
	// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
	ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)

	Close() error
}
