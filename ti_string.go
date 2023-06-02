package xdistikv

import (
	"context"

	"github.com/weedge/pkg/driver"
	openkvdriver "github.com/weedge/pkg/driver/openkv"
)

type DBString struct {
	*DB
}

func NewDBString(db *DB) *DBString {
	return &DBString{DB: db}
}

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return ErrValueSize
	}
	return nil
}

func (db *DBString) Set(ctx context.Context, key []byte, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}
	if err := checkValueSize(value); err != nil {
		return err
	}

	key = db.encodeStringKey(key)
	err := db.IKV.PutWithTTL(ctx, key, value, 0)
	if err != nil {
		return err
	}

	return nil
}
func (db *DBString) SetNX(ctx context.Context, key []byte, value []byte) (n int64, err error)
func (db *DBString) SetEX(ctx context.Context, key []byte, duration int64, value []byte) error
func (db *DBString) SetNXEX(ctx context.Context, key []byte, duration int64, value []byte) error
func (db *DBString) SetXXEX(ctx context.Context, key []byte, duration int64, value []byte) error

func (db *DBString) Get(ctx context.Context, key []byte) ([]byte, error)
func (db *DBString) GetSlice(ctx context.Context, key []byte) (openkvdriver.ISlice, error)
func (db *DBString) GetSet(ctx context.Context, key []byte, value []byte) ([]byte, error)

func (db *DBString) Incr(ctx context.Context, key []byte) (int64, error)
func (db *DBString) IncrBy(ctx context.Context, key []byte, increment int64) (int64, error)
func (db *DBString) Decr(ctx context.Context, key []byte) (int64, error)
func (db *DBString) DecrBy(ctx context.Context, key []byte, decrement int64) (int64, error)

func (db *DBString) MGet(ctx context.Context, keys ...[]byte) ([][]byte, error)
func (db *DBString) MSet(ctx context.Context, args ...driver.KVPair) error

func (db *DBString) SetRange(ctx context.Context, key []byte, offset int, value []byte) (int64, error)
func (db *DBString) GetRange(ctx context.Context, key []byte, start int, end int) ([]byte, error)

func (db *DBString) StrLen(ctx context.Context, key []byte) (int64, error)
func (db *DBString) Append(ctx context.Context, key []byte, value []byte) (int64, error)

func (db *DBString) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBString) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBString) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBString) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBString) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBString) Persist(ctx context.Context, key []byte) (int64, error)
