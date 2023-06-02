package xdistikv

import (
	"context"

	"github.com/weedge/pkg/driver"
)

type DBHash struct {
	*DB
}

func NewDBHash(db *DB) *DBHash {
	return &DBHash{DB: db}
}

func (db *DBHash) HSet(ctx context.Context, key []byte, field []byte, value []byte) (int64, error)
func (db *DBHash) HGet(ctx context.Context, key []byte, field []byte) ([]byte, error)
func (db *DBHash) HLen(ctx context.Context, key []byte) (int64, error)
func (db *DBHash) HMset(ctx context.Context, key []byte, args ...driver.FVPair) error
func (db *DBHash) HMget(ctx context.Context, key []byte, args ...[]byte) ([][]byte, error)
func (db *DBHash) HDel(ctx context.Context, key []byte, args ...[]byte) (int64, error)
func (db *DBHash) HIncrBy(ctx context.Context, key []byte, field []byte, delta int64) (int64, error)
func (db *DBHash) HGetAll(ctx context.Context, key []byte) ([]driver.FVPair, error)
func (db *DBHash) HKeys(ctx context.Context, key []byte) ([][]byte, error)
func (db *DBHash) HValues(ctx context.Context, key []byte) ([][]byte, error)

func (db *DBHash) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBHash) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBHash) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBHash) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBHash) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBHash) Persist(ctx context.Context, key []byte) (int64, error)
