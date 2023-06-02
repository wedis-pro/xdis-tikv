package xdistikv

import (
	"context"
	"time"
)

type DBList struct {
	*DB
}

func NewDBList(db *DB) *DBList {
	return &DBList{DB: db}
}

func (db *DBList) LIndex(ctx context.Context, key []byte, index int32) ([]byte, error)
func (db *DBList) LLen(ctx context.Context, key []byte) (int64, error)
func (db *DBList) LPop(ctx context.Context, key []byte) ([]byte, error)
func (db *DBList) LTrim(ctx context.Context, key []byte, start, stop int64) error
func (db *DBList) LTrimFront(ctx context.Context, key []byte, trimSize int32) (int32, error)
func (db *DBList) LTrimBack(ctx context.Context, key []byte, trimSize int32) (int32, error)
func (db *DBList) LPush(ctx context.Context, key []byte, args ...[]byte) (int64, error)
func (db *DBList) LSet(ctx context.Context, key []byte, index int32, value []byte) error
func (db *DBList) LRange(ctx context.Context, key []byte, start int32, stop int32) ([][]byte, error)
func (db *DBList) RPop(ctx context.Context, key []byte) ([]byte, error)
func (db *DBList) RPush(ctx context.Context, key []byte, args ...[]byte) (int64, error)
func (db *DBList) BLPop(ctx context.Context, keys [][]byte, timeout time.Duration) ([]interface{}, error)
func (db *DBList) BRPop(ctx context.Context, keys [][]byte, timeout time.Duration) ([]interface{}, error)

func (db *DBList) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBList) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBList) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBList) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBList) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBList) Persist(ctx context.Context, key []byte) (int64, error)
