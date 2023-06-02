package xdistikv

import "context"

type DBBitmap struct {
	*DB
}

func NewDBBitmap(db *DB) *DBBitmap {
	return &DBBitmap{DB: db}
}

func (db *DBBitmap) BitOP(ctx context.Context, op string, destKey []byte, srcKeys ...[]byte) (int64, error)
func (db *DBBitmap) BitCount(ctx context.Context, key []byte, start int, end int) (int64, error)
func (db *DBBitmap) BitPos(ctx context.Context, key []byte, on int, start int, end int) (int64, error)
func (db *DBBitmap) SetBit(ctx context.Context, key []byte, offset int, on int) (int64, error)
func (db *DBBitmap) GetBit(ctx context.Context, key []byte, offset int) (int64, error)

func (db *DBBitmap) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBBitmap) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBBitmap) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBBitmap) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBBitmap) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBBitmap) Persist(ctx context.Context, key []byte) (int64, error)
