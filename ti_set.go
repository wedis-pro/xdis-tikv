package xdistikv

import "context"

type DBSet struct {
	*DB
}

func NewDBSet(db *DB) *DBSet {
	return &DBSet{DB: db}
}

func (db *DBSet) SAdd(ctx context.Context, key []byte, args ...[]byte) (int64, error)
func (db *DBSet) SCard(ctx context.Context, key []byte) (int64, error)
func (db *DBSet) SDiff(ctx context.Context, keys ...[]byte) ([][]byte, error)
func (db *DBSet) SDiffStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error)
func (db *DBSet) SInter(ctx context.Context, keys ...[]byte) ([][]byte, error)
func (db *DBSet) SInterStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error)
func (db *DBSet) SIsMember(ctx context.Context, key []byte, member []byte) (int64, error)
func (db *DBSet) SMembers(ctx context.Context, key []byte) ([][]byte, error)
func (db *DBSet) SRem(ctx context.Context, key []byte, args ...[]byte) (int64, error)
func (db *DBSet) SUnion(ctx context.Context, keys ...[]byte) ([][]byte, error)
func (db *DBSet) SUnionStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error)

func (db *DBSet) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBSet) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBSet) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBSet) Persist(ctx context.Context, key []byte) (int64, error)
