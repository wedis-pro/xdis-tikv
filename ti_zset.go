package xdistikv

import (
	"context"

	"github.com/weedge/pkg/driver"
)

type DBZSet struct {
	*DB
}

func NewDBZSet(db *DB) *DBZSet {
	return &DBZSet{DB: db}
}

func (db *DBZSet) ZAdd(ctx context.Context, key []byte, args ...driver.ScorePair) (int64, error)
func (db *DBZSet) ZCard(ctx context.Context, key []byte) (int64, error)
func (db *DBZSet) ZScore(ctx context.Context, key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRem(ctx context.Context, key []byte, members ...[]byte) (int64, error)
func (db *DBZSet) ZIncrBy(ctx context.Context, key []byte, delta int64, member []byte) (int64, error)
func (db *DBZSet) ZCount(ctx context.Context, key []byte, min int64, max int64) (int64, error)
func (db *DBZSet) ZRank(ctx context.Context, key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRemRangeByRank(ctx context.Context, key []byte, start int, stop int) (int64, error)
func (db *DBZSet) ZRemRangeByScore(ctx context.Context, key []byte, min int64, max int64) (int64, error)
func (db *DBZSet) ZRevRange(ctx context.Context, key []byte, start int, stop int) ([]driver.ScorePair, error)
func (db *DBZSet) ZRevRank(ctx context.Context, key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRevRangeByScore(ctx context.Context, key []byte, min int64, max int64, offset int, count int) ([]driver.ScorePair, error)
func (db *DBZSet) ZRangeGeneric(ctx context.Context, key []byte, start int, stop int, reverse bool) ([]driver.ScorePair, error)
func (db *DBZSet) ZRangeByScoreGeneric(ctx context.Context, key []byte, min int64, max int64, offset int, count int, reverse bool) ([]driver.ScorePair, error)
func (db *DBZSet) ZUnionStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error)
func (db *DBZSet) ZInterStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error)
func (db *DBZSet) ZRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType, offset int, count int) ([][]byte, error)
func (db *DBZSet) ZRemRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error)
func (db *DBZSet) ZLexCount(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error)

func (db *DBZSet) Del(ctx context.Context, keys ...[]byte) (int64, error)
func (db *DBZSet) Exists(ctx context.Context, key []byte) (int64, error)
func (db *DBZSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error)
func (db *DBZSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error)
func (db *DBZSet) TTL(ctx context.Context, key []byte) (int64, error)
func (db *DBZSet) Persist(ctx context.Context, key []byte) (int64, error)
