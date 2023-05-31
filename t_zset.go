package xdistikv

import "github.com/weedge/pkg/driver"

type DBZSet struct {
}

func NewDBZSet() *DBZSet {
	return &DBZSet{}
}

func (db *DBZSet) ZAdd(key []byte, args ...driver.ScorePair) (int64, error)
func (db *DBZSet) ZCard(key []byte) (int64, error)
func (db *DBZSet) ZScore(key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRem(key []byte, members ...[]byte) (int64, error)
func (db *DBZSet) ZIncrBy(key []byte, delta int64, member []byte) (int64, error)
func (db *DBZSet) ZCount(key []byte, min int64, max int64) (int64, error)
func (db *DBZSet) ZRank(key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRemRangeByRank(key []byte, start int, stop int) (int64, error)
func (db *DBZSet) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error)
func (db *DBZSet) ZRevRange(key []byte, start int, stop int) ([]driver.ScorePair, error)
func (db *DBZSet) ZRevRank(key []byte, member []byte) (int64, error)
func (db *DBZSet) ZRevRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]driver.ScorePair, error)
func (db *DBZSet) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]driver.ScorePair, error)
func (db *DBZSet) ZRangeByScoreGeneric(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]driver.ScorePair, error)
func (db *DBZSet) ZUnionStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error)
func (db *DBZSet) ZInterStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error)
func (db *DBZSet) ZRangeByLex(key []byte, min []byte, max []byte, rangeType driver.RangeType, offset int, count int) ([][]byte, error)
func (db *DBZSet) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error)
func (db *DBZSet) ZLexCount(key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error)

func (db *DBZSet) Del(keys ...[]byte) (int64, error)
func (db *DBZSet) Exists(key []byte) (int64, error)
func (db *DBZSet) Expire(key []byte, duration int64) (int64, error)
func (db *DBZSet) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBZSet) TTL(key []byte) (int64, error)
func (db *DBZSet) Persist(key []byte) (int64, error)
