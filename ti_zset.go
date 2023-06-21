package xdistikv

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	tDriver "github.com/weedge/xdis-tikv/driver"
)

type DBZSet struct {
	*DB
}

func NewDBZSet(db *DB) *DBZSet {
	return &DBZSet{DB: db}
}

func checkZSetKMSize(key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(member) > MaxZSetMemberSize || len(member) == 0 {
		return ErrZSetMemberSize
	}
	return nil
}

func (db *DBZSet) zSetItem(ctx context.Context, txn *transaction.KVTxn, key []byte, score int64, member []byte) (int64, error) {
	var exists int64
	ek := db.zEncodeSetKey(key, member)

	if v, err := txn.Get(ctx, ek); err != nil {
		return 0, err
	} else if v != nil {
		exists = 1

		s, err := Int64(v, err)
		if err != nil {
			return 0, err
		}

		sk := db.zEncodeScoreKey(key, member, s)
		if err := txn.Delete(sk); err != nil {
			return 0, err
		}
	}

	if err := txn.Set(ek, PutInt64(score)); err != nil {
		return 0, err
	}

	sk := db.zEncodeScoreKey(key, member, score)
	if err := txn.Set(sk, []byte{}); err != nil {
		return 0, err
	}

	return exists, nil
}

func (db *DBZSet) zIncrSize(ctx context.Context, txn *transaction.KVTxn, key []byte, delta int64) (int64, error) {
	sk := db.zEncodeSizeKey(key)
	size, err := Int64(txn.Get(ctx, sk))
	if err != nil {
		return 0, err
	}
	size += delta
	if size <= 0 {
		size = 0
		if err := txn.Delete(sk); err != nil {
			return 0, err
		}
		if _, err := db.rmExpire(ctx, txn, ZSetType, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err := txn.Set(sk, PutInt64(size)); err != nil {
		return 0, err
	}

	return size, nil
}

func (db *DBZSet) ZAdd(ctx context.Context, key []byte, args ...driver.ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	for i := 0; i < len(args); i++ {
		member := args[i].Member
		if err := checkZSetKMSize(key, member); err != nil {
			return 0, err
		}
		score := args[i].Score
		if score <= MinScore || score >= MaxScore {
			return 0, ErrScoreOverflow
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var num int64
		for i := 0; i < len(args); i++ {
			score := args[i].Score
			member := args[i].Member

			if n, err := db.zSetItem(ctx, txn, key, score, member); err != nil {
				return 0, err
			} else if n == 0 {
				//add new
				num++
			}
		}

		if _, err := db.zIncrSize(ctx, txn, key, num); err != nil {
			return 0, err
		}

		return num, nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBZSet) ZCard(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := db.zEncodeSizeKey(key)
	return Int64(db.kvClient.GetKVClient().Get(ctx, sk))
}

func (db *DBZSet) ZScore(ctx context.Context, key []byte, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	score := InvalidScore
	k := db.zEncodeSetKey(key, member)
	v, err := db.kvClient.GetKVClient().Get(ctx, k)
	if err != nil {
		return InvalidScore, err
	}
	if v == nil {
		return InvalidScore, ErrScoreMiss
	}
	if score, err = Int64(v, nil); err != nil {
		return InvalidScore, err
	}

	return score, nil
}

func (db *DBZSet) zDelItem(ctx context.Context, txn *transaction.KVTxn, key []byte, member []byte, skipDelScore bool) (int64, error) {
	ek := db.zEncodeSetKey(key, member)
	if v, err := txn.Get(ctx, ek); err != nil {
		return 0, err
	} else if v == nil {
		//not exists
		return 0, nil
	} else {
		//exists
		if !skipDelScore {
			//we must del score
			s, err := Int64(v, err)
			if err != nil {
				return 0, err
			}
			sk := db.zEncodeScoreKey(key, member, s)
			if err := txn.Delete(sk); err != nil {
				return 0, err
			}
		}
	}

	if err := txn.Delete(ek); err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBZSet) ZRem(ctx context.Context, key []byte, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	for i := 0; i < len(members); i++ {
		if err := checkZSetKMSize(key, members[i]); err != nil {
			return 0, err
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var num int64
		for i := 0; i < len(members); i++ {
			if n, err := db.zDelItem(ctx, txn, key, members[i], false); err != nil {
				return 0, err
			} else if n == 1 {
				num++
			}
		}

		if _, err := db.zIncrSize(ctx, txn, key, -num); err != nil {
			return 0, err
		}

		return num, nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBZSet) ZIncrBy(ctx context.Context, key []byte, delta int64, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var oldScore int64
		ek := db.zEncodeSetKey(key, member)
		v, err := txn.Get(ctx, ek)
		if err != nil {
			return InvalidScore, err
		} else if v == nil {
			if _, err := db.zIncrSize(ctx, txn, key, 1); err != nil {
				return InvalidScore, err
			}
		} else {
			if oldScore, err = Int64(v, err); err != nil {
				return InvalidScore, err
			}
		}

		newScore := oldScore + delta
		if newScore >= MaxScore || newScore <= MinScore {
			return InvalidScore, ErrScoreOverflow
		}

		sk := db.zEncodeScoreKey(key, member, newScore)
		if err := txn.Set(sk, []byte{}); err != nil {
			return InvalidScore, err
		}
		if err := txn.Set(ek, PutInt64(newScore)); err != nil {
			return InvalidScore, err
		}

		if v != nil {
			// so as to update score, we must delete the old one
			oldSk := db.zEncodeScoreKey(key, member, oldScore)
			if err := txn.Delete(oldSk); err != nil {
				return InvalidScore, err
			}
		}

		return newScore, nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBZSet) ZCount(ctx context.Context, key []byte, min int64, max int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	minKey := db.zEncodeStartScoreKey(key, min)
	maxKey := db.zEncodeStopScoreKey(key, max)
	keys, _, err := db.kvClient.GetKVClient().Scan(ctx, minKey, maxKey, rawkv.MaxRawKVScanLimit)
	if err != nil {
		return 0, err
	}

	return int64(len(keys)), nil
}

func (db *DBZSet) zrank(ctx context.Context, key []byte, member []byte, reverse bool) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return 0, err
	}

	k := db.zEncodeSetKey(key, member)
	v, err := db.kvClient.GetKVClient().Get(ctx, k)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return -1, nil
	}

	s, err := Int64(v, nil)
	if err != nil {
		return 0, err
	}

	var keys [][]byte
	sk := db.zEncodeScoreKey(key, member, s)
	if !reverse {
		minKey := db.zEncodeStartScoreKey(key, MinScore)
		// range close [s,e]
		keys, _, err = db.kvClient.GetKVClient().Scan(ctx, minKey, append(sk, 0), rawkv.MaxRawKVScanLimit)
		if err != nil {
			return 0, err
		}
	} else {
		maxKey := db.zEncodeStopScoreKey(key, MaxScore)
		// range close [s,e]
		keys, _, err = db.kvClient.GetKVClient().ReverseScan(ctx, sk, append(maxKey, 0), rawkv.MaxRawKVScanLimit)
		if err != nil {
			return 0, err
		}
	}

	n := int64(len(keys))
	lastKey := keys[n-1]
	if _, m, _, err := db.zDecodeScoreKey(lastKey); err == nil && bytes.Equal(m, member) {
		n--
		return n, nil
	}

	return -1, nil
}

func (db *DBZSet) ZRank(ctx context.Context, key []byte, member []byte) (int64, error) {
	return db.zrank(ctx, key, member, false)
}

func (db *DBZSet) ZRevRank(ctx context.Context, key []byte, member []byte) (int64, error) {
	return db.zrank(ctx, key, member, true)
}

// zIterator range close: [min,max]
func (db *DBZSet) zIterator(ctx context.Context, txn *transaction.KVTxn, key []byte, min int64, max int64, offset int, count int, reverse bool) (tDriver.IIterator, error) {
	minKey := db.zEncodeStartScoreKey(key, min)
	maxKey := db.zEncodeStopScoreKey(key, max)

	if !reverse {
		//return db.kvClient.GetTxnKVClient().Iter(ctx, txn, minKey, append(maxKey, 0), offset, count)
		return db.kvClient.GetTxnKVClient().Iter(ctx, txn, []byte{}, nil, offset, count)
	}
	return db.kvClient.GetTxnKVClient().ReverseIter(ctx, txn, minKey, append(maxKey, 0), offset, count)
}

func (db *DBZSet) zRange(ctx context.Context, key []byte, min int64, max int64, offset int, count int, reverse bool) ([]driver.ScorePair, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeySize
	}

	if offset < 0 {
		return []driver.ScorePair{}, nil
	}

	nv := count
	// count may be very large, so we must limit it for below mem make.
	if nv <= 0 || nv > 1024 {
		nv = 64
	}
	v := make([]driver.ScorePair, 0, nv)

	it, err := db.zIterator(ctx, nil, key, min, max, offset, count, reverse)
	if err != nil {
		return nil, err
	}

	for ; it.Valid(); it.Next() {
		_, m, s, err := db.zDecodeScoreKey(it.Key())
		if err != nil {
			continue
		}
		v = append(v, driver.ScorePair{Member: m, Score: s})
	}
	it.Close()

	return v, nil
}

// zParseLimit parse index pos to limit offset count
func (db *DBZSet) zParseLimit(ctx context.Context, key []byte, start int, stop int) (offset int, count int, err error) {
	if start < 0 || stop < 0 {
		var size int64
		size, err = db.ZCard(ctx, key)
		if err != nil {
			return
		}

		llen := int(size)

		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}

		if start < 0 {
			start = 0
		}

		if start >= llen {
			offset = -1
			return
		}
	}

	if start > stop {
		offset = -1
		return
	}

	offset = start
	count = (stop - start) + 1
	return
}

// ZRangeGeneric is a generic function for scan zset.
// zrange/zrevrange index pos start,stop
func (db *DBZSet) ZRangeGeneric(ctx context.Context, key []byte, start int, stop int, reverse bool) ([]driver.ScorePair, error) {
	offset, count, err := db.zParseLimit(ctx, key, start, stop)
	if err != nil {
		return nil, err
	}

	return db.zRange(ctx, key, MinScore, MaxScore, offset, count, reverse)
}

func (db *DBZSet) ZRevRange(ctx context.Context, key []byte, start int, stop int) ([]driver.ScorePair, error) {
	return db.ZRangeGeneric(ctx, key, start, stop, true)
}

// ZRangeByScoreGeneric is a generic function to scan zset with score.
// min and max must be inclusive
// if no limit, set offset = 0 and count<0
func (db *DBZSet) ZRangeByScoreGeneric(ctx context.Context, key []byte, min int64, max int64,
	offset int, count int, reverse bool) ([]driver.ScorePair, error) {

	return db.zRange(ctx, key, min, max, offset, count, reverse)
}

// ZRevRangeByScore gets the data with score at [min, max]
func (db *DBZSet) ZRevRangeByScore(ctx context.Context, key []byte, min int64, max int64, offset int, count int) ([]driver.ScorePair, error) {
	return db.ZRangeByScoreGeneric(ctx, key, min, max, offset, count, true)
}

func (db *DBZSet) scoreEncode(key []byte, min []byte, max []byte, rangeType driver.RangeType) (minScore []byte, maxScore []byte) {
	if min == nil {
		min = db.zEncodeStartSetKey(key)
	} else {
		min = db.zEncodeSetKey(key, min)
	}
	if max == nil {
		max = db.zEncodeStopSetKey(key)
	} else {
		max = db.zEncodeSetKey(key, max)
	}

	switch rangeType {
	case driver.RangeClose:
		maxScore = append(max, 0)
	case driver.RangeROpen:
	case driver.RangeLOpen:
		minScore = append(min, 0)
		maxScore = append(max, 0)
	case driver.RangeOpen:
		minScore = append(min, 0)
	default:
		return nil, nil
	}

	return
}

// ZRangeByLex scans the zset lexicographically
func (db *DBZSet) ZRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType, offset int, count int) ([][]byte, error) {
	min, max = db.scoreEncode(key, min, max, rangeType)
	it, err := db.kvClient.GetTxnKVClient().Iter(ctx, nil, min, max, offset, count)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	ay := make([][]byte, 0, 16)
	for ; it.Valid(); it.Next() {
		if _, m, err := db.zDecodeSetKey(it.Key()); err == nil {
			ay = append(ay, m)
		}
	}

	return ay, nil
}

// ZRemRangeByLex remvoes members in [min, max] lexicographically
func (db *DBZSet) ZRemRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
	min, max = db.scoreEncode(key, min, max, rangeType)
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		it, err := db.kvClient.GetTxnKVClient().Iter(ctx, txn, min, max, 0, rawkv.MaxRawKVScanLimit)
		if err != nil {
			return 0, err
		}
		defer it.Close()

		var n int64
		for ; it.Valid(); it.Next() {
			if err := txn.Delete(it.Key()); err != nil {
				return 0, err
			}
			n++
		}

		return n, nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

// ZLexCount gets the count of zset lexicographically.
func (db *DBZSet) ZLexCount(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
	min, max = db.scoreEncode(key, min, max, rangeType)
	it, err := db.kvClient.GetTxnKVClient().Iter(ctx, nil, min, max, 0, rawkv.MaxRawKVScanLimit)
	if err != nil {
		return 0, err
	}
	defer it.Close()

	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}

	return n, nil
}

func getAggregateFunc(aggregate []byte) func(int64, int64) int64 {
	aggr := strings.ToLower(utils.Bytes2String(aggregate))
	switch aggr {
	case AggregateSum:
		return func(a int64, b int64) int64 {
			return a + b
		}
	case AggregateMax:
		return func(a int64, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}
	case AggregateMin:
		return func(a int64, b int64) int64 {
			if a > b {
				return b
			}
			return a
		}
	}
	return nil
}

// ZUnionStore unions the zsets and stores to dest zset.
func (db *DBZSet) ZUnionStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {
	aggregateFunc := getAggregateFunc(aggregate)
	if aggregateFunc == nil {
		return 0, ErrInvalidAggregate
	}
	if len(srcKeys) < 1 {
		return 0, ErrInvalidSrcKeyNum
	}
	if weights != nil {
		if len(srcKeys) != len(weights) {
			return 0, ErrInvalidWeightNum
		}
	} else {
		weights = make([]int64, len(srcKeys))
		for i := 0; i < len(weights); i++ {
			weights[i] = 1
		}
	}

	var destMap = map[string]int64{}
	for i, key := range srcKeys {
		scorePairs, err := db.ZRangeGeneric(ctx, key, 0, -1, false)
		if err != nil {
			return 0, err
		}
		for _, pair := range scorePairs {
			if score, ok := destMap[utils.Bytes2String(pair.Member)]; !ok {
				destMap[utils.Bytes2String(pair.Member)] = pair.Score * weights[i]
			} else {
				destMap[utils.Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i])
			}
		}
	}

	return db.zDestDelStore(ctx, destKey, destMap)
}

// ZInterStore intersects the zsets and stores to dest zset.
func (db *DBZSet) ZInterStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {
	aggregateFunc := getAggregateFunc(aggregate)
	if aggregateFunc == nil {
		return 0, ErrInvalidAggregate
	}
	if len(srcKeys) < 1 {
		return 0, ErrInvalidSrcKeyNum
	}
	if weights != nil {
		if len(srcKeys) != len(weights) {
			return 0, ErrInvalidWeightNum
		}
	} else {
		weights = make([]int64, len(srcKeys))
		for i := 0; i < len(weights); i++ {
			weights[i] = 1
		}
	}

	var destMap = map[string]int64{}
	scorePairs, err := db.ZRangeGeneric(ctx, srcKeys[0], 0, -1, false)
	if err != nil {
		return 0, err
	}
	for _, pair := range scorePairs {
		destMap[utils.Bytes2String(pair.Member)] = pair.Score * weights[0]
	}

	for i, key := range srcKeys[1:] {
		scorePairs, err := db.ZRangeGeneric(ctx, key, 0, -1, false)
		if err != nil {
			return 0, err
		}
		tmpMap := map[string]int64{}
		for _, pair := range scorePairs {
			if score, ok := destMap[utils.Bytes2String(pair.Member)]; ok {
				tmpMap[utils.Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i+1])
			}
		}
		destMap = tmpMap
	}

	return db.zDestDelStore(ctx, destKey, destMap)
}

func (db *DBZSet) zDestDelStore(ctx context.Context, destKey []byte, destMap map[string]int64) (int64, error) {
	for member, score := range destMap {
		if err := checkZSetKMSize(destKey, []byte(member)); err != nil {
			return 0, err
		}
		if score <= MinScore || score >= MaxScore {
			return 0, ErrScoreOverflow
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		if _, err := db.delete(ctx, txn, destKey); err != nil {
			return 0, err
		}

		for member, score := range destMap {
			if _, err := db.zSetItem(ctx, txn, destKey, score, []byte(member)); err != nil {
				return 0, err
			}
		}

		var n = int64(len(destMap))
		sk := db.zEncodeSizeKey(destKey)
		if err := txn.Set(sk, PutInt64(n)); err != nil {
			return 0, err
		}

		return n, nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBZSet) zRemRange(ctx context.Context, txn *transaction.KVTxn, key []byte, min int64, max int64, offset int, count int) (int64, error) {
	if len(key) > MaxKeySize {
		return 0, ErrKeySize
	}

	it, err := db.zIterator(ctx, txn, key, min, max, offset, count, false)
	if err != nil {
		return 0, err
	}

	var num int64
	for ; it.Valid(); it.Next() {
		sk := it.Key()
		_, m, _, err := db.zDecodeScoreKey(sk)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(ctx, txn, key, m, true); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}

		if err := txn.Delete(sk); err != nil {
			return 0, err
		}
	}
	it.Close()

	if _, err := db.zIncrSize(ctx, txn, key, -num); err != nil {
		return 0, err
	}

	return num, nil
}

// ZRemRangeByRank removes the member at range from start to stop.
func (db *DBZSet) ZRemRangeByRank(ctx context.Context, key []byte, start int, stop int) (int64, error) {
	offset, count, err := db.zParseLimit(ctx, key, start, stop)
	if err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.zRemRange(ctx, txn, key, MinScore, MaxScore, offset, count)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

// ZRemRangeByScore removes the data with score at [min, max]
func (db *DBZSet) ZRemRangeByScore(ctx context.Context, key []byte, min int64, max int64) (int64, error) {
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.zRemRange(ctx, txn, key, min, max, 0, -1)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBZSet) delete(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error) {
	num, err = db.zRemRange(ctx, txn, key, MinScore, MaxScore, 0, -1)
	return
}

func (db *DBZSet) Del(ctx context.Context, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var nums int64 = 0
		for _, key := range keys {
			n, err := db.zRemRange(ctx, txn, key, MinScore, MaxScore, 0, -1)
			if err != nil {
				return 0, err
			}
			if n > 0 {
				nums++
			}
		}
		return nums, nil
	})
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (db *DBZSet) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.zEncodeSizeKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBZSet) sExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	data, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		if scnt, err := db.ZCard(ctx, key); err != nil || scnt == 0 {
			return 0, err
		}
		if err := db.expireAt(txn, ZSetType, key, when); err != nil {
			return 0, err
		}
		return 1, nil
	})
	if err != nil {
		return 0, err
	}

	return int64(data.(int)), nil
}

func (db *DBZSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, time.Now().Unix()+duration)
}

func (db *DBZSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, when)
}

func (db *DBZSet) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.zEncodeSizeKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(ctx, ZSetType, key)
}

func (db *DBZSet) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.rmExpire(ctx, txn, ZSetType, key)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), err
}
