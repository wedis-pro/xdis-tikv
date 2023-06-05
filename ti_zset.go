package xdistikv

import (
	"bytes"
	"context"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/driver"
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
	if score <= MinScore || score >= MaxScore {
		return 0, ErrScoreOverflow
	}

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
		keys, _, err = db.kvClient.GetKVClient().ReverseScan(ctx, append(maxKey, 0), sk, rawkv.MaxRawKVScanLimit)
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

func (db *DBZSet) ZRemRangeByRank(ctx context.Context, key []byte, start int, stop int) (int64, error)

func (db *DBZSet) ZRemRangeByScore(ctx context.Context, key []byte, min int64, max int64) (int64, error)

func (db *DBZSet) ZRevRange(ctx context.Context, key []byte, start int, stop int) ([]driver.ScorePair, error)

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
