package xdistikv

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func MinInt32(a int32, b int32) int32 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt32(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

type DBList struct {
	*DB
}

func NewDBList(db *DB) *DBList {
	return &DBList{DB: db}
}

func (db *DBList) lSetMeta(txn *transaction.KVTxn, ek []byte, headSeq int32, tailSeq int32) (int32, error) {
	size := tailSeq - headSeq + 1
	if size < 0 {
		return 0, fmt.Errorf("invalid meta sequence range [%d, %d]", headSeq, tailSeq)
	}

	if size == 0 {
		err := txn.Delete(ek)
		return 0, err
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(headSeq))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(tailSeq))
	if err := txn.Set(ek, buf); err != nil {
		return 0, err
	}

	return size, nil
}

func (db *DBList) lGetMeta(ctx context.Context, txn *transaction.KVTxn, ek []byte) (headSeq int32, tailSeq int32, size int32, err error) {
	var v []byte
	if txn != nil {
		v, err = txn.Get(ctx, ek)
	} else {
		v, err = db.kvClient.GetKVClient().Get(ctx, ek)
	}
	if err != nil {
		return
	}
	if v == nil {
		headSeq = listInitialSeq
		tailSeq = listInitialSeq
		size = 0
		return
	}

	headSeq = int32(binary.LittleEndian.Uint32(v[0:4]))
	tailSeq = int32(binary.LittleEndian.Uint32(v[4:8]))
	size = tailSeq - headSeq + 1
	return
}

func (db *DBList) LIndex(ctx context.Context, key []byte, index int32) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	metaKey := db.lEncodeMetaKey(key)
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		headSeq, tailSeq, _, err := db.lGetMeta(ctx, txn, metaKey)
		if err != nil {
			return nil, err
		}

		seq := headSeq + index
		if index < 0 {
			seq = tailSeq + index + 1
		}
		sk := db.lEncodeListKey(key, seq)

		return txn.Get(ctx, sk)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([]byte), nil
}

func (db *DBList) LLen(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	ek := db.lEncodeMetaKey(key)
	_, _, size, err := db.lGetMeta(ctx, nil, ek)
	return int64(size), err
}

func (db *DBList) lpop(ctx context.Context, key []byte, whereSeq int32) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (value interface{}, err error) {
		metaKey := db.lEncodeMetaKey(key)
		headSeq, tailSeq, size, err := db.lGetMeta(ctx, nil, metaKey)
		if err != nil {
			return
		}
		if size == 0 {
			return
		}

		seq := headSeq
		if whereSeq == listTailSeq {
			seq = tailSeq
		}

		itemKey := db.lEncodeListKey(key, seq)
		value, err = txn.Get(ctx, itemKey)
		if err != nil {
			return
		}

		if whereSeq == listHeadSeq {
			headSeq++
		} else {
			tailSeq--
		}

		if err = txn.Delete(itemKey); err != nil {
			return
		}

		size, err = db.lSetMeta(txn, metaKey, headSeq, tailSeq)
		if err != nil {
			return
		}
		if size == 0 {
			_, err = db.rmExpire(ctx, txn, ListType, key)
			if err != nil {
				return
			}
		}

		return
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([]byte), err
}

func (db *DBList) LPop(ctx context.Context, key []byte) ([]byte, error) {
	return db.lpop(ctx, key, listHeadSeq)
}

func (db *DBList) LTrim(ctx context.Context, key []byte, start, stop int64) error {
	if err := checkKeySize(key); err != nil {
		return err
	}

	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (res interface{}, err error) {
		ek := db.lEncodeMetaKey(key)
		headSeq, _, llen, err := db.lGetMeta(ctx, txn, ek)
		if err != nil {
			return
		}

		start := int32(start)
		stop := int32(stop)
		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}
		if start >= llen || start > stop {
			if _, err = db.delete(ctx, txn, key); err != nil {
				return
			}
			if _, err = db.rmExpire(ctx, txn, ListType, key); err != nil {
				return
			}
			return nil, nil
		}

		if start < 0 {
			start = 0
		}
		if stop >= llen {
			stop = llen - 1
		}

		if start > 0 {
			for i := int32(0); i < start; i++ {
				if err = txn.Delete(db.lEncodeListKey(key, headSeq+i)); err != nil {
					return
				}
			}
		}
		if stop < int32(llen-1) {
			for i := int32(stop + 1); i < llen; i++ {
				if err = txn.Delete(db.lEncodeListKey(key, headSeq+i)); err != nil {
					return
				}
			}
		}

		_, err = db.lSetMeta(txn, ek, headSeq+start, headSeq+stop)

		return nil, err
	})

	return err
}

func (db *DBList) ltrim(ctx context.Context, key []byte, trimSize, whereSeq int32) (int32, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	if trimSize == 0 {
		return 0, nil
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (res interface{}, err error) {
		metaKey := db.lEncodeMetaKey(key)
		headSeq, tailSeq, size, err := db.lGetMeta(ctx, txn, metaKey)
		if err != nil {
			return 0, err
		} else if size == 0 {
			return 0, nil
		}

		var (
			trimStartSeq int32
			trimEndSeq   int32
		)

		if whereSeq == listHeadSeq {
			trimStartSeq = headSeq
			trimEndSeq = MinInt32(trimStartSeq+trimSize-1, tailSeq)
			headSeq = trimEndSeq + 1
		} else {
			trimEndSeq = tailSeq
			trimStartSeq = MaxInt32(trimEndSeq-trimSize+1, headSeq)
			tailSeq = trimStartSeq - 1
		}

		for trimSeq := trimStartSeq; trimSeq <= trimEndSeq; trimSeq++ {
			itemKey := db.lEncodeListKey(key, trimSeq)
			if err = txn.Delete(itemKey); err != nil {
				return 0, err
			}
		}

		if size, err = db.lSetMeta(txn, metaKey, headSeq, tailSeq); err != nil {
			return 0, err
		}
		if size == 0 {
			if _, err = db.rmExpire(ctx, txn, ListType, key); err != nil {
				return 0, err
			}
		}

		return trimEndSeq - trimStartSeq + 1, err
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int32), nil
}

func (db *DBList) LTrimFront(ctx context.Context, key []byte, trimSize int32) (int32, error) {
	return db.ltrim(ctx, key, trimSize, listHeadSeq)
}

func (db *DBList) LTrimBack(ctx context.Context, key []byte, trimSize int32) (int32, error) {
	return db.ltrim(ctx, key, trimSize, listTailSeq)
}

func (db *DBList) lpush(ctx context.Context, key []byte, whereSeq int32, args ...[]byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (res interface{}, err error) {
		metaKey := db.lEncodeMetaKey(key)
		headSeq, tailSeq, size, err := db.lGetMeta(ctx, txn, metaKey)
		if err != nil {
			return 0, err
		}

		pushCnt := len(args)
		if pushCnt == 0 {
			return int64(size), nil
		}

		seq := headSeq
		var delta int32 = -1
		if whereSeq == listTailSeq {
			seq = tailSeq
			delta = 1
		}

		//	append elements
		if size > 0 {
			seq += delta
		}
		for i := 0; i < pushCnt; i++ {
			ek := db.lEncodeListKey(key, seq+int32(i)*delta)
			if err = txn.Set(ek, args[i]); err != nil {
				return 0, err
			}
		}

		seq += int32(pushCnt-1) * delta
		if seq <= listMinSeq || seq >= listMaxSeq {
			return 0, ErrListSeq
		}

		//	set meta info
		if whereSeq == listHeadSeq {
			headSeq = seq
		} else {
			tailSeq = seq
		}
		if _, err = db.lSetMeta(txn, metaKey, headSeq, tailSeq); err != nil {
			return
		}

		return int64(size) + int64(pushCnt), nil
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	//db.NotifyAsReady(ctx, key)

	return res.(int64), err
}

func (db *DBList) LPush(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	return db.lpush(ctx, key, listHeadSeq, args...)
}

func (db *DBList) LSet(ctx context.Context, key []byte, index int32, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}

	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (res interface{}, err error) {
		metaKey := db.lEncodeMetaKey(key)
		headSeq, tailSeq, _, err := db.lGetMeta(ctx, txn, metaKey)
		if err != nil {
			return nil, err
		}

		seq := headSeq + index
		if index < 0 {
			seq = tailSeq + index + 1
		}
		if seq < headSeq || seq > tailSeq {
			return nil, ErrListIndex
		}
		sk := db.lEncodeListKey(key, seq)
		return nil, txn.Set(sk, value)
	})

	return err
}

func (db *DBList) LRange(ctx context.Context, key []byte, start int32, stop int32) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (res interface{}, err error) {
		metaKey := db.lEncodeMetaKey(key)
		headSeq, _, llen, err := db.lGetMeta(ctx, txn, metaKey)
		if err != nil {
			return nil, err
		}

		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}
		if start < 0 {
			start = 0
		}

		if start > stop || start >= llen {
			return [][]byte{}, nil
		}

		if stop >= llen {
			stop = llen - 1
		}

		headSeq += start
		startKey := db.lEncodeListKey(key, headSeq)
		it, err := txn.Iter(startKey, nil)
		if err != nil {
			return nil, err
		}
		defer it.Close()

		limit := (stop - start) + 1
		v := make([][]byte, 0, limit)
		for ; it.Valid() && limit > 0; it.Next() {
			v = append(v, it.Value()[:])
			limit--
		}
		return v, nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([][]byte), nil
}

func (db *DBList) RPop(ctx context.Context, key []byte) ([]byte, error) {
	return db.lpop(ctx, key, listTailSeq)
}
func (db *DBList) RPush(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	return db.lpush(ctx, key, listTailSeq, args...)
}

// todo: v2
// list block pop, use PD(etcd) watch to do it
func (db *DBList) BLPop(ctx context.Context, keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return nil, ErrCmdNotSupport
}
func (db *DBList) BRPop(ctx context.Context, keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return nil, ErrCmdNotSupport
}

func (db *DBList) delete(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error) {
	mk := db.lEncodeMetaKey(key)
	headSeq, tailSeq, _, err := db.lGetMeta(ctx, txn, mk)
	if err != nil {
		return
	}

	// range close: [s,e]
	startKey := db.lEncodeListKey(key, headSeq)
	stopKey := db.lEncodeListKey(key, tailSeq)
	it, err := txn.Iter(startKey, append(stopKey, 0))
	if err != nil {
		return
	}
	for ; it.Valid(); it.Next() {
		err = txn.Delete(it.Key())
		if err != nil {
			return
		}
		num++
	}
	it.Close()

	err = txn.Delete(mk)
	if err != nil {
		return
	}

	return num, nil
}

func (db *DBList) Del(ctx context.Context, keys ...[]byte) (int64, error) {
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
			n, err := db.delete(ctx, txn, key)
			if err != nil {
				return 0, err
			}
			if n > 0 {
				nums++
			}
			_, err = db.rmExpire(ctx, txn, ListType, key)
			if err != nil {
				return 0, err
			}
		}
		return nums, nil
	})
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (db *DBList) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	mk := db.lEncodeMetaKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, mk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBList) lExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	data, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		if llen, err := db.LLen(ctx, key); err != nil || llen == 0 {
			return 0, err
		}

		if err := db.expireAt(txn, ListType, key, when); err != nil {
			return 0, err
		}
		return 1, nil
	})
	if err != nil {
		return 0, err
	}

	return int64(data.(int)), nil
}
func (db *DBList) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.lExpireAt(ctx, key, time.Now().Unix()+duration)
}
func (db *DBList) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.lExpireAt(ctx, key, when)
}

func (db *DBList) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.lEncodeMetaKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(ctx, ListType, key)
}

func (db *DBList) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.rmExpire(ctx, txn, ListType, key)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), err
}
