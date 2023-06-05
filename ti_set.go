package xdistikv

import (
	"context"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/utils"
)

type DBSet struct {
	*DB
}

func NewDBSet(db *DB) *DBSet {
	return &DBSet{DB: db}
}

func checkSetKMSize(ctx context.Context, key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(member) > MaxSetMemberSize || len(member) == 0 {
		return ErrSetMemberSize
	}
	return nil
}

func (db *DBSet) sIncrSize(ctx context.Context, txn *transaction.KVTxn, key []byte, delta int64) (size int64, err error) {
	sk := db.sEncodeSizeKey(key)
	if size, err = Int64(txn.Get(ctx, sk)); err != nil {
		return 0, err
	}

	size += delta
	if size <= 0 {
		if err = txn.Delete(sk); err != nil {
			return 0, err
		}
		if _, err = db.rmExpire(ctx, txn, SetType, key); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err = txn.Set(sk, PutInt64(size)); err != nil {
		return 0, err
	}

	return size, nil
}

func (db *DBSet) SAdd(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(ctx, key, args[i]); err != nil {
			return 0, err
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var ek []byte
		var num int64
		for i := 0; i < len(args); i++ {
			ek = db.sEncodeSetKey(key, args[i])
			if v, err := txn.Get(ctx, ek); err != nil {
				return 0, err
			} else if v == nil {
				num++
			}

			if err := txn.Set(ek, nil); err != nil {
				return 0, err
			}
		}
		if _, err := db.sIncrSize(ctx, txn, key, num); err != nil {
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

func (db *DBSet) SCard(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.sEncodeSizeKey(key)
	return Int64(db.kvClient.GetKVClient().Get(ctx, sk))
}

func (db *DBSet) sMembers(ctx context.Context, txn *transaction.KVTxn, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	// ropen:[s,e)
	start := db.sEncodeStartKey(key)
	stop := db.sEncodeStopKey(key)
	it, err := txn.Iter(start, stop)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	v := make([][]byte, 0, 16)
	for ; it.Valid(); it.Next() {
		_, m, err := db.sDecodeSetKey(it.Key())
		if err != nil {
			return nil, err
		}

		v = append(v, m)
	}

	return v, nil
}

func (db *DBSet) sDiffGeneric(ctx context.Context, txn *transaction.KVTxn, keys ...[]byte) ([][]byte, error) {
	destMap := make(map[string]bool)

	members, err := db.sMembers(ctx, txn, keys[0])
	if err != nil {
		return nil, err
	}

	for _, m := range members {
		destMap[utils.Bytes2String(m)] = true
	}

	for _, k := range keys[1:] {
		members, err := db.sMembers(ctx, txn, k)
		if err != nil {
			return nil, err
		}

		for _, m := range members {
			if _, ok := destMap[utils.Bytes2String(m)]; !ok {
				continue
			} else if ok {
				delete(destMap, utils.Bytes2String(m))
			}
		}
		// O - A = O, O is zero set.
		if len(destMap) == 0 {
			return nil, nil
		}
	}

	slice := make([][]byte, len(destMap))
	idx := 0
	for k, v := range destMap {
		if !v {
			continue
		}
		slice[idx] = []byte(k)
		idx++
	}

	return slice, nil
}

func (db *DBSet) SDiff(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sDiffGeneric(ctx, txn, keys...)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([][]byte), nil
}

func (db *DBSet) sStoreGeneric(ctx context.Context, txn *transaction.KVTxn, dstKey []byte, optType byte, keys ...[]byte) (int64, error) {
	var err error
	var ek []byte
	var v [][]byte

	switch optType {
	case UnionType:
		v, err = db.sUnionGeneric(ctx, txn, keys...)
	case DiffType:
		v, err = db.sDiffGeneric(ctx, txn, keys...)
	case InterType:
		v, err = db.sInterGeneric(ctx, txn, keys...)
	}
	if err != nil {
		return 0, err
	}

	if _, err := db.delete(ctx, txn, dstKey); err != nil {
		return 0, err
	}

	for _, m := range v {
		if err := checkSetKMSize(ctx, dstKey, m); err != nil {
			return 0, err
		}

		ek = db.sEncodeSetKey(dstKey, m)
		if _, err := db.kvClient.GetKVClient().Get(ctx, ek); err != nil {
			return 0, err
		}

		if err := txn.Set(ek, nil); err != nil {
			return 0, err
		}
	}

	var n = int64(len(v))
	sk := db.sEncodeSizeKey(dstKey)
	if err := txn.Set(sk, PutInt64(n)); err != nil {
		return 0, err
	}

	return n, nil
}

func (db *DBSet) SDiffStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sStoreGeneric(ctx, txn, dstKey, DiffType, keys...)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBSet) sInterGeneric(ctx context.Context, txn *transaction.KVTxn, keys ...[]byte) ([][]byte, error) {
	destMap := make(map[string]bool)

	members, err := db.sMembers(ctx, txn, keys[0])
	if err != nil {
		return nil, err
	}

	for _, m := range members {
		destMap[utils.Bytes2String(m)] = true
	}

	for _, key := range keys[1:] {
		if err := checkKeySize(key); err != nil {
			return nil, err
		}

		members, err := db.sMembers(ctx, txn, key)
		if err != nil {
			return nil, err
		} else if len(members) == 0 {
			return nil, err
		}

		tempMap := make(map[string]bool)
		for _, member := range members {
			if err := checkKeySize(member); err != nil {
				return nil, err
			}
			if _, ok := destMap[utils.Bytes2String(member)]; ok {
				tempMap[utils.Bytes2String(member)] = true //mark this item as selected
			}
		}
		destMap = tempMap //reduce the size of the result set
		if len(destMap) == 0 {
			return nil, nil
		}
	}

	slice := make([][]byte, len(destMap))
	idx := 0
	for k, v := range destMap {
		if !v {
			continue
		}

		slice[idx] = []byte(k)
		idx++
	}

	return slice, nil
}

func (db *DBSet) SInter(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sInterGeneric(ctx, txn, keys...)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([][]byte), nil
}

func (db *DBSet) SInterStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sStoreGeneric(ctx, txn, dstKey, InterType, keys...)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.(int64), nil
}

func (db *DBSet) SIsMember(ctx context.Context, key []byte, member []byte) (int64, error) {
	ek := db.sEncodeSetKey(key, member)

	var n int64 = 1
	if v, err := db.kvClient.GetKVClient().Get(ctx, ek); err != nil {
		return 0, err
	} else if v == nil {
		n = 0
	}
	return n, nil
}

func (db *DBSet) SMembers(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	start := db.sEncodeStartKey(key)
	stop := db.sEncodeStopKey(key)
	keys, _, err := db.kvClient.GetKVClient().Scan(ctx, start, stop, rawkv.MaxRawKVScanLimit)
	if err != nil {
		return nil, err
	}

	v := make([][]byte, 0, 16)
	for i := 0; i < len(keys); i++ {
		_, m, err := db.sDecodeSetKey(keys[i])
		if err != nil {
			return nil, err
		}
		v = append(v, m)
	}

	return v, nil
}

func (db *DBSet) SRem(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(ctx, key, args[i]); err != nil {
			return 0, err
		}
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var ek []byte
		var num int64
		for i := 0; i < len(args); i++ {
			ek = db.sEncodeSetKey(key, args[i])
			v, err := txn.Get(ctx, ek)
			if err != nil {
				return 0, err
			}
			if v == nil {
				continue
			} else {
				num++
				if err := txn.Delete(ek); err != nil {
					return 0, err
				}
			}
		}

		if _, err := db.sIncrSize(ctx, txn, key, -num); err != nil {
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

func (db *DBSet) sUnionGeneric(ctx context.Context, txn *transaction.KVTxn, keys ...[]byte) ([][]byte, error) {
	dstMap := make(map[string]bool)
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return nil, err
		}

		members, err := db.sMembers(ctx, txn, key)
		if err != nil {
			return nil, err
		}

		for _, member := range members {
			dstMap[utils.Bytes2String(member)] = true
		}
	}

	slice := make([][]byte, len(dstMap))
	idx := 0
	for k, v := range dstMap {
		if !v {
			continue
		}
		slice[idx] = []byte(k)
		idx++
	}

	return slice, nil
}

func (db *DBSet) SUnion(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sUnionGeneric(ctx, txn, keys...)
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	return res.([][]byte), nil
}

func (db *DBSet) SUnionStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.sStoreGeneric(ctx, txn, dstKey, UnionType, keys...)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), nil
}

func (db *DBSet) delete(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error) {
	start := db.sEncodeStartKey(key)
	stop := db.sEncodeStopKey(key)
	it, err := txn.Iter(start, stop)
	if err != nil {
		return
	}
	for ; it.Valid(); it.Next() {
		if err = txn.Delete(it.Key()); err != nil {
			return
		}
		num++
	}
	it.Close()

	sk := db.sEncodeSizeKey(key)
	if err = txn.Delete(sk); err != nil {
		return 0, err
	}

	return num, nil
}

func (db *DBSet) Del(ctx context.Context, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
	}

	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		for _, key := range keys {
			if err := checkKeySize(key); err != nil {
				return 0, err
			}

			_, err := db.delete(ctx, txn, key)
			if err != nil {
				return 0, err
			}
			_, err = db.rmExpire(ctx, txn, SetType, key)
			if err != nil {
				return 0, err
			}
		}
		return int64(len(keys)), nil
	})
	if err != nil {
		return 0, err
	}

	return int64(len(keys)), nil
}

func (db *DBSet) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.sEncodeSizeKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBSet) sExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		if scnt, err := db.SCard(ctx, key); err != nil || scnt == 0 {
			return 0, err
		}
		if err := db.expireAt(txn, SetType, key, when); err != nil {
			return 0, err
		}
		return 1, nil
	})
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, time.Now().Unix()+duration)
}

func (db *DBSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, when)
}

func (db *DBSet) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(ctx, SetType, key)
}

func (db *DBSet) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.rmExpire(ctx, txn, SetType, key)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), err
}
