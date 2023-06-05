package xdistikv

import (
	"context"
	"strconv"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

type DBHash struct {
	*DB
}

func NewDBHash(db *DB) *DBHash {
	return &DBHash{DB: db}
}

func checkHashKFSize(ctx context.Context, key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(field) > MaxHashFieldSize || len(field) == 0 {
		return ErrHashFieldSize
	}
	return nil
}

func (db *DBHash) hIncrSize(ctx context.Context, txn *transaction.KVTxn, key []byte, delta int64) (size int64, err error) {
	sk := db.hEncodeSizeKey(key)
	if size, err = Int64(txn.Get(ctx, sk)); err != nil {
		return 0, err
	}

	size += delta
	if size <= 0 {
		size = 0
		if err = txn.Delete(sk); err != nil {
			return 0, err
		}
		if _, err = db.rmExpire(ctx, txn, HashType, key); err != nil {
			return 0, err
		}
	} else {
		if err = txn.Set(sk, PutInt64(size)); err != nil {
			return 0, err
		}
	}

	return
}

func (db *DBHash) hSetItem(ctx context.Context, txn *transaction.KVTxn, key []byte, field []byte, value []byte) (int64, error) {
	ek := db.hEncodeHashKey(key, field)

	var n int64 = 0
	v, err := txn.Get(ctx, ek)
	if err != nil {
		return 0, err
	}

	if v == nil {
		n = 1
		if _, err := db.hIncrSize(ctx, txn, key, n); err != nil {
			return 0, err
		}
	}

	err = txn.Set(ek, value)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (db *DBHash) HSet(ctx context.Context, key []byte, field []byte, value []byte) (int64, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.hSetItem(ctx, txn, key, field, value)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), nil
}

func (db *DBHash) HGet(ctx context.Context, key []byte, field []byte) ([]byte, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return nil, err
	}

	return db.kvClient.GetKVClient().Get(ctx, db.hEncodeHashKey(key, field))
}

func (db *DBHash) HLen(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	return Int64(db.kvClient.GetKVClient().Get(ctx, db.hEncodeSizeKey(key)))
}

func (db *DBHash) HMset(ctx context.Context, key []byte, args ...driver.FVPair) error {
	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		var num int64
		for i := 0; i < len(args); i++ {
			if err := checkHashKFSize(ctx, key, args[i].Field); err != nil {
				return nil, err
			} else if err := checkValueSize(args[i].Value); err != nil {
				return nil, err
			}

			ek := db.hEncodeHashKey(key, args[i].Field)
			if v, err := txn.Get(ctx, ek); err != nil {
				return nil, err
			} else if v == nil {
				num++
			}

			if err := txn.Set(ek, args[i].Value); err != nil {
				return nil, err
			}
		}

		if _, err := db.hIncrSize(ctx, txn, key, num); err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

func (db *DBHash) HMget(ctx context.Context, key []byte, args ...[]byte) ([][]byte, error) {
	ekeys := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(ctx, key, args[i]); err != nil {
			return nil, err
		}
		ekeys[i] = db.hEncodeHashKey(key, args[i])
	}

	return db.kvClient.GetKVClient().BatchGet(ctx, ekeys)
}

func (db *DBHash) HDel(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	ekeys := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(ctx, key, args[i]); err != nil {
			return 0, err
		}
		ekeys[i] = db.hEncodeHashKey(key, args[i])
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		mapKv, err := txn.BatchGet(ctx, ekeys)
		if err != nil {
			return 0, err
		}
		var num int64
		for k, v := range mapKv {
			if v == nil {
				continue
			}
			err := txn.Delete(utils.String2Bytes(k))
			if err != nil {
				return 0, err
			}
			num++
		}
		if _, err := db.hIncrSize(ctx, txn, key, -num); err != nil {
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

func (db *DBHash) HIncrBy(ctx context.Context, key []byte, field []byte, delta int64) (int64, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		ek := db.hEncodeHashKey(key, field)
		n, err := utils.StrInt64(txn.Get(ctx, ek))
		if err != nil {
			return 0, err
		}

		n += delta
		_, err = db.hSetItem(ctx, txn, key, field, strconv.AppendInt(nil, n, 10))
		if err != nil {
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

func (db *DBHash) HGetAll(ctx context.Context, key []byte) ([]driver.FVPair, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	v := make([]driver.FVPair, 0, 16)

	start := db.hEncodeStartKey(key)
	end := db.hEncodeStopKey(key)
	keys, values, err := db.kvClient.GetKVClient().Scan(ctx, append(start, 0), append(end, 0), rawkv.MaxRawKVScanLimit)
	if err != nil {
		return nil, err
	}

	for i, k := range keys {
		_, f, err := db.hDecodeHashKey(k)
		if err != nil {
			return nil, err
		}

		v = append(v, driver.FVPair{Field: f, Value: values[i]})
	}

	return v, nil
}

func (db *DBHash) HKeys(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	v := make([][]byte, 0, 16)

	start := db.hEncodeStartKey(key)
	end := db.hEncodeStopKey(key)
	keys, _, err := db.kvClient.GetKVClient().Scan(ctx, append(start, 0), append(end, 0), rawkv.MaxRawKVScanLimit)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		_, f, err := db.hDecodeHashKey(k)
		if err != nil {
			return nil, err
		}

		v = append(v, f)
	}

	return v, nil
}

func (db *DBHash) HValues(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	v := make([][]byte, 0, 16)

	start := db.hEncodeStartKey(key)
	end := db.hEncodeStopKey(key)
	keys, values, err := db.kvClient.GetKVClient().Scan(ctx, append(start, 0), append(end, 0), rawkv.MaxRawKVScanLimit)
	if err != nil {
		return nil, err
	}

	for i, k := range keys {
		_, _, err := db.hDecodeHashKey(k)
		if err != nil {
			return nil, err
		}

		v = append(v, values[i])
	}

	return v, nil
}

func (db *DBHash) delete(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error) {
	// range ropen: [s,e)
	start := db.hEncodeStartKey(key)
	stop := db.hEncodeStopKey(key)
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

	sk := db.hEncodeSizeKey(key)
	if err = txn.Delete(sk); err != nil {
		return 0, err
	}

	return
}

func (db *DBHash) Del(ctx context.Context, keys ...[]byte) (int64, error) {
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
			_, err = db.rmExpire(ctx, txn, HashType, key)
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

func (db *DBHash) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.hEncodeSizeKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBHash) hExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		if hlen, err := db.HLen(ctx, key); err != nil || hlen == 0 {
			return 0, err
		}

		if err := db.expireAt(txn, HashType, key, when); err != nil {
			return 0, err
		}

		return 1, nil
	})
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBHash) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.hExpireAt(ctx, key, time.Now().Unix()+duration)
}

func (db *DBHash) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.hExpireAt(ctx, key, when)
}

func (db *DBHash) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(ctx, HashType, key)

}

func (db *DBHash) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.rmExpire(ctx, txn, HashType, key)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), err
}
