package xdistikv

import (
	"context"
	"time"

	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/driver"
	openkvdriver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/xdis-tikv/tikv"
)

type DBString struct {
	*DB
}

func NewDBString(db *DB) *DBString {
	return &DBString{DB: db}
}

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return ErrValueSize
	}
	return nil
}

func (db *DBString) Set(ctx context.Context, key []byte, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}
	if err := checkValueSize(value); err != nil {
		return err
	}

	key = db.encodeStringKey(key)
	err := db.kvClient.GetKVClient().Put(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (db *DBString) SetNX(ctx context.Context, key []byte, value []byte) (n int64, err error) {
	if err = checkKeySize(key); err != nil {
		return
	}
	if err = checkValueSize(value); err != nil {
		return
	}

	n = 1
	key = db.encodeStringKey(key)
	err = db.kvClient.GetKVClient().PutNotExists(ctx, key, value)
	if err != nil {
		if err == tikv.ErrKeyExist {
			return 0, nil
		}
		return
	}

	return
}

func (db *DBString) SetEX(ctx context.Context, key []byte, duration int64, value []byte) (err error) {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	} else if duration <= 0 {
		return ErrExpireValue
	}

	_, err = db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		ek := db.encodeStringKey(key)
		err := txn.Set(ek, value)
		if err != nil {
			return nil, err
		}
		err = db.expireAt(txn, StringType, key, time.Now().Unix()+duration)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})

	return
}

func (db *DBString) SetNXEX(ctx context.Context, key []byte, duration int64, value []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	} else if duration <= 0 {
		return 0, ErrExpireValue
	}

	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		ek := db.encodeStringKey(key)
		if v, err := txn.Get(ctx, ek); err != nil {
			return nil, err
		} else if v != nil {
			return nil, nil
		}

		err := txn.Set(ek, value)
		if err != nil {
			return nil, err
		}
		err = db.expireAt(txn, StringType, key, time.Now().Unix()+duration)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBString) SetXXEX(ctx context.Context, key []byte, duration int64, value []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	} else if duration <= 0 {
		return 0, ErrExpireValue
	}

	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		ek := db.encodeStringKey(key)
		if v, err := txn.Get(ctx, ek); err != nil {
			return nil, err
		} else if v == nil {
			return nil, nil
		}

		err := txn.Set(ek, value)
		if err != nil {
			return nil, err
		}
		err = db.expireAt(txn, StringType, key, time.Now().Unix()+duration)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBString) Get(ctx context.Context, key []byte) (val []byte, err error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeStringKey(key)
	return db.kvClient.GetKVClient().Get(ctx, key)
}

func (db *DBString) GetSlice(ctx context.Context, key []byte) (openkvdriver.ISlice, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeStringKey(key)
	val, err := db.kvClient.GetKVClient().Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return openkvdriver.GoSlice(val), nil
}

func (db *DBString) GetSet(ctx context.Context, key []byte, value []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}
	if err := checkValueSize(value); err != nil {
		return nil, err
	}

	ekey := db.encodeStringKey(key)
	oldVal, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		oldVal, err := txn.Get(ctx, ekey)
		if err != nil {
			return nil, err
		}
		err = txn.Set(ekey, value)
		if err != nil {
			return nil, err
		}

		return oldVal, nil
	})

	if oldVal == nil {
		return nil, nil
	}

	return oldVal.([]byte), err
}

func (db *DBString) incr(ctx context.Context, key []byte, delta int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeStringKey(key)
	return db.kvClient.GetKVClient().Incr(ctx, key, delta)
}

func (db *DBString) Incr(ctx context.Context, key []byte) (int64, error) {
	return db.incr(ctx, key, 1)
}
func (db *DBString) IncrBy(ctx context.Context, key []byte, increment int64) (int64, error) {
	return db.incr(ctx, key, increment)
}
func (db *DBString) Decr(ctx context.Context, key []byte) (int64, error) {
	return db.incr(ctx, key, -1)
}
func (db *DBString) DecrBy(ctx context.Context, key []byte, decrement int64) (int64, error) {
	return db.incr(ctx, key, -decrement)
}

func (db *DBString) MGet(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	ekeys := make([][]byte, len(keys))
	for i := 0; i < len(keys); i++ {
		ekeys[i] = db.encodeStringKey(keys[i])
	}

	return db.kvClient.GetKVClient().BatchGet(ctx, ekeys)
}

func (db *DBString) MSet(ctx context.Context, args ...driver.KVPair) error {
	if len(args) == 0 {
		return nil
	}

	ekeys := make([][]byte, len(args))
	values := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		if err := checkKeySize(args[i].Key); err != nil {
			return err
		} else if err := checkValueSize(args[i].Value); err != nil {
			return err
		}

		ekeys[i] = db.encodeStringKey(args[i].Key)
		values[i] = args[i].Value
	}

	return db.kvClient.GetKVClient().BatchPut(ctx, ekeys, values)
}

func (db *DBString) SetRange(ctx context.Context, key []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > MaxValueSize {
		return 0, ErrValueSize
	}

	ekey := db.encodeStringKey(key)
	oldValLen, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		oldValue, err := txn.Get(ctx, ekey)
		if err != nil {
			return 0, err
		}

		extra := offset + len(value) - len(oldValue)
		if extra > 0 {
			oldValue = append(oldValue, make([]byte, extra)...)
		}

		copy(oldValue[offset:], value)
		err = txn.Set(ekey, value)
		if err != nil {
			return 0, err
		}

		return int64(len(oldValue)), nil
	})
	if err != nil {
		return 0, err
	}

	if oldValLen == nil {
		return 0, nil
	}

	return oldValLen.(int64), err
}

func getRange(start int, end int, valLen int) (int, int) {
	if start < 0 {
		start = valLen + start
	}

	if end < 0 {
		end = valLen + end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= valLen {
		end = valLen - 1
	}
	return start, end
}
func (db *DBString) GetRange(ctx context.Context, key []byte, start int, end int) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	ekey := db.encodeStringKey(key)
	value, err := db.kvClient.GetKVClient().Get(ctx, ekey)
	if err != nil {
		return nil, err
	}

	valLen := len(value)
	start, end = getRange(start, end, valLen)
	if start > end {
		return nil, nil
	}

	return value[start : end+1], nil
}

func (db *DBString) StrLen(ctx context.Context, key []byte) (int64, error) {
	s, err := db.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	return int64(len(s)), nil
}

func (db *DBString) Append(ctx context.Context, key []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	ekey := db.encodeStringKey(key)
	oldValLen, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		oldValue, err := txn.Get(ctx, ekey)
		if err != nil {
			return 0, err
		}
		if len(oldValue)+len(value) > MaxValueSize {
			return 0, ErrValueSize
		}

		oldValue = append(oldValue, value...)
		err = txn.Set(ekey, oldValue)
		if err != nil {
			return 0, err
		}

		return int64(len(oldValue)), nil
	})
	if err != nil {
		return 0, err
	}
	if oldValLen == nil {
		return 0, nil
	}

	return oldValLen.(int64), err
}

func (db *DBString) delete(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error) {
	ek := db.encodeBitmapKey(key)
	if err = txn.Delete(ek); err != nil {
		return
	}

	return 1, nil
}

// Del must atomic txn del
func (db *DBString) Del(ctx context.Context, keys ...[]byte) (int64, error) {
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
		}
		return nums, nil
	})
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (db *DBString) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeStringKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, key)
	if v != nil && err == nil {
		return 1, nil
	}

	return 0, err
}

func (db *DBString) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, time.Now().Unix()+duration)
}

func (db *DBString) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, when)
}

func (db *DBString) setExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	_, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		exist, err := db.Exists(ctx, key)
		if err != nil || exist == 0 {
			return 0, err
		}

		err = db.expireAt(txn, StringType, key, when)
		if err != nil {
			return 0, err
		}

		return 1, nil
	})
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (db *DBString) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.encodeStringKey(key)
	v, err := db.kvClient.GetKVClient().Get(ctx, sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(ctx, StringType, key)
}

func (db *DBString) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return db.rmExpire(ctx, txn, StringType, key)
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}

	return res.(int64), err
}
