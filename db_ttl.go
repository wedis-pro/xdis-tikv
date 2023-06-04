package xdistikv

import (
	"context"
	"time"

	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func (db *DB) expireAt(t *transaction.KVTxn, dataType byte, key []byte, when int64) (err error) {
	mk := db.expEncodeMetaKey(dataType, key)
	tk := db.expEncodeTimeKey(dataType, key, when)

	err = t.Set(tk, mk)
	if err != nil {
		return
	}
	err = t.Set(mk, PutInt64(when))
	if err != nil {
		return
	}

	db.ttlChecker.setNextCheckTime(when, false)
	return
}

func (db *DB) ttl(ctx context.Context, dataType byte, key []byte) (t int64, err error) {
	mk := db.expEncodeMetaKey(dataType, key)

	if t, err = Int64(db.kvClient.GetKVClient().Get(ctx, mk)); err != nil || t == 0 {
		t = -1
	} else {
		t -= time.Now().Unix()
		if t <= 0 {
			t = -1
		}
	}

	return t, err
}

func (db *DB) rmExpire(ctx context.Context, t *transaction.KVTxn, dataType byte, key []byte) (int64, error) {
	mk := db.expEncodeMetaKey(dataType, key)
	v, err := db.kvClient.GetKVClient().Get(ctx, mk)
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	}

	when, err2 := Int64(v, nil)
	if err2 != nil {
		return 0, err2
	}

	tk := db.expEncodeTimeKey(dataType, key, when)
	err = t.Delete(mk)
	if err != nil {
		return 0, err
	}
	err = t.Delete(tk)
	if err != nil {
		return 0, err
	}

	return 1, nil
}
