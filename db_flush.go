package xdistikv

import (
	"context"
	"fmt"

	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// FlushDB flushes the data.
func (db *DB) FlushDB(ctx context.Context) (drop int64, err error) {
	dataTypes := []byte{
		StringType,
		ListType,
		HashType,
		SetType,
		ZSetType,
		BitmapType,
	}

	for _, dataType := range dataTypes {
		res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (value interface{}, err error) {
			return db.flushType(ctx, txn, dataType)
		})
		if err != nil {
			return 0, err
		}
		if res == nil {
			continue
		}
		drop += res.(int64)
	}

	return

}

func (db *DB) flushType(ctx context.Context, txn *transaction.KVTxn, dataType byte) (drop int64, err error) {
	var deleteFunc func(ctx context.Context, txn *transaction.KVTxn, key []byte) (int64, error)
	var metaDataType byte
	switch dataType {
	case StringType:
		deleteFunc = db.string.delete
		metaDataType = StringType
	case ListType:
		deleteFunc = db.list.delete
		metaDataType = LMetaType
	case HashType:
		deleteFunc = db.hash.delete
		metaDataType = HSizeType
	case SetType:
		deleteFunc = db.set.delete
		metaDataType = SSizeType
	case ZSetType:
		deleteFunc = db.zset.delete
		metaDataType = ZSizeType
	default:
		return 0, fmt.Errorf("invalid data type: %s", TypeName[dataType])
	}

	var keys [][]byte
	keys, err = db.scanGeneric(ctx, metaDataType, nil, 1024, false, "", false)
	for len(keys) != 0 || err != nil {
		for _, key := range keys {
			if _, err = deleteFunc(ctx, txn, key); err != nil {
				return 0, err
			}
			if _, err = db.rmExpire(ctx, txn, dataType, key); err != nil {
				return 0, err
			}
		}

		drop += int64(len(keys))
		keys, err = db.scanGeneric(ctx, metaDataType, nil, 1024, false, "", false)
	}
	return
}
