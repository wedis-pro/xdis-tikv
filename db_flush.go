package xdistikv

import (
	"context"
	"fmt"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// FlushDB flushes the data in select db.
func (db *DB) FlushDB(ctx context.Context) (drop int64, err error) {
	return db.flushDBByScanTypeData(ctx)
}

func (db *DB) flushDBByScanTypeData(ctx context.Context) (drop int64, err error) {
	dataTypes := []byte{
		StringType,
		ListType,
		HashType,
		SetType,
		ZSetType,
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
		klog.CtxInfof(ctx, "flush index %d dataType %s ok, flush cn %d", db.index, TypeName[dataType], res.(int64))
		drop += res.(int64)
	}
	klog.CtxInfof(ctx, "flush db index %d ok, total flush cn %d", db.index, drop)

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
	keys, err = db.scanGeneric(ctx, txn, metaDataType, nil, ScanOnceNums, false, "", false)
	for len(keys) != 0 && err == nil {
		klog.CtxDebugf(ctx, "flush db index %d ok, type %s keyslen %d", db.index, TypeName[dataType], len(keys))
		for i, key := range keys {
			if n, err := deleteFunc(ctx, txn, key); err != nil {
				return 0, err
			} else if n > 0 {
				drop++
			}
			if _, err = db.rmExpire(ctx, txn, dataType, key); err != nil {
				return 0, err
			}
			_ = i
		}

		//drop += int64(len(keys))
		keys, err = db.scanGeneric(ctx, txn, metaDataType, nil, ScanOnceNums, false, "", false)
	}
	return
}
