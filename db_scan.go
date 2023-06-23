package xdistikv

import (
	"context"

	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-tikv/driver"
)

func (db *DB) scanGeneric(ctx context.Context, txn *transaction.KVTxn, storeDataType byte, key []byte, count int,
	inclusive bool, match string, reverse bool) ([][]byte, error) {

	r, err := utils.BuildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	minKey, maxKey, err := db.buildScanKeyRange(storeDataType, key, reverse)
	if err != nil {
		return nil, err
	}

	it, err := db.buildScanIterator(ctx, txn, minKey, maxKey, inclusive, reverse)
	if err != nil {
		return nil, err
	}

	count = checkScanCount(count)
	v := make([][]byte, 0, count)
	for i := 0; it.Valid() && i < count; it.Next() {
		key := it.Key()
		if k, err := db.decodeScanKey(storeDataType, key); err != nil {
			continue
		} else if r != nil && !r.Match(k) {
			continue
		} else {
			v = append(v, k)
			i++
		}
	}
	it.Close()
	return v, nil
}

func (db *DB) buildScanKeyRange(storeDataType byte, key []byte, reverse bool) (minKey []byte, maxKey []byte, err error) {
	if !reverse {
		if minKey, err = db.encodeScanMinKey(storeDataType, key); err != nil {
			return
		}
		if maxKey, err = db.encodeScanMaxKey(storeDataType, nil); err != nil {
			return
		}
		return
	}

	if minKey, err = db.encodeScanMinKey(storeDataType, nil); err != nil {
		return
	}
	if maxKey, err = db.encodeScanMaxKey(storeDataType, key); err != nil {
		return
	}
	return
}

func checkScanCount(count int) int {
	if count <= 0 {
		count = DefaultScanCount
	}

	return count
}

func (db *DB) buildScanIterator(ctx context.Context, txn *transaction.KVTxn, min []byte, max []byte, inclusive bool, reverse bool) (iter driver.IIterator, err error) {
	// open (s,e)
	minKey := append(min, 0)
	maxKey := max

	// Ropen [s,e)
	if !reverse && inclusive {
		minKey = min
		maxKey = max
	}
	// Lopen (s,e]
	if reverse && inclusive {
		minKey = append(min, 0)
		maxKey = append(max, 0)
	}

	if !reverse {
		return db.kvClient.GetTxnKVClient().Iter(ctx, txn, minKey, maxKey, 0, -1)
	}
	return db.kvClient.GetTxnKVClient().ReverseIter(ctx, txn, minKey, maxKey, 0, -1)
}
