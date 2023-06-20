package tikv

import (
	"bytes"
	"context"
	"strconv"
	"strings"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-tikv/config"
	"github.com/weedge/xdis-tikv/driver"
)

type TxnKVClientWrapper struct {
	opts *config.TikvClientOptions
	*txnkv.Client
	//onceTxn *sync.Once
}

func NewTxKVClient(opts *config.TikvClientOptions) (*TxnKVClientWrapper, error) {
	txnCli, err := txnkv.NewClient(strings.Split(opts.PDAddrs, ","))
	if err != nil {
		return nil, err
	}

	cli := &TxnKVClientWrapper{
		opts:   opts,
		Client: txnCli,
		//onceTxn: &sync.Once{},
	}

	return cli, nil
}

func (m *TxnKVClientWrapper) Close() error {
	if m.Client == nil {
		return nil
	}
	return m.Client.Close()
}

func (m *TxnKVClientWrapper) BeginOnceTxn(txnOpts ...TxnOpt) (txn *transaction.KVTxn, err error) {
	return m.Client.Begin()
}

type TxHandle func(txn *transaction.KVTxn) (interface{}, error)

func (m *TxnKVClientWrapper) ExecuteTxn(ctx context.Context, handle TxHandle, txnOpts ...TxnOpt) (res interface{}, err error) {
	opts := &options{
		tryOnePcCommit: m.opts.TryOnePcCommit,
		asyncCommit:    m.opts.UseAsyncCommit,
		pessimisticTxn: m.opts.UsePessimisticTxn,
	}
	for _, opt := range txnOpts {
		opt(opts)
	}

	tx, err := m.Client.Begin()
	if err != nil {
		return
	}

	tx.SetEnable1PC(opts.tryOnePcCommit)
	tx.SetEnableAsyncCommit(opts.asyncCommit)
	tx.SetPessimistic(opts.pessimisticTxn)

	res, err = handle(tx)
	if err != nil {
		tx.Rollback()
		return
	}

	err = tx.Commit(ctx)
	return
}

func (m *TxnKVClientWrapper) Put(ctx context.Context, key, value []byte) error {
	_, err := m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		err := txn.Set(key, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, WithAsyncCommit(true), WithTryOnePcCommit(true))

	return err
}

func (m *TxnKVClientWrapper) PutNotExists(ctx context.Context, key, value []byte) (err error) {
	_, err = m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		v, err := txn.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if v != nil {
			return nil, ErrKeyExist
		}

		err = txn.Set(key, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, WithAsyncCommit(true), WithTryOnePcCommit(true))

	return
}

func (m *TxnKVClientWrapper) Get(ctx context.Context, key []byte) (val []byte, err error) {
	tx, err := m.Client.Begin()
	if err != nil {
		return
	}

	val, err = tx.Get(ctx, key)
	if tikverr.IsErrNotFound(err) {
		return nil, nil
	}

	return
}

// Incr txn incr
func (m *TxnKVClientWrapper) Incr(ctx context.Context, key []byte, delta int64) (res int64, err error) {
	val, err := m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		n, err := utils.StrInt64(txn.Get(ctx, key))
		if err != nil {
			return 0, ErrValueIntOutOfRange
		}
		n += delta
		err = txn.Set(key, strconv.AppendInt(nil, n, 10))
		if err != nil {
			return 0, err
		}
		return n, nil
	})
	if err != nil {
		return
	}
	if val == nil {
		return
	}

	res = val.(int64)
	return
}

func (m *TxnKVClientWrapper) BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error) {
	res, err := m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		return txn.BatchGet(ctx, keys)
	})
	if err != nil {
		return
	}
	if res == nil {
		return
	}

	mapRes := res.(map[string][]byte)
	vals = make([][]byte, len(keys))
	for i := range keys {
		if val, ok := mapRes[utils.Bytes2String(keys[i])]; ok {
			vals[i] = val
		} else {
			vals[i] = nil
		}
	}

	return
}

func (m *TxnKVClientWrapper) BatchPut(ctx context.Context, keys, values [][]byte) error {
	_, err := m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		for i := 0; i < len(keys); i++ {
			err := txn.Set(keys[i], values[i])
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	return err
}

func (m *TxnKVClientWrapper) Scan(ctx context.Context, minKey, maxKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	txn, err := m.Client.Begin()
	if err != nil {
		return nil, nil, err
	}
	it, err := txn.Iter(minKey, maxKey)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	keys = [][]byte{}
	values = [][]byte{}
	for it.Valid() && limit > 0 {
		keys = append(keys, it.Key()[:])
		values = append(values, it.Value()[:])
		limit--
		it.Next()
	}

	return
}

func (m *TxnKVClientWrapper) ReverseScan(ctx context.Context, minKey, maxKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	txn, err := m.Client.Begin()
	if err != nil {
		return nil, nil, err
	}
	it, err := txn.IterReverse(maxKey)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	keys = [][]byte{}
	values = [][]byte{}
	for it.Valid() && limit > 0 {
		if bytes.Compare(minKey, it.Key()) > 0 {
			break
		}
		keys = append(keys, it.Key()[:])
		values = append(values, it.Value()[:])
		limit--
		it.Next()
	}

	return
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *TxnKVClientWrapper) Iter(ctx context.Context, txn *transaction.KVTxn, minKey, maxKey []byte, offset, limit int) (iter driver.IIterator, err error) {
	if txn == nil {
		txn, err = m.Client.Begin()
		if err != nil {
			return nil, err
		}
	}
	it, err := txn.Iter(minKey, maxKey)
	if err != nil {
		return nil, err
	}
	txnIter := &RangeIter{
		it:        it,
		minKey:    minKey,
		maxKey:    maxKey,
		limit:     limit,
		offset:    offset,
		isReverse: false,
		step:      0,
		txn:       txn,
	}
	iter = txnIter.Offset()
	return
}

func (m *TxnKVClientWrapper) ReverseIter(ctx context.Context, txn *transaction.KVTxn, minKey, maxKey []byte, offset, limit int) (iter driver.IIterator, err error) {
	if txn == nil {
		txn, err = m.Client.Begin()
		if err != nil {
			return nil, err
		}
	}
	it, err := txn.IterReverse(maxKey)
	if err != nil {
		return nil, err
	}
	txnIter := &RangeIter{
		it:        it,
		minKey:    minKey,
		maxKey:    maxKey,
		limit:     limit,
		offset:    offset,
		isReverse: true,
		step:      0,
		txn:       txn,
	}
	iter = txnIter.Offset()
	return
}
