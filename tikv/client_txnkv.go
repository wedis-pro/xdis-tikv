package tikv

import (
	"context"
	"strconv"
	"strings"

	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-tikv/v1/config"
)

type TxnKVClientWrapper struct {
	opts   *config.TikvClientOptions
	client *txnkv.Client
}

func NewTxKVClient(opts *config.TikvClientOptions) (*TxnKVClientWrapper, error) {
	txnCli, err := txnkv.NewClient(strings.Split(opts.PDAddrs, ","))
	if err != nil {
		return nil, err
	}

	cli := &TxnKVClientWrapper{
		opts:   opts,
		client: txnCli,
	}

	return cli, nil
}

func (m *TxnKVClientWrapper) Close() error {
	return m.client.Close()
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

	tx, err := m.client.Begin()
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
	tx, err := m.client.Begin()
	if err != nil {
		return
	}

	return tx.Get(ctx, key)
}

// Incr txn incr
func (m *TxnKVClientWrapper) Incr(ctx context.Context, key []byte, delta int64) (res int64, err error) {
	val, err := m.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		n, err := utils.StrInt64(txn.Get(ctx, key))
		if err != nil {
			return 0, err
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

func (m *TxnKVClientWrapper) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *TxnKVClientWrapper) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
