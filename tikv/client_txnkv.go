package tikv

import (
	"context"
	"strings"

	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/xdis-tikv/v1/config"
)

type TxnKVClientWrapper struct {
	client  *txnkv.Client
	retries int
}

func NewTxKVClient(opts *config.TikvClientOptions) (*TxnKVClientWrapper, error) {
	txnCli, err := txnkv.NewClient(strings.Split(opts.PDAddrs, ","))
	if err != nil {
		return nil, err
	}

	cli := &TxnKVClientWrapper{
		client:  txnCli,
		retries: opts.TxnRetryCn,
	}

	return cli, nil
}

type TxHandle func(txn *transaction.KVTxn) (interface{}, error)

func (m *TxnKVClientWrapper) ExecuteTxn(ctx context.Context, handle TxHandle) (res interface{}, err error) {
	tx, err := m.client.Begin()
	if err != nil {
		return
	}

	res, err = handle(tx)
	if err != nil {
		tx.Rollback()
		return
	}

	err = tx.Commit(ctx)

	return
}

func (m *TxnKVClientWrapper) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	tx, err := m.client.Begin()
	if err != nil {
		return err
	}

	err = tx.Set(key, value)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit(ctx)

	return err
}

func (m *TxnKVClientWrapper) PutNotExists(ctx context.Context, key, value []byte) (err error) {
	tx, err := m.client.Begin()
	if err != nil {
		return err
	}

	v, err := tx.Get(ctx, key)
	if err != nil {
		return err
	}
	if v != nil {
		return ErrKeyExist
	}

	err = tx.Set(key, value)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit(ctx)

	return
}

func (m *TxnKVClientWrapper) Get(ctx context.Context, key []byte) (val []byte, err error)
func (m *TxnKVClientWrapper) BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error)
func (m *TxnKVClientWrapper) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error
func (m *TxnKVClientWrapper) Delete(ctx context.Context, key []byte) error
func (m *TxnKVClientWrapper) BatchDelete(ctx context.Context, keys [][]byte) error
func (m *TxnKVClientWrapper) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *TxnKVClientWrapper) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *TxnKVClientWrapper) Close() error
