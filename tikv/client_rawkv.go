package tikv

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/rawkv"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-tikv/v1/config"
)

type RawKVClientWrapper struct {
	client *rawkv.Client
}

func NewRawKVClient(opts *config.TikvClientOptions) (*RawKVClientWrapper, error) {
	rawCli, err := rawkv.NewClientWithOpts(context.Background(), strings.Split(opts.PDAddrs, ","))
	if err != nil {
		return nil, err
	}

	cli := &RawKVClientWrapper{
		client: rawCli,
	}

	return cli, nil
}
func (m *RawKVClientWrapper) Close() error {
	return m.client.Close()
}

func (m *RawKVClientWrapper) Put(ctx context.Context, key, value []byte) error {
	err := m.client.Put(ctx, key, value)
	return err
}

func (m *RawKVClientWrapper) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	err := m.client.PutWithTTL(ctx, key, value, ttl)
	return err
}

func (m *RawKVClientWrapper) PutNotExists(ctx context.Context, key, value []byte) error {
	_, swapped, err := m.client.CompareAndSwap(ctx, key, nil, value)
	if err != nil {
		return err
	}
	if !swapped {
		return ErrKeyExist
	}
	return nil
}

func (m *RawKVClientWrapper) Get(ctx context.Context, key []byte) (val []byte, err error) {
	return m.client.Get(ctx, key)
}

// Incr CAS incr
func (m *RawKVClientWrapper) Incr(ctx context.Context, key []byte, delta int64) (res int64, err error) {
	swapped := false
	var newInt int64
	for i := 0; i < MaxCASLoopCn; i++ {
		var val []byte
		var preInt int64
		val, err = m.client.Get(ctx, key)
		preInt, err = utils.StrInt64(val, err)
		if err != nil {
			return
		}
		newInt = preInt + delta
		newVal := utils.String2Bytes(strconv.FormatInt(newInt, 10))
		_, swapped, err = m.client.CompareAndSwap(ctx, key, val, newVal)
		if err != nil {
			return
		}
		if swapped {
			break
		}

		sleepTime := time.Duration(i * int(time.Millisecond))
		if i > 200 {
			sleepTime = time.Duration(200 * time.Millisecond)
		}
		time.Sleep(sleepTime)
	}

	if !swapped {
		err = ErrCASExhausted
		return
	}

	res = newInt
	return
}

func (m *RawKVClientWrapper) BatchGet(ctx context.Context, keys [][]byte) (vals [][]byte, err error) {
	return m.client.BatchGet(ctx, keys)
}

func (m *RawKVClientWrapper) BatchPut(ctx context.Context, keys, values [][]byte) error {
	return m.client.BatchPut(ctx, keys, values)
}

func (m *RawKVClientWrapper) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64) error {
	return m.client.BatchPutWithTTL(ctx, keys, values, ttls)
}

func (m *RawKVClientWrapper) BatchDelete(ctx context.Context, keys [][]byte) error {
	return m.client.BatchDelete(ctx, keys)
}

func (m *RawKVClientWrapper) Delete(ctx context.Context, key []byte) error
func (m *RawKVClientWrapper) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
func (m *RawKVClientWrapper) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error)
