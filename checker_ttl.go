package xdistikv

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/xdis-tikv/v1/config"
)

type onExpired func(ctx context.Context, txn *transaction.KVTxn, key []byte) (num int64, err error)

type TTLChecker struct {
	db *DB

	cbs []onExpired

	//next check time
	nc int64
	sync.RWMutex
}

func NewTTLChecker(db *DB) *TTLChecker {
	c := &TTLChecker{
		db: db,
		nc: 0,
	}

	// todo: register data type with on expired event
	c.register(StringType, db.string.delete)
	c.register(ListType, db.list.delete)
	c.register(HashType, db.hash.delete)
	c.register(SetType, db.set.delete)
	c.register(ZSetType, db.zset.delete)
	c.register(BitmapType, db.bitmap.delete)

	return c
}

func (c *TTLChecker) register(dataType byte, handle onExpired) {
	c.cbs[dataType] = handle
}

func (c *TTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *TTLChecker) Run(ctx context.Context) {
	now := time.Now().Unix()

	c.Lock()
	when := c.nc
	c.Unlock()

	if now < when {
		return
	}

	when = now + config.MaxTTLCheckInterval

	newWhen, err := c.clearExpired(ctx, when, now)
	if err != nil {
		klog.CtxErrorf(ctx, "clearExpired err %s", err.Error())
		return
	}

	c.setNextCheckTime(newWhen, true)
}

func (c *TTLChecker) clearExpired(ctx context.Context, when, now int64) (nextWhen int64, err error) {
	minKey := c.db.expEncodeTimeKey(NoneType, nil, 0)
	maxKey := c.db.expEncodeTimeKey(maxDataType, nil, when)

	it, err := c.db.kvClient.GetTxnKVClient().Iter(ctx, nil, minKey, maxKey, 0, -1)
	if err != nil {
		return when, err
	}
	for ; it.Valid(); it.Next() {
		tk := it.Key()
		mk := it.Value()
		dt, k, nt, err := c.db.expDecodeTimeKey(tk)
		if err != nil {
			continue
		}

		if nt > now {
			//the next ttl check time is nt!
			nextWhen = nt
			break
		}

		cb := c.cbs[dt]
		if cb == nil {
			continue
		}

		_, err = c.db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
			exp, err := Int64(txn.Get(ctx, mk))
			if err != nil {
				return nil, err
			}
			// check expire again
			if exp > now {
				return nil, nil
			}
			if _, err = cb(ctx, txn, k); err != nil {
				return nil, err
			}
			if err = txn.Delete(tk); err != nil {
				return nil, err
			}
			if err = txn.Delete(mk); err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			return nextWhen, err
		}
	}

	return
}
