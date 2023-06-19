package xdistikv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/safer"
	"github.com/weedge/xdis-tikv/config"
	tDriver "github.com/weedge/xdis-tikv/driver"
	"github.com/weedge/xdis-tikv/tikv"
)

// Storager core store struct for server use like redis
type Storager struct {
	opts *config.StoragerOptions

	// tikv store client
	kvClient *tikv.Client

	// multi storager db instances on one kv store engine
	dbs map[int]*DB
	// dbs map lock for get and set map[int]*DB
	dbLock sync.Mutex

	// ttl check
	ttlCheckers  []*TTLChecker
	ttlCheckerCh chan *TTLChecker
	wg           sync.WaitGroup
	quit         chan struct{}

	// leader check
	leaderChecker *LeaderChecker
	// gc check
	gcChecker *GCChecker
	// biz config prefix key for logic isolation
	prefixKey []byte
}

func Open(opts *config.StoragerOptions) (store *Storager, err error) {
	store = &Storager{}
	store.InitOpts(opts)

	defer func(s *Storager) {
		if err != nil {
			if e := s.Close(); e != nil {
				klog.Errorf("close store err: %s", e.Error())
			}
		}
	}(store)

	store.dbs = make(map[int]*DB, opts.Databases)
	store.quit = make(chan struct{})

	if store.kvClient, err = tikv.NewClient(&opts.TiKVClient); err != nil {
		return nil, err
	}

	store.SetPrefix(store.opts.PrefixKey)

	store.check(context.Background())
	return
}

func (m *Storager) InitOpts(opts *config.StoragerOptions) {
	if opts.Databases == 0 {
		opts.Databases = config.DefaultDatabases
	} else if opts.Databases > MaxDatabases {
		opts.Databases = MaxDatabases
	}

	if opts.TTLCheckInterval < 0 {
		opts.TTLCheckInterval = config.DefaultTTLCheckInterval
	}
	if opts.TTLCheckInterval > config.MaxTTLCheckInterval-config.DefaultTTLCheckInterval {
		opts.TTLCheckInterval = config.MaxTTLCheckInterval - config.DefaultTTLCheckInterval
	}

	m.opts = opts
}

// Close close tikv client
func (m *Storager) Close() (err error) {
	if m.quit != nil {
		close(m.quit)
	}
	m.wg.Wait()

	errs := []error{}
	if m.kvClient != nil {
		errs = append(errs, m.kvClient.Close())
		m.kvClient = nil
	}

	for _, db := range m.dbs {
		errs = append(errs, db.Close())
	}

	errStrs := []string{}
	for _, er := range errs {
		if er != nil {
			errStrs = append(errStrs, er.Error())
		}
	}
	if len(errStrs) > 0 {
		err = fmt.Errorf("errs: %s", strings.Join(errStrs, " | "))
	}
	return
}

// Select chooses a database.
func (m *Storager) Select(ctx context.Context, index int) (idb driver.IDB, err error) {
	if index < 0 || index >= m.opts.Databases {
		return nil, fmt.Errorf("invalid db index %d, must in [0, %d]", index, m.opts.Databases-1)
	}

	m.dbLock.Lock()
	db, ok := m.dbs[index]
	if ok {
		idb = db
		m.dbLock.Unlock()
		return
	}
	db = NewDB(m, index)
	m.dbs[index] = db

	// async send checker,
	// if recv checkTTL tick to check,ch full, maybe block
	go func(db *DB) {
		m.ttlCheckerCh <- db.ttlChecker
	}(db)

	idb = db
	m.dbLock.Unlock()

	return
}

// check checker job to run
func (m *Storager) check(ctx context.Context) {
	m.checkTTL(ctx)
	m.checkLeaderAndGC(ctx)
}

func (m *Storager) checkTTL(ctx context.Context) {
	m.ttlCheckers = make([]*TTLChecker, 0, config.DefaultDatabases)
	m.ttlCheckerCh = make(chan *TTLChecker, config.DefaultDatabases)
	safer.GoSafely(&m.wg, false, func() {
		tick := time.NewTicker(time.Duration(m.opts.TTLCheckInterval) * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				for _, c := range m.ttlCheckers {
					c.Run(ctx)
				}
			case c := <-m.ttlCheckerCh:
				m.ttlCheckers = append(m.ttlCheckers, c)
				c.Run(ctx)
			case <-m.quit:
				return
			}
		}
	}, nil, os.Stderr)
}

func (m *Storager) checkLeaderAndGC(ctx context.Context) {
	m.leaderChecker = NewLeaderChecker(&m.opts.LeaderJob, m.kvClient)
	safer.GoSafely(&m.wg, false, func() {
		m.leaderChecker.Run(ctx)
	}, nil, os.Stderr)

	m.gcChecker = NewGCChecker(&m.opts.GCJob, m.kvClient, m.leaderChecker)
	safer.GoSafely(&m.wg, false, func() {
		m.gcChecker.Run(ctx)
	}, nil, os.Stderr)
}

// SetPrefix set the prefix key.
func (m *Storager) SetPrefix(prefix string) {
	m.prefixKey = encodePrefixKey(prefix)
}

// PrefixKey get the prefix key
func (m *Storager) PrefixKey() []byte {
	return m.prefixKey
}

// FlushAll will clear all data
// if use shared dist tikv , need prefix key to logic isolation
// use namespace/tenantId(appId/bizId);
func (m *Storager) FlushAll(ctx context.Context) (err error) {
	var iter tDriver.IIterator
	iter, err = m.kvClient.GetTxnKVClient().Iter(ctx, nil, m.prefixKey, nil, 0, -1)
	if err != nil {
		return err
	}
	defer iter.Close()
	it, ok := iter.(*tikv.RangeIter)
	if !ok {
		return nil
	}

	defer func() {
		if err != nil {
			it.GetTxn().Rollback()
		}
	}()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
		if n == 10000 {
			if err = it.GetTxn().Commit(ctx); err != nil {
				klog.Errorf("flush all commit error: %s", err.Error())
				return err
			}
			n = 0
		}
		it.GetTxn().Delete(it.Key())
	}

	if err = it.GetTxn().Commit(ctx); err != nil {
		klog.Errorf("flush all commit error: %s", err.Error())
		return err
	}

	return nil
}
