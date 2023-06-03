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
	"github.com/weedge/xdis-tikv/v1/config"
	"github.com/weedge/xdis-tikv/v1/tikv"
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

	return
}

func (m *Storager) InitOpts(opts *config.StoragerOptions) {
	if opts.Databases == 0 {
		opts.Databases = config.DefaultDatabases
	} else if opts.Databases > MaxDatabases {
		opts.Databases = MaxDatabases
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

func (m *Storager) checkTTL() {
	m.ttlCheckers = make([]*TTLChecker, 0, config.DefaultDatabases)
	m.ttlCheckerCh = make(chan *TTLChecker, config.DefaultDatabases)

	if m.opts.TTLCheckInterval < 0 {
		m.opts.TTLCheckInterval = config.DefaultTTLCheckInterval
	}

	safer.GoSafely(&m.wg, false, func() {
		tick := time.NewTicker(time.Duration(m.opts.TTLCheckInterval) * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				for _, c := range m.ttlCheckers {
					c.check()
				}
			case c := <-m.ttlCheckerCh:
				m.ttlCheckers = append(m.ttlCheckers, c)
				c.check()
			case <-m.quit:
				return
			}
		}
	}, nil, os.Stderr)
}

// FlushAll will clear all data
func (m *Storager) FlushAll(ctx context.Context) error {
	// todo
	return nil
}
