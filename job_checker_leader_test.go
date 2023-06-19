package xdistikv

import (
	"context"
	"sync"
	"testing"

	"github.com/weedge/xdis-tikv/config"
	"github.com/weedge/xdis-tikv/tikv"
)

var (
	opts          *config.LeaderJobOptions
	kvClient      *tikv.Client
	store         *Storager
	err           error
	ctx           context.Context
	leaderChecker *LeaderChecker
)

func TestInitStorager(t *testing.T) {
	ctx = context.TODO()
	store = New(config.DefaultStoragerOptions())
}

func TestInitLeaderChecker(t *testing.T) {
	TestInitStorager(t)
	opts = config.DefaultLeaderJobOptions()
	kvClient, err = tikv.NewClient(config.DefaultTikvClientOptions())
	if err != nil {
		t.Fatalf("new tikv client err %s", err.Error())
	}
	leaderChecker = NewLeaderChecker(opts, kvClient, store)
}

func TestClose(t *testing.T) {
	TestInitStorager(t)
	err = store.Close()
	if err != nil {
		t.Fatalf("close test err %s", err.Error())
	}
}

func TestLeaderChecker_concurrency_check(t *testing.T) {
	clientCn := 10
	arrChecker := make([]*LeaderChecker, clientCn)
	arrStorager := make([]*Storager, clientCn)
	for i := 0; i < clientCn; i++ {
		TestInitLeaderChecker(t)
		arrChecker[i] = leaderChecker
		arrStorager[i] = store
	}
	wg := &sync.WaitGroup{}
	wg.Add(clientCn)
	for i := 0; i < clientCn; i++ {
		go func(i int) {
			defer wg.Done()
			arrChecker[i].check(ctx)
		}(i)
	}
	wg.Wait()

	for i := 0; i < clientCn; i++ {
		arrChecker[i].free(ctx)
		err = arrStorager[i].Close()
		if err != nil {
			t.Fatalf("close test err %s", err.Error())
		}
	}
}
