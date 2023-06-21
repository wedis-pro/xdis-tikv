package xdistikv

import (
	"context"
	"testing"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/xdis-tikv/config"
	"github.com/weedge/xdis-tikv/tikv"
)

func TestStorager_Implements(t *testing.T) {
	var i interface{} = &Storager{}
	if _, ok := i.(driver.IStorager); !ok {
		t.Fatalf("does not implement driver.IStorage")
	}
}

var (
	ctx      context.Context
	kvClient *tikv.Client
	err      error
	store    *Storager
)

func TestInitStorager(t *testing.T) {
	ctx = context.TODO()
	store = New(config.DefaultStoragerOptions())
}

func TestClose(t *testing.T) {
	TestInitStorager(t)
	err = store.Close()
	if err != nil {
		t.Fatalf("close test err %s", err.Error())
	}
}

func TestInitTikvClient(t *testing.T) {
	kvClient, err = tikv.NewClient(config.DefaultTikvClientOptions())
	if err != nil {
		t.Fatalf("new tikv client err %s", err.Error())
	}
}

func TestStorager_Open(t *testing.T) {
	TestInitStorager(t)
	err = store.Open(ctx)
	if err != nil {
		t.Fatalf("open err %s", err.Error())
	}
}
