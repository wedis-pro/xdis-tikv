package xdistikv

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestDB_Implements(t *testing.T) {
	var i interface{} = &DB{}
	if _, ok := i.(driver.IDB); !ok {
		t.Fatalf("does not implement driver.IDB")
	}
}

var (
	db *DB
)

func TestNewDB(t *testing.T) {
	TestStorager_Open(t)
	db = NewDB(store, 0)
}
