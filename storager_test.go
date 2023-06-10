package xdistikv 

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestStorager_Implements(t *testing.T) {
	var i interface{} = &Storager{}
	if _, ok := i.(driver.IStorager); !ok {
		t.Fatalf("does not implement driver.IStorage")
	}
}
