package xdistikv

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestDBBitmap_Implements(t *testing.T) {
	var i interface{} = &DBBitmap{}
	if _, ok := i.(driver.IBitmapCmd); !ok {
		t.Fatalf("does not implement driver.IBitmapCmd")
	}
}
func TestDBString_Implements(t *testing.T) {
	var i interface{} = &DBString{}
	if _, ok := i.(driver.IStringCmd); !ok {
		t.Fatalf("does not implement driver.IStringCmd")
	}
}

func TestDBHash_Implements(t *testing.T) {
	var i interface{} = &DBHash{}
	if _, ok := i.(driver.IHashCmd); !ok {
		t.Fatalf("does not implement driver.IHashCmd")
	}
}

func TestDBList_Implements(t *testing.T) {
	var i interface{} = &DBList{}
	if _, ok := i.(driver.IListCmd); !ok {
		t.Fatalf("does not implement driver.IListCmd")
	}
}
func TestDBSet_Implements(t *testing.T) {
	var i interface{} = &DBSet{}
	if _, ok := i.(driver.ISetCmd); !ok {
		t.Fatalf("does not implement driver.ISetCmd")
	}
}
func TestDBZSet_Implements(t *testing.T) {
	var i interface{} = &DBZSet{}
	if _, ok := i.(driver.IZsetCmd); !ok {
		t.Fatalf("does not implement driver.IZsetCmd")
	}
}
