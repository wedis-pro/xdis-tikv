package xdistikv

import (
	"reflect"
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestDBZSet_Del(t *testing.T) {
	TestNewDB(t)

	key := []byte("zaddkey")
	args := []driver.ScorePair{
		{Score: 1, Member: []byte("m1")},
		{Score: 2, Member: []byte("m2")},
		{Score: 3, Member: []byte("m3")},
	}
	_, err = db.DBZSet().Del(ctx, key)
	if err != nil {
		t.Fatalf("zset del fail err:%s", err.Error())
	}

	n, err := db.DBZSet().ZAdd(ctx, key, args...)
	if err != nil {
		t.Fatalf("zadd fail err:%s", err.Error())
	}
	if !reflect.DeepEqual(n, int64(len(args))) {
		t.Fatalf("zadd fail get %d expected %d", n, int64(len(args)))
	}
	n, err = db.DBZSet().Del(ctx, key)
	if err != nil {
		t.Fatalf("zset del fail err:%s", err.Error())
	}
	if !reflect.DeepEqual(n, int64(1)) {
		t.Fatalf("zset del fail get %d expected %d", n, int64(1))
	}
}
