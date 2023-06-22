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

func TestDBZSet_ZRangeByLex(t *testing.T) {
	TestNewDB(t)

	key := []byte("zrangebylexkey")
	args := []driver.ScorePair{
		{Score: 1, Member: []byte("mm1")},
		{Score: 2, Member: []byte("mm2")},
		{Score: 3, Member: []byte("mm3")},
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

	res, err := db.DBZSet().ZRangeByLex(ctx, key, nil, nil, driver.RangeClose, 0, -1)
	if err != nil {
		t.Fatalf("ZRangeByLex err:%s", err.Error())
	}
	expected := [][]byte{[]byte("mm1"), []byte("mm2"), []byte("mm3")}
	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("zadd fail get %+v expected %+v", res, expected)
	}

	res, err = db.DBZSet().ZRangeByLex(ctx, key, nil, nil, driver.RangeClose, 1, 2)
	if err != nil {
		t.Fatalf("ZRangeByLex err:%s", err.Error())
	}
	expected = [][]byte{[]byte("mm2"), []byte("mm3")}
	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("zadd fail get %+v expected %+v", res, expected)
	}
}
