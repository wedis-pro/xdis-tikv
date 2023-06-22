package tikv

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/xdis-tikv/config"
)

var (
	ctx      context.Context
	kvClient *TxnKVClientWrapper
	err      error
)

func TestInitTxnClient(t *testing.T) {
	kvClient, err = NewTxKVClient(config.DefaultTikvClientOptions())
	if err != nil {
		t.Fatalf("new tikv client err %s", err.Error())
	}
	ctx = context.Background()
}

func TestTxnKVClientWrapper_Iter(t *testing.T) {
	TestInitTxnClient(t)

	strTime := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	cn := 3
	key := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x00\x00\x00\x00\x00\x00\x00\x02:m3")
	val := []byte{000}
	for i := 0; i < cn; i++ {
		//k := append(key, []byte(strconv.Itoa(i))...)
		k := append(key, byte(i))
		err = kvClient.Put(ctx, k, val)
		if err != nil {
			t.Fatalf("put err %s", err.Error())
		}
		v, err := kvClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("get err %s", err.Error())
		}
		if !reflect.DeepEqual(val, v) {
			t.Fatalf("get %s expected %s", v, val)
		}
	}

	minKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear<\x80\x00\x00\x00\x00\x00\x00\x01:")
	maxKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x7f\xff\xff\xff\xff\xff\xff\xff;")
	iter, err := kvClient.Iter(ctx, nil, minKey, maxKey, 0, -1)
	if err != nil {
		t.Fatalf("iter err %s", err.Error())
	}
	it, ok := iter.(*RangeIter)
	if !ok {
		t.Fatalf("iter not RangeIter")
	}
	i := 0
	for ; iter.Valid(); iter.Next() {
		ik := iter.Key()
		//k := append(key, []byte(strconv.Itoa(i))...)
		k := append(key, byte(i))
		if !reflect.DeepEqual(ik, k) {
			t.Fatalf("get %s expected %s", ik, k)
		}
		i++
		err = it.GetTxn().Delete(ik)
		if err != nil {
			t.Fatalf("delete key %s err %s", ik, err.Error())
		}
	}
	if err = it.GetTxn().Commit(ctx); err != nil {
		t.Fatalf("commit err %s", err.Error())
	}
	iter.Close()

	if !reflect.DeepEqual(cn, i) {
		t.Fatalf("get %d expected %d", i, cn)
	}
}

func TestTxnKVClientWrapper_ExecutTxn_Iter(t *testing.T) {
	TestInitTxnClient(t)

	strTime := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	cn := 3
	key := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x00\x00\x00\x00\x00\x00\x00\x02:m3")
	val := []byte{000}
	for i := 0; i < cn; i++ {
		//k := append(key, []byte(strconv.Itoa(i))...)
		k := append(key, byte(i))
		err = kvClient.Put(ctx, k, val)
		if err != nil {
			t.Fatalf("put err %s", err.Error())
		}
		v, err := kvClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("get err %s", err.Error())
		}
		if !reflect.DeepEqual(val, v) {
			t.Fatalf("get %s expected %s", v, val)
		}
	}

	_, err = kvClient.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		minKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear<\x80\x00\x00\x00\x00\x00\x00\x01:")
		maxKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x7f\xff\xff\xff\xff\xff\xff\xff;")
		iter, err := kvClient.Iter(ctx, txn, minKey, maxKey, 0, -1)
		if err != nil {
			t.Fatalf("iter err %s", err.Error())
		}
		_, ok := iter.(*RangeIter)
		if !ok {
			t.Fatalf("iter not RangeIter")
		}

		i := 0
		for ; iter.Valid(); iter.Next() {
			ik := iter.Key()
			//k := append(key, []byte(strconv.Itoa(i))...)
			k := append(key, byte(i))
			if !reflect.DeepEqual(ik, k) {
				t.Fatalf("get %s expected %s", ik, k)
			}
			i++
			err = txn.Delete(ik)
			if err != nil {
				t.Fatalf("delete key %s err %s", ik, err.Error())
			}
		}
		iter.Close()

		if !reflect.DeepEqual(cn, i) {
			t.Fatalf("get %d expected %d", i, cn)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("err %s", err.Error())
	}
}

func TestTxnKVClientWrapper_ExecutTxn_Put_Iter(t *testing.T) {
	TestInitTxnClient(t)

	//strTime := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	strTime := ""
	cn := 3
	key := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x00\x00\x00\x00\x00\x00\x00\x02:m3")
	val := []byte{000}
	_, err = kvClient.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		for i := 0; i < cn; i++ {
			//k := append(key, []byte(strconv.Itoa(i))...)
			k := append(key, byte(i))
			err = txn.Set(k, val)
			if err != nil {
				t.Fatalf("txn set err %s", err.Error())
			}
			v, err := txn.Get(ctx, k)
			if err != nil {
				t.Fatalf("txn get err %s", err.Error())
			}
			if !reflect.DeepEqual(val, v) {
				t.Fatalf("get %s expected %s", v, val)
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("err %s", err.Error())
	}

	_, err = kvClient.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		minKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear<\x80\x00\x00\x00\x00\x00\x00\x01:")
		maxKey := []byte(strTime + "\x00\x06hitikv\x00\x01\x00\n\x00\azmclear=\x7f\xff\xff\xff\xff\xff\xff\xff;")
		iter, err := kvClient.Iter(ctx, txn, minKey, maxKey, 0, -1)
		if err != nil {
			t.Fatalf("iter err %s", err.Error())
		}
		_, ok := iter.(*RangeIter)
		if !ok {
			t.Fatalf("iter not RangeIter")
		}

		i := 0
		for ; iter.Valid(); iter.Next() {
			ik := iter.Key()
			//k := append(key, []byte(strconv.Itoa(i))...)
			k := append(key, byte(i))
			if !reflect.DeepEqual(ik, k) {
				t.Fatalf("get %s expected %s", ik, k)
			}
			i++
			err = txn.Delete(ik)
			if err != nil {
				t.Fatalf("delete key %s err %s", ik, err.Error())
			}
		}
		iter.Close()

		if !reflect.DeepEqual(cn, i) {
			t.Fatalf("get %d expected %d", i, cn)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("err %s", err.Error())
	}
}

func TestTxnKVClientWrapper_ExecutTxn_Iter_2(t *testing.T) {
	TestInitTxnClient(t)

	cn := 3
	key := []byte("\x00\x01\x00\n\x00\azaddkey=\x00\x00\x00\x00\x00\x00\x00\x01:m1")
	// notice: don't set empty val, tikv don't scan empty val key
	val := EmptySetVal
	for i := 0; i < cn; i++ {
		//k := append(key, []byte(strconv.Itoa(i))...)
		k := append(key, byte(i))
		err = kvClient.Put(ctx, k, val)
		if err != nil {
			t.Fatalf("put err %s", err.Error())
		}
		v, err := kvClient.Get(ctx, k)
		if err != nil {
			t.Fatalf("get err %s", err.Error())
		}
		if !reflect.DeepEqual(val, v) {
			t.Fatalf("get %s expected %s", v, val)
		}
	}

	_, err = kvClient.ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		minKey := []byte("\x00\x01\x00\n\x00\azaddkey<\x80\x00\x00\x00\x00\x00\x00\x01:")
		maxKey := []byte("\x00\x01\x00\n\x00\azaddkey=\x7f\xff\xff\xff\xff\xff\xff\xff;")
		iter, err := kvClient.Iter(ctx, txn, minKey, maxKey, 0, -1)
		if err != nil {
			t.Fatalf("iter err %s", err.Error())
		}
		_, ok := iter.(*RangeIter)
		if !ok {
			t.Fatalf("iter not RangeIter")
		}

		i := 0
		for ; iter.Valid(); iter.Next() {
			ik := iter.Key()
			//k := append(key, []byte(strconv.Itoa(i))...)
			k := append(key, byte(i))
			if !reflect.DeepEqual(ik, k) {
				t.Fatalf("get %s expected %s", ik, k)
			}
			i++
			err = txn.Delete(ik)
			if err != nil {
				t.Fatalf("delete key %s err %s", ik, err.Error())
			}
		}
		iter.Close()

		if !reflect.DeepEqual(cn, i) {
			t.Fatalf("get %d expected %d", i, cn)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatalf("err %s", err.Error())
	}
}
