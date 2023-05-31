package tikv

import (
	"github.com/tikv/client-go/v2/txnkv"
)

type TxnKVClientWrapper struct {
	client  *txnkv.Client
	retries int
}
