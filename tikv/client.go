package tikv

import (
	"github.com/weedge/xdis-tikv/v1/config"
	"github.com/weedge/xdis-tikv/v1/driver"
)

type Client struct {
	opts  *config.TikvClientOptions
	rawKV *RawKVClientWrapper
	txnKV *TxnKVClientWrapper
}

func NewClient(opts *config.TikvClientOptions) (client *Client, err error) {
	rawKv, err := NewRawKVClient(opts)
	if err != nil {
		return
	}

	txnKv, err := NewTxKVClient(opts)
	if err != nil {
		return
	}

	client = &Client{opts: opts, rawKV: rawKv, txnKV: txnKv}
	return
}

// @todo v2
// use rawkv/txnkv connect pool (rr balance)
func (m *Client) GetKVClient() driver.IKV {
	if m.opts.UseTxnApi {
		return m.txnKV
	}

	return m.rawKV
}

func (m *Client) Close() error {
	return nil
}
