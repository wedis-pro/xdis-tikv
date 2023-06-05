package tikv

import (
	"fmt"
	"strings"

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
		return m.GetTxnKVClient()
	}

	return m.rawKV
}

func (m *Client) GetTxnKVClient() *TxnKVClientWrapper {
	return m.txnKV
}

func (m *Client) Close() error {
	errs := []error{}
	if m.txnKV != nil {
		errs = append(errs, m.txnKV.Close())
	}
	if m.rawKV != nil {
		errs = append(errs, m.rawKV.Close())
	}

	strErrs := []string{}
	for _, err := range errs {
		if err == nil {
			continue
		}
		strErrs = append(strErrs, err.Error())
	}
	if len(strErrs) > 0 {
		return fmt.Errorf("close err %s", strings.Join(strErrs, " | "))
	}

	return nil
}
