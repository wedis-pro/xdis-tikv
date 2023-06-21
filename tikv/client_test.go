package tikv

import (
	"testing"

	"github.com/weedge/xdis-tikv/config"
)

func TestNewClient(t *testing.T) {
	_, err := NewClient(config.DefaultTikvClientOptions())
	if err != nil {
		t.Fatalf("new tikv client err %s", err.Error())
	}
}
