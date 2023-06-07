package xdistikv

import (
	"sync"
	"time"
)

type TTLChecker struct {
	db *DB

	//next check time
	nc int64
	sync.RWMutex
}

func NewTTLChecker(db *DB) *TTLChecker {
	c := &TTLChecker{
		db: db,
		nc: 0,
	}

	// todo: register data type with on expired event

	return c
}

func (c *TTLChecker) register(dataType byte) {
}

func (c *TTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *TTLChecker) check() {
	now := time.Now().Unix()

	c.Lock()
	nc := c.nc
	c.Unlock()

	if now < nc {
		return
	}

	nc = now + 3600

	c.clearExpired()

	c.setNextCheckTime(nc, true)
}

func (c *TTLChecker) clearExpired() {

}
