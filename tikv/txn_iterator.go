package tikv

import (
	"bytes"

	"github.com/weedge/xdis-tikv/v1/driver"
)

type RangeIter struct {
	it        driver.IIterator
	offset    int
	limit     int
	minKey    []byte
	maxKey    []byte
	isReverse bool

	// for iter step++ instead of limit-- when limit < 0
	step int
}

func (m *RangeIter) Valid() bool {
	if m.offset < 0 {
		return false
	}
	if !m.it.Valid() {
		return false
	}
	if m.limit >= 0 && m.step >= m.limit {
		return false
	}
	if m.isReverse && bytes.Compare(m.minKey, m.it.Key()) > 0 {
		return false
	}
	if !m.isReverse && bytes.Compare(m.maxKey, m.it.Key()) < 0 {
		return false
	}

	return true
}

func (m *RangeIter) Key() []byte {
	return m.it.Key()
}

func (m *RangeIter) Value() []byte {
	return m.it.Value()
}

func (m *RangeIter) Next() error {
	m.step++
	return m.it.Next()
}

func (m *RangeIter) Close() {
	m.it.Close()
}

func (m *RangeIter) Offset() *RangeIter {
	for i := 0; i < m.offset; i++ {
		if m.Valid() {
			m.Next()
		}
	}

	return m
}
