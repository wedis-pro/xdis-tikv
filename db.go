package xdistikv

import (
	"encoding/binary"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	kvDriver "github.com/weedge/xdis-tikv/driver"
)

// DB core sturct
// impl like redis string, list, hash, set, zset, bitmap struct store db op
type DB struct {
	store *Storager
	// database index
	index int
	// database index to varint buffer
	indexVarBuf []byte
	// IKVStoreDB impl
	kvDriver.IKV

	string *DBString
	list   *DBList
	hash   *DBHash
	set    *DBSet
	zset   *DBZSet
	bitmap *DBBitmap

	ttlChecker *TTLChecker
}

func NewDB(store *Storager, idx int) *DB {
	db := &DB{store: store}
	db.SetIndex(idx)
	db.IKV = store.kvClient

	db.string = NewDBString(db)
	db.list = NewDBList(db)
	db.hash = NewDBHash(db)
	db.set = NewDBSet(db)
	db.zset = NewDBZSet(db)
	db.bitmap = NewDBBitmap(db)

	db.ttlChecker = NewTTLChecker(db)

	return db
}

func (m *DB) DBString() driver.IStringCmd {
	return m.string
}
func (m *DB) DBList() driver.IListCmd {
	return m.list
}
func (m *DB) DBHash() driver.IHashCmd {
	return m.hash
}
func (m *DB) DBSet() driver.ISetCmd {
	return m.set
}
func (m *DB) DBZSet() driver.IZsetCmd {
	return m.zset
}
func (m *DB) DBBitmap() driver.IBitmapCmd {
	return m.bitmap
}

func (m *DB) Close() (err error) {
	if utils.IsNil(m.IKV) {
		return
	}

	return m.IKV.Close()
}

// Index gets the index of database.
func (db *DB) Index() int {
	return db.index
}

// IndexVarBuf gets the index varint buf of database.
func (db *DB) IndexVarBuf() []byte {
	return db.indexVarBuf
}

// SetIndex set the index of database.
func (db *DB) SetIndex(index int) {
	db.index = index
	// the most size for varint is 10 bytes
	buf := make([]byte, 10)
	n := binary.PutUvarint(buf, uint64(index))

	db.indexVarBuf = buf[0:n]
}
