package xdistikv

import (
	"github.com/weedge/pkg/driver"
	"github.com/weedge/xdis-tikv/tikv"
)

// DB core sturct
// impl like redis string, list, hash, set, zset, bitmap struct store db op
type DB struct {
	store *Storager
	// database index
	index int
	// database index to varint buffer
	indexVarBuf []byte
	// kv client
	kvClient *tikv.Client

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
	db.kvClient = store.kvClient

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
	if m.kvClient == nil {
		return
	}

	return m.kvClient.Close()
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
	db.indexVarBuf = encodeIndex(index)
}
