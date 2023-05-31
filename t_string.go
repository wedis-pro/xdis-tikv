package xdistikv

import (
	"github.com/weedge/pkg/driver"
	openkvdriver "github.com/weedge/pkg/driver/openkv"
)

type DBString struct {
}

func NewDBString() *DBString {
	return &DBString{}
}

func (db *DBString) Del(keys ...[]byte) (int64, error)
func (db *DBString) Exists(key []byte) (int64, error)
func (db *DBString) Expire(key []byte, duration int64) (int64, error)
func (db *DBString) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBString) TTL(key []byte) (int64, error)
func (db *DBString) Persist(key []byte) (int64, error)


func (db *DBString) Get(key []byte) ([]byte, error)
func (db *DBString) Set(key []byte, value []byte) error
func (db *DBString) GetSlice(key []byte) (openkvdriver.ISlice, error)
func (db *DBString) GetSet(key []byte, value []byte) ([]byte, error)
func (db *DBString) Incr(key []byte) (int64, error)
func (db *DBString) IncrBy(key []byte, increment int64) (int64, error)
func (db *DBString) Decr(key []byte) (int64, error)
func (db *DBString) DecrBy(key []byte, decrement int64) (int64, error)
func (db *DBString) MGet(keys ...[]byte) ([][]byte, error)
func (db *DBString) MSet(args ...driver.KVPair) error
func (db *DBString) SetNX(key []byte, value []byte) (n int64, err error)
func (db *DBString) SetEX(key []byte, duration int64, value []byte) error
func (db *DBString) SetRange(key []byte, offset int, value []byte) (int64, error)
func (db *DBString) GetRange(key []byte, start int, end int) ([]byte, error)
func (db *DBString) StrLen(key []byte) (int64, error)
func (db *DBString) Append(key []byte, value []byte) (int64, error)
