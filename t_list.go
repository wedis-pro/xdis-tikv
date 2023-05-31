package xdistikv

import "time"

type DBList struct {
}

func NewDBList() *DBList {
	return &DBList{}
}

func (db *DBList) LIndex(key []byte, index int32) ([]byte, error)
func (db *DBList) LLen(key []byte) (int64, error)
func (db *DBList) LPop(key []byte) ([]byte, error)
func (db *DBList) LTrim(key []byte, start, stop int64) error
func (db *DBList) LTrimFront(key []byte, trimSize int32) (int32, error)
func (db *DBList) LTrimBack(key []byte, trimSize int32) (int32, error)
func (db *DBList) LPush(key []byte, args ...[]byte) (int64, error)
func (db *DBList) LSet(key []byte, index int32, value []byte) error
func (db *DBList) LRange(key []byte, start int32, stop int32) ([][]byte, error)
func (db *DBList) RPop(key []byte) ([]byte, error)
func (db *DBList) RPush(key []byte, args ...[]byte) (int64, error)
func (db *DBList) BLPop(keys [][]byte, timeout time.Duration) ([]interface{}, error)
func (db *DBList) BRPop(keys [][]byte, timeout time.Duration) ([]interface{}, error)

func (db *DBList) Del(keys ...[]byte) (int64, error)
func (db *DBList) Exists(key []byte) (int64, error)
func (db *DBList) Expire(key []byte, duration int64) (int64, error)
func (db *DBList) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBList) TTL(key []byte) (int64, error)
func (db *DBList) Persist(key []byte) (int64, error)
