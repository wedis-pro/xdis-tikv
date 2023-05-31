package xdistikv

import "github.com/weedge/pkg/driver"

type DBHash struct {
}

func NewDBHash() *DBHash {
	return &DBHash{}
}

func (db *DBHash) HSet(key []byte, field []byte, value []byte) (int64, error)
func (db *DBHash) HGet(key []byte, field []byte) ([]byte, error)
func (db *DBHash) HLen(key []byte) (int64, error)
func (db *DBHash) HMset(key []byte, args ...driver.FVPair) error
func (db *DBHash) HMget(key []byte, args ...[]byte) ([][]byte, error)
func (db *DBHash) HDel(key []byte, args ...[]byte) (int64, error)
func (db *DBHash) HIncrBy(key []byte, field []byte, delta int64) (int64, error)
func (db *DBHash) HGetAll(key []byte) ([]driver.FVPair, error)
func (db *DBHash) HKeys(key []byte) ([][]byte, error)
func (db *DBHash) HValues(key []byte) ([][]byte, error)

func (db *DBHash) Del(keys ...[]byte) (int64, error)
func (db *DBHash) Exists(key []byte) (int64, error)
func (db *DBHash) Expire(key []byte, duration int64) (int64, error)
func (db *DBHash) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBHash) TTL(key []byte) (int64, error)
func (db *DBHash) Persist(key []byte) (int64, error)
