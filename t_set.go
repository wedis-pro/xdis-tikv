package xdistikv

type DBSet struct {
}

func NewDBSet() *DBSet {
	return &DBSet{}
}

func (db *DBSet) SAdd(key []byte, args ...[]byte) (int64, error)
func (db *DBSet) SCard(key []byte) (int64, error)
func (db *DBSet) SDiff(keys ...[]byte) ([][]byte, error)
func (db *DBSet) SDiffStore(dstKey []byte, keys ...[]byte) (int64, error)
func (db *DBSet) SInter(keys ...[]byte) ([][]byte, error)
func (db *DBSet) SInterStore(dstKey []byte, keys ...[]byte) (int64, error)
func (db *DBSet) SIsMember(key []byte, member []byte) (int64, error)
func (db *DBSet) SMembers(key []byte) ([][]byte, error)
func (db *DBSet) SRem(key []byte, args ...[]byte) (int64, error)
func (db *DBSet) SUnion(keys ...[]byte) ([][]byte, error)
func (db *DBSet) SUnionStore(dstKey []byte, keys ...[]byte) (int64, error)

func (db *DBSet) Del(keys ...[]byte) (int64, error)
func (db *DBSet) Exists(key []byte) (int64, error)
func (db *DBSet) Expire(key []byte, duration int64) (int64, error)
func (db *DBSet) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBSet) TTL(key []byte) (int64, error)
func (db *DBSet) Persist(key []byte) (int64, error)
