package xdistikv

type DBBitmap struct {
}

func NewDBBitmap() *DBBitmap {
	return &DBBitmap{}
}

func (db *DBBitmap) BitOP(op string, destKey []byte, srcKeys ...[]byte) (int64, error)
func (db *DBBitmap) BitCount(key []byte, start int, end int) (int64, error)
func (db *DBBitmap) BitPos(key []byte, on int, start int, end int) (int64, error)
func (db *DBBitmap) SetBit(key []byte, offset int, on int) (int64, error)
func (db *DBBitmap) GetBit(key []byte, offset int) (int64, error)

func (db *DBBitmap) Del(keys ...[]byte) (int64, error)
func (db *DBBitmap) Exists(key []byte) (int64, error)
func (db *DBBitmap) Expire(key []byte, duration int64) (int64, error)
func (db *DBBitmap) ExpireAt(key []byte, when int64) (int64, error)
func (db *DBBitmap) TTL(key []byte) (int64, error)
func (db *DBBitmap) Persist(key []byte) (int64, error)
