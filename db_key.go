package xdistikv

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/weedge/pkg/utils"
)

func (db *DB) checkKey(buf []byte) (int, error) {
	prefixKeyLen := len(db.store.prefixKey)
	dbIndexLen := len(db.indexVarBuf)
	l := prefixKeyLen + dbIndexLen
	if len(buf) < l {
		return 0, fmt.Errorf("key is too small")
	}
	if !bytes.Equal(db.store.prefixKey, buf[0:prefixKeyLen]) {
		return 0, fmt.Errorf("invalid prefix key")
	}
	if !bytes.Equal(db.indexVarBuf, buf[prefixKeyLen:l]) {
		return 0, fmt.Errorf("invalid db index")
	}

	return l, nil
}

// --- prefix key ---

func encodePrefixKey(prefix string) []byte {
	if len(prefix) == 0 {
		return []byte{}
	}
	buf := make([]byte, 2+len(prefix))
	idx := 0
	binary.BigEndian.PutUint16(buf[idx:], uint16(len(prefix)))
	idx += 2
	copy(buf[idx:], utils.String2Bytes(prefix))

	return buf
}

// --- db index ---

func encodeIndex(index int) []byte {
	// the most size for varint is 10 bytes
	ebuf := make([]byte, 10)
	n := binary.PutUvarint(ebuf, uint64(index))

	buf := make([]byte, 2+n)
	binary.BigEndian.PutUint16(buf[0:], uint16(n))
	copy(buf[2:], ebuf[0:n])

	return buf
}

// --- bitmap ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) encodeBitmapKey(key []byte) []byte {
	ek := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(ek, db.store.prefixKey)
	n := copy(ek[pos:], db.indexVarBuf)
	pos += n
	ek[pos] = BitmapType
	pos++
	copy(ek[pos:], key)
	return ek
}

func (db *DB) decodeBitmapKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}
	if pos+1 > len(ek) || ek[pos] != BitmapType {
		return nil, ErrBitmapKey
	}

	pos++

	return ek[pos:], nil
}

// --- string ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) encodeStringKey(key []byte) []byte {
	ek := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(ek, db.store.prefixKey)
	n := copy(ek[pos:], db.indexVarBuf)
	pos += n
	ek[pos] = StringType
	pos++
	copy(ek[pos:], key)
	return ek
}

func (db *DB) decodeStringKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}
	if pos+1 > len(ek) || ek[pos] != StringType {
		return nil, ErrStringKey
	}

	pos++

	return ek[pos:], nil
}

// --- list ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) lEncodeMetaKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n
	buf[pos] = LMetaType
	pos++
	copy(buf[pos:], key)
	return buf
}

func (db *DB) lDecodeMetaKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != LMetaType {
		return nil, ErrLMetaKey
	}

	pos++
	return ek[pos:], nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | seq(4)
func (db *DB) lEncodeListKey(key []byte, seq int32) []byte {
	buf := make([]byte, len(key)+7+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n
	buf[pos] = ListType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	binary.BigEndian.PutUint32(buf[pos:], uint32(seq))

	return buf
}

func (db *DB) lDecodeListKey(ek []byte) (key []byte, seq int32, err error) {
	pos := 0
	pos, err = db.checkKey(ek)
	if err != nil {
		return
	}

	if pos+1 > len(ek) || ek[pos] != ListType {
		err = ErrListKey
		return
	}

	pos++

	if pos+2 > len(ek) {
		err = ErrListKey
		return
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if keyLen+pos+4 != len(ek) {
		err = ErrListKey
		return
	}

	key = ek[pos : pos+keyLen]
	seq = int32(binary.BigEndian.Uint32(ek[pos+keyLen:]))
	return
}

// --- hash ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) hEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n
	buf[pos] = HSizeType
	pos++
	copy(buf[pos:], key)

	return buf
}

func (db *DB) hDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != HSizeType {
		return nil, ErrHSizeKey
	}
	pos++

	return ek[pos:], nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | hashStartSep(1) | field
func (db *DB) hEncodeHashKey(key []byte, field []byte) []byte {
	buf := make([]byte, len(key)+len(field)+1+1+2+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = HashType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = hashStartSep
	pos++
	copy(buf[pos:], field)

	return buf
}

func (db *DB) hDecodeHashKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != HashType {
		return nil, nil, ErrHashKey
	}
	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrHashKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, ErrHashKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, ErrHashKey
	}

	pos++
	field := ek[pos:]
	return key, field, nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | hashStartSep(1)
func (db *DB) hEncodeStartKey(key []byte) []byte {
	return db.hEncodeHashKey(key, nil)
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | hashStopSep(1)
func (db *DB) hEncodeStopKey(key []byte) []byte {
	k := db.hEncodeHashKey(key, nil)

	k[len(k)-1] = hashStopSep

	return k
}

// --- set ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) sEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = SSizeType
	pos++
	copy(buf[pos:], key)
	return buf
}

func (db *DB) sDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != SSizeType {
		return nil, ErrSSizeKey
	}
	pos++

	return ek[pos:], nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | setStartSep(1) | member
func (db *DB) sEncodeSetKey(key []byte, member []byte) []byte {
	buf := make([]byte, len(key)+len(member)+1+1+2+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = SetType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = setStartSep
	pos++
	copy(buf[pos:], member)

	return buf
}

func (db *DB) sDecodeSetKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != SetType {
		return nil, nil, ErrSetKey
	}

	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, ErrSetKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, ErrSetKey
	}

	pos++
	member := ek[pos:]
	return key, member, nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | setStartSep(1)
func (db *DB) sEncodeStartKey(key []byte) []byte {
	return db.sEncodeSetKey(key, nil)
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | setStopSep(1)
func (db *DB) sEncodeStopKey(key []byte) []byte {
	k := db.sEncodeSetKey(key, nil)

	k[len(k)-1] = setStopSep

	return k
}

// --- zset ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | key
func (db *DB) zEncodeSizeKey(key []byte) []byte {
	buf := make([]byte, len(key)+1+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n
	buf[pos] = ZSizeType
	pos++
	copy(buf[pos:], key)
	return buf
}

func (db *DB) zDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != ZSizeType {
		return nil, ErrZSizeKey
	}
	pos++
	return ek[pos:], nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetStartMemSep(1) | member
func (db *DB) zEncodeSetKey(key []byte, member []byte) []byte {
	buf := make([]byte, len(key)+len(member)+4+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = ZSetType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)

	return buf
}

func (db *DB) zDecodeSetKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkKey(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != ZSetType {
		return nil, nil, ErrZSetKey
	}

	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrZSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	if keyLen+pos > len(ek) {
		return nil, nil, ErrZSetKey
	}

	pos += 2
	key := ek[pos : pos+keyLen]

	if ek[pos+keyLen] != zsetStartMemSep {
		return nil, nil, ErrZSetKey
	}
	pos++

	member := ek[pos+keyLen:]
	return key, member, nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetStartMemSep(1)
func (db *DB) zEncodeStartSetKey(key []byte) []byte {
	k := db.zEncodeSetKey(key, nil)
	return k
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetStopMemSep(1)
func (db *DB) zEncodeStopSetKey(key []byte) []byte {
	k := db.zEncodeSetKey(key, nil)
	k[len(k)-1] = zsetStopMemSep
	return k
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetNScoreSep/zsetPScoreSep(1) | score(8) | zsetStartMemSep(1) | member
func (db *DB) zEncodeScoreKey(key []byte, member []byte, score int64) []byte {
	buf := make([]byte, len(key)+len(member)+13+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = ZScoreType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	if score < 0 {
		buf[pos] = zsetNScoreSep
	} else {
		buf[pos] = zsetPScoreSep
	}

	pos++
	binary.BigEndian.PutUint64(buf[pos:], uint64(score))
	pos += 8

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)
	return buf
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetNScoreSep/zsetPScoreSep(1) | score(8) | zsetStartMemSep(1)
func (db *DB) zEncodeStartScoreKey(key []byte, score int64) []byte {
	return db.zEncodeScoreKey(key, nil, score)
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | dataType(1) | len(key)(2) | key | zsetNScoreSep/zsetPScoreSep(1) | score(8) | zsetStopMemSep(1)
func (db *DB) zEncodeStopScoreKey(key []byte, score int64) []byte {
	k := db.zEncodeScoreKey(key, nil, score)
	k[len(k)-1] = zsetStopMemSep
	return k
}

func (db *DB) zDecodeScoreKey(ek []byte) (key []byte, member []byte, score int64, err error) {
	pos := 0
	pos, err = db.checkKey(ek)
	if err != nil {
		return
	}

	if pos+1 > len(ek) || ek[pos] != ZScoreType {
		err = ErrZScoreKey
		return
	}
	pos++

	if pos+2 > len(ek) {
		err = ErrZScoreKey
		return
	}
	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		err = ErrZScoreKey
		return
	}

	key = ek[pos : pos+keyLen]
	pos += keyLen

	if pos+10 > len(ek) {
		err = ErrZScoreKey
		return
	}

	if (ek[pos] != zsetNScoreSep) && (ek[pos] != zsetPScoreSep) {
		err = ErrZScoreKey
		return
	}
	pos++

	score = int64(binary.BigEndian.Uint64(ek[pos:]))
	pos += 8

	if ek[pos] != zsetStartMemSep {
		err = ErrZScoreKey
		return
	}

	pos++

	member = ek[pos:]
	return
}

// --- scan key ---

func (db *DB) encodeScanMinKey(storeDataType byte, key []byte) ([]byte, error) {
	return db.encodeScanKey(storeDataType, key)
}

func (db *DB) encodeScanMaxKey(storeDataType byte, key []byte) ([]byte, error) {
	if len(key) > 0 {
		return db.encodeScanKey(storeDataType, key)
	}

	k, err := db.encodeScanKey(storeDataType, nil)
	if err != nil {
		return nil, err
	}
	k[len(k)-1] = storeDataType + 1
	return k, nil
}

func (db *DB) encodeScanKey(storeDataType byte, key []byte) ([]byte, error) {
	switch storeDataType {
	case StringType:
		return db.encodeStringKey(key), nil
	case LMetaType:
		return db.lEncodeMetaKey(key), nil
	case HSizeType:
		return db.hEncodeSizeKey(key), nil
	case ZSizeType:
		return db.zEncodeSizeKey(key), nil
	case SSizeType:
		return db.sEncodeSizeKey(key), nil
	case BitmapType:
		return db.encodeBitmapKey(key), nil
	default:
		return nil, ErrDataType
	}
}

func (db *DB) decodeScanKey(storeDataType byte, ek []byte) (key []byte, err error) {
	switch storeDataType {
	case StringType:
		key, err = db.decodeStringKey(ek)
	case LMetaType:
		key, err = db.lDecodeMetaKey(ek)
	case HSizeType:
		key, err = db.hDecodeSizeKey(ek)
	case ZSizeType:
		key, err = db.zDecodeSizeKey(ek)
	case SSizeType:
		key, err = db.sDecodeSizeKey(ek)
	case BitmapType:
		key, err = db.decodeBitmapKey(ek)
	default:
		err = ErrDataType
	}
	return
}

// --- expire ttl ---

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | ExpMetaType(1) | dataType(1) | key
func (db *DB) expEncodeMetaKey(dataType byte, key []byte) []byte {
	buf := make([]byte, len(key)+2+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = ExpMetaType
	pos++
	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

func (db *DB) expDecodeMetaKey(mk []byte) (byte, []byte, error) {
	pos, err := db.checkKey(mk)
	if err != nil {
		return 0, nil, err
	}

	if pos+2 > len(mk) || mk[pos] != ExpMetaType {
		return 0, nil, ErrExpMetaKey
	}

	return mk[pos+1], mk[pos+2:], nil
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex) | ExpTimeType(1) | when(8) | dataType(1) | key
func (db *DB) expEncodeTimeKey(dataType byte, key []byte, when int64) []byte {
	buf := make([]byte, len(key)+10+len(db.indexVarBuf)+len(db.store.prefixKey))
	pos := copy(buf, db.store.prefixKey)
	n := copy(buf[pos:], db.indexVarBuf)
	pos += n

	buf[pos] = ExpTimeType
	pos++

	binary.BigEndian.PutUint64(buf[pos:], uint64(when))
	pos += 8

	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

func (db *DB) expDecodeTimeKey(tk []byte) (byte, []byte, int64, error) {
	pos, err := db.checkKey(tk)
	if err != nil {
		return 0, nil, 0, err
	}

	if pos+10 > len(tk) || tk[pos] != ExpTimeType {
		return 0, nil, 0, ErrExpTimeKey
	}

	return tk[pos+9], tk[pos+10:], int64(binary.BigEndian.Uint64(tk[pos+1:])), nil
}

// --- job key ----
func jobEncodeLeaderKey() []byte {
	return PutUint16(LeaderPreKey)
}

func jobEncodeGCPointKey() []byte {
	return PutUint16(GCPreKey)
}

//--- ext binary number ---

// PutUint16 puts the u16 integer.
func PutUint16(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

// PutInt64 puts the 64 integer.
func PutInt64(v int64) []byte {
	return PutUInt64(uint64(v))
}

// PutUInt64 puts the u64 integer.
func PutUInt64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// Int64 gets 64 integer with the little endian format.
func Int64(v []byte, err error) (int64, error) {
	u, err := Uint64(v, err)
	if err != nil {
		return 0, err
	}

	return int64(u), nil
}

// Uint64 gets unsigned 64 integer with the little endian format.
func Uint64(v []byte, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, ErrIntNumber
	}

	return binary.LittleEndian.Uint64(v), nil
}
