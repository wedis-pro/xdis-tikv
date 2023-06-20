package xdistikv

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/xdis-tikv/tikv"
)

type DBBitmap struct {
	*DB
}

func NewDBBitmap(db *DB) *DBBitmap {
	return &DBBitmap{DB: db}
}

func (db *DBBitmap) BitOP(ctx context.Context, op string, destKey []byte, srcKeys ...[]byte) (int64, error) {
	if err := checkKeySize(destKey); err != nil {
		return 0, err
	}

	op = strings.ToLower(op)
	if len(srcKeys) == 0 {
		return 0, nil
	} else if op == BitNot && len(srcKeys) > 1 {
		return 0, fmt.Errorf("BITOP NOT has only one srckey")
	} else if len(srcKeys) < 2 {
		return 0, nil
	}

	key := db.encodeStringKey(srcKeys[0])
	value, err := db.kvClient.GetKVClient().Get(ctx, key)
	if err != nil {
		return 0, err
	}

	if op == BitNot {
		for i := 0; i < len(value); i++ {
			value[i] = ^value[i]
		}
	} else {
		for j := 1; j < len(srcKeys); j++ {
			if err := checkKeySize(srcKeys[j]); err != nil {
				return 0, err
			}

			key = db.encodeStringKey(srcKeys[j])
			ovalue, err := db.kvClient.GetKVClient().Get(ctx, key)
			if err != nil {
				return 0, err
			}

			if len(value) < len(ovalue) {
				value, ovalue = ovalue, value
			}

			for i := 0; i < len(ovalue); i++ {
				switch op {
				case BitAND:
					value[i] &= ovalue[i]
				case BitOR:
					value[i] |= ovalue[i]
				case BitXOR:
					value[i] ^= ovalue[i]
				default:
					return 0, fmt.Errorf("invalid op type: %s", op)
				}
			}

			for i := len(ovalue); i < len(value); i++ {
				switch op {
				case BitAND:
					value[i] &= 0
				case BitOR:
					value[i] |= 0
				case BitXOR:
					value[i] ^= 0
				}
			}
		} // end for
	} // end if

	key = db.encodeStringKey(destKey)
	err = db.kvClient.GetKVClient().Put(ctx, key, value)
	if err != nil {
		return 0, err
	}

	return int64(len(value)), nil
}

var bitsInByte = [256]int32{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3,
	4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3,
	3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
	5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
	3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
	5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2,
	2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3,
	4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4,
	5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
	6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
	6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8}

func numberBitCount(i uint32) uint32 {
	i = i - ((i >> 1) & 0x55555555)
	i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
	return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
}

func (db *DBBitmap) BitCount(ctx context.Context, key []byte, start int, end int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeStringKey(key)
	value, err := db.kvClient.GetKVClient().Get(ctx, key)
	if err != nil {
		return 0, err
	}

	start, end = getRange(start, end, len(value))
	value = value[start : end+1]

	var n int64
	pos := 0
	for ; pos+4 <= len(value); pos = pos + 4 {
		n += int64(numberBitCount(binary.BigEndian.Uint32(value[pos : pos+4])))
	}

	for ; pos < len(value); pos++ {
		n += int64(bitsInByte[value[pos]])
	}

	return n, nil
}

func (db *DBBitmap) BitPos(ctx context.Context, key []byte, on int, start int, end int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	var skipValue uint8
	if on == 0 {
		skipValue = 0xFF
	}

	key = db.encodeStringKey(key)
	value, err := db.kvClient.GetKVClient().Get(ctx, key)
	if err != nil {
		return 0, err
	}

	start, end = getRange(start, end, len(value))
	value = value[start : end+1]

	for i, v := range value {
		if uint8(v) != skipValue {
			for j := 0; j < 8; j++ {
				isNull := uint8(v)&(1<<uint8(7-j)) == 0

				if (on == 1 && !isNull) || (on == 0 && isNull) {
					return int64((start+i)*8 + j), nil
				}
			}
		}
	}

	return -1, nil
}

func (db *DBBitmap) SetBit(ctx context.Context, key []byte, offset int, on int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	key = db.encodeStringKey(key)
	res, err := db.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		value, err := txn.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		byteOffset := int(uint32(offset) >> 3)
		extra := byteOffset + 1 - len(value)
		if extra > 0 {
			value = append(value, make([]byte, extra)...)
		}

		byteVal := value[byteOffset]
		bit := 7 - uint8(uint32(offset)&0x7)
		bitVal := byteVal & (1 << bit)
		byteVal &= ^(1 << bit)
		byteVal |= (uint8(on&0x1) << bit)
		value[byteOffset] = byteVal
		err = txn.Set(key, value)
		if err != nil {
			return 0, err
		}

		if bitVal > 0 {
			return 1, nil
		}
		return 0, nil
	}, tikv.WithAsyncCommit(true), tikv.WithTryOnePcCommit(true))
	if err != nil {
		return 0, err
	}

	return int64(res.(int)), nil
}

func (db *DBBitmap) GetBit(ctx context.Context, key []byte, offset int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeStringKey(key)
	value, err := db.kvClient.GetKVClient().Get(ctx, key)
	if err != nil {
		return 0, err
	}

	byteOffset := uint32(offset) >> 3
	bit := 7 - uint8(uint32(offset)&0x7)

	if byteOffset >= uint32(len(value)) {
		return 0, nil
	}

	bitVal := value[byteOffset] & (1 << bit)
	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}
