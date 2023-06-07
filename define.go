package xdistikv

import (
	"errors"
	"math"
)

// For different const size configuration
const (
	// max allowed databases
	MaxDatabases int = 10240

	// max key size
	MaxKeySize int = 1024

	// max hash field size
	MaxHashFieldSize int = 1024

	// max zset member size
	MaxZSetMemberSize int = 1024

	// max set member size
	MaxSetMemberSize int = 1024

	// max value size
	MaxValueSize int = 1024 * 1024 * 1024

	// default scan count
	DefaultScanCount int = 10
)

// DataType is defined for the different types
type DataType byte

// for out use
const (
	STRING DataType = iota
	LIST
	HASH
	SET
	ZSET
	BITMAP
)

// For different type name
const (
	StringName = "STRING"
	ListName   = "LIST"
	HashName   = "HASH"
	SetName    = "SET"
	ZSetName   = "ZSET"
	BitmapName = "BITMAP"
)

func (d DataType) String() string {
	switch d {
	case STRING:
		return StringName
	case LIST:
		return ListName
	case HASH:
		return HashName
	case SET:
		return SetName
	case ZSET:
		return ZSetName
	case BITMAP:
		return BitmapName
	default:
		return "unknown"
	}
}

// for backend store key type
// notice: Please add new type in order
const (
	NoneType byte = iota
	StringType

	HashType
	HSizeType

	ListType
	LMetaType

	SetType
	SSizeType

	ZSetType
	ZSizeType
	ZScoreType

	ExpTimeType
	ExpMetaType

	BitmapType

	maxDataType byte = 100
)

// TypeName is the map of type -> name
var TypeName = map[byte]string{
	StringType:  "string",
	HashType:    "hash",
	HSizeType:   "hsize",
	ListType:    "list",
	LMetaType:   "lmeta",
	SetType:     "set",
	SSizeType:   "ssize",
	ZSetType:    "zset",
	ZSizeType:   "zsize",
	ZScoreType:  "zscore",
	ExpTimeType: "exptime",
	ExpMetaType: "expmeta",
	BitmapType:  "bitmap",
}

var (
	ErrCmdNotSupport = errors.New("ERR cmd not supported")

	ErrIntNumber   = errors.New("invalid integer")
	ErrKeySize     = errors.New("invalid key size")
	ErrValueSize   = errors.New("invalid value size")
	ErrExpireValue = errors.New("invalid expire value")

	ErrStringKey = errors.New("invalid encode string key")

	ErrLMetaKey  = errors.New("invalid lmeta key")
	ErrListKey   = errors.New("invalid list key")
	ErrListSeq   = errors.New("invalid list sequence, overflow")
	ErrListIndex = errors.New("invalid list index")

	ErrHashKey       = errors.New("invalid hash key")
	ErrHSizeKey      = errors.New("invalid hsize key")
	ErrHashFieldSize = errors.New("invalid hash field size")

	ErrSetKey        = errors.New("invalid set key")
	ErrSSizeKey      = errors.New("invalid ssize key")
	ErrSetMemberSize = errors.New("invalid set member size")

	ErrZSizeKey         = errors.New("invalid zsize key")
	ErrZSetKey          = errors.New("invalid zset key")
	ErrZScoreKey        = errors.New("invalid zscore key")
	ErrScoreOverflow    = errors.New("zset score overflow")
	ErrInvalidAggregate = errors.New("invalid aggregate")
	ErrInvalidWeightNum = errors.New("invalid weight number")
	ErrInvalidSrcKeyNum = errors.New("invalid src key number")
	ErrZSetMemberSize   = errors.New("invalid zset member size")

	ErrExpMetaKey = errors.New("invalid expire meta key")
	ErrExpTimeKey = errors.New("invalid expire time key")

	ErrDataType = errors.New("error data type")
	ErrMetaKey  = errors.New("error meta key")

	ErrBitmapKey = errors.New("invalid encode bitmap key")

	// For different common errors
	ErrScoreMiss = errors.New("zset score miss")

	// for job err
	ErrLeaderValSize = errors.New("invalid leader checker job val size")
)

// For list op
const (
	listHeadSeq int32 = 1
	listTailSeq int32 = 2

	listMinSeq     int32 = 1000
	listMaxSeq     int32 = 1<<31 - 1000
	listInitialSeq int32 = listMinSeq + (listMaxSeq-listMinSeq)/2
)

// For hash op
const (
	hashStartSep byte = ':'
	hashStopSep  byte = hashStartSep + 1
)

// For set op
const (
	setStartSep byte = ':'
	setStopSep  byte = setStartSep + 1

	UnionType byte = 51
	DiffType  byte = 52
	InterType byte = 53
)

// For zset op
const (
	MinScore     int64 = math.MinInt64 + 1
	MaxScore     int64 = math.MaxInt64
	InvalidScore int64 = math.MinInt64

	AggregateSum = "sum"
	AggregateMin = "min"
	AggregateMax = "max"

	zsetNScoreSep    byte = '<'
	zsetPScoreSep    byte = zsetNScoreSep + 1
	zsetStopScoreSep byte = zsetPScoreSep + 1

	zsetStartMemSep byte = ':'
	zsetStopMemSep  byte = zsetStartMemSep + 1
)

// For bit operation
const (
	BitAND = "and"
	BitOR  = "or"
	BitXOR = "xor"
	BitNot = "not"
)

// For checker job operation key
const (
	LeaderPreKey = 255
	GCPreKey     = 256
)
