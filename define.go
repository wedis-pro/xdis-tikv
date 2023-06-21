package xdistikv

import (
	"errors"
	"math"
)

// For different const size configuration
const (
	// max allowed databases
	MaxDatabases int = 128

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

	ErrIntNumber   = errors.New("ERR invalid integer")
	ErrKeySize     = errors.New("ERR invalid key size")
	ErrValueSize   = errors.New("ERR invalid value size")
	ErrExpireValue = errors.New("ERR invalid expire value")

	ErrStringKey = errors.New("ERR invalid encode string key")

	ErrLMetaKey  = errors.New("ERR invalid lmeta key")
	ErrListKey   = errors.New("ERR invalid list key")
	ErrListSeq   = errors.New("ERR invalid list sequence, overflow")
	ErrListIndex = errors.New("ERR invalid list index")

	ErrHashKey       = errors.New("ERR invalid hash key")
	ErrHashIntVal    = errors.New("ERR hash value is not an integer")
	ErrHSizeKey      = errors.New("ERR invalid hsize key")
	ErrHashFieldSize = errors.New("ERR invalid hash field size")

	ErrSetKey        = errors.New("ERR invalid set key")
	ErrSSizeKey      = errors.New("ERR invalid ssize key")
	ErrSetMemberSize = errors.New("ERR invalid set member size")

	ErrZSizeKey         = errors.New("ERR invalid zsize key")
	ErrZSetKey          = errors.New("ERR invalid zset key")
	ErrZScoreKey        = errors.New("ERR invalid zscore key")
	ErrScoreOverflow    = errors.New("ERR zset score overflow")
	ErrInvalidAggregate = errors.New("ERR invalid aggregate")
	ErrInvalidWeightNum = errors.New("ERR invalid weight number")
	ErrInvalidSrcKeyNum = errors.New("ERR invalid src key number")
	ErrZSetMemberSize   = errors.New("ERR invalid zset member size")

	ErrExpMetaKey = errors.New("ERR invalid expire meta key")
	ErrExpTimeKey = errors.New("ERR invalid expire time key")

	ErrDataType = errors.New("ERR error data type")
	ErrMetaKey  = errors.New("ERR error meta key")

	ErrBitmapKey = errors.New("ERR invalid encode bitmap key")

	// For different common errors
	ErrScoreMiss = errors.New("ERR zset score miss")

	// for job err
	ErrLeaderValSize = errors.New("ERR invalid leader checker job val size")

	ErrTTLNoExp = errors.New("ERR TTL no exp to del")
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

// for scan
const (
	ScanOnceNums = 1024
	//ScanOnceNums = 1
)

// For checker job operation key
const (
	LeaderPreKey = 255
	GCPreKey     = 256
)

const (
	RegisterStoragerName = "xdis-tikv"
)
