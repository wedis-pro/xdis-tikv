package driver

type IIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}
