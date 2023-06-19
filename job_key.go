package xdistikv

import "encoding/binary"

// --- job key ----
// (len(prefixKey)(2) | prefixKey) | LeaderPrekey
func (store *Storager) jobEncodeLeaderKey() []byte {
	buf := make([]byte, 2+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)

	binary.BigEndian.PutUint16(buf[pos:], LeaderPreKey)
	return buf
}

// (len(prefixKey)(2) | prefixKey) | GCPrekey
func (store *Storager) jobEncodeGCPointKey() []byte {
	buf := make([]byte, 2+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)

	binary.BigEndian.PutUint16(buf[pos:], GCPreKey)
	return buf
}
