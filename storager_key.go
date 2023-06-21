package xdistikv

import "encoding/binary"

// --- flush ----

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex 0)
func (store *Storager) encodeFlushStartKey() []byte {
	startIndexBuf := encodeIndex(0)
	buf := make([]byte, len(startIndexBuf)+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)
	copy(buf[pos:], startIndexBuf)

	return buf
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex databases+1)
func (store *Storager) encodeFlushEndKey() []byte {
	endIndexBuf := encodeIndex(store.opts.Databases + 1)
	buf := make([]byte, len(endIndexBuf)+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)
	copy(buf[pos:], endIndexBuf)

	return buf
}

// --- job key ----

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex databases+1) | LeaderPrekey
func (store *Storager) jobEncodeLeaderKey() []byte {
	endIndexBuf := encodeIndex(store.opts.Databases + 1)
	buf := make([]byte, len(endIndexBuf)+2+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)
	binary.BigEndian.PutUint16(buf[pos:], LeaderPreKey)
	return buf
}

// (len(prefixKey)(2) | prefixKey) | (len(dbIndex)(2) | dbIndex databases+1) | GCPrekey
func (store *Storager) jobEncodeGCPointKey() []byte {
	endIndexBuf := encodeIndex(store.opts.Databases + 1)
	buf := make([]byte, len(endIndexBuf)+2+len(store.prefixKey))
	pos := copy(buf, store.prefixKey)
	binary.BigEndian.PutUint16(buf[pos:], GCPreKey)
	return buf
}
