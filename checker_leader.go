package xdistikv

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/weedge/xdis-tikv/v1/config"
	"github.com/weedge/xdis-tikv/v1/tikv"
)

type LeaderChecker struct {
	opts *config.LeaderJobOptions

	kvClient *tikv.Client
	uuid     uuid.UUID
}

var leaderCheckerOnce sync.Once
var leaderCheckerInstance *LeaderChecker

func NewLeaderChecker(opts *config.LeaderJobOptions, client *tikv.Client) *LeaderChecker {
	if leaderCheckerInstance == nil {
		leaderCheckerOnce.Do(func() {
			leaderCheckerInstance = &LeaderChecker{
				opts:     opts,
				kvClient: client,
				uuid:     uuid.New(),
			}
		})
	}
	return leaderCheckerInstance
}

func (m *LeaderChecker) Run(ctx context.Context) {
	klog.CtxInfof(ctx, "start leader checker with interval %d seconds, lease %s seconds", m.opts.LeaderCheckInterval, m.opts.LeaderLeaseDuration)
	ticker := time.NewTicker(time.Duration(m.opts.LeaderCheckInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			m.check(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// check leader and lease time out
// use transaction (txn queue) try to be leader and write lease uuid and time
// there don't need dist Lock performance optimization, if want, use CAS :)
func (m *LeaderChecker) check(ctx context.Context) {
	klog.CtxInfof(ctx, "check leader lease with uuid %s", m.uuid)
	res, err := m.kvClient.GetTxnKVClient().ExecuteTxn(ctx, func(txn *transaction.KVTxn) (interface{}, error) {
		leaderKey := jobEncodeLeaderKey()
		val, err := txn.Get(ctx, leaderKey)
		if err != nil {
			return nil, err
		}

		if uuid, tsTime, err := m.checkVal(val); err == nil {
			if time.Now().UTC().Sub(tsTime) <
				time.Duration(m.opts.LeaderLeaseDuration)*time.Second {
				if string(uuid) == m.uuid.String() {
					klog.CtxInfof(ctx, "I am already leader, renew lease with uuid %s and timestamp %d", uuid, tsTime)
					err = m.renewLease(txn)
					if err != nil {
						return false, err
					}
					return true, nil
				}
				return false, nil
			}
		}

		err = m.renewLease(txn)
		if err != nil {
			return false, err
		}

		return true, nil
	})
	if err != nil {
		klog.CtxErrorf(ctx, "check leader and renew lease failed, error: %s", err.Error())
		return
	}
	if res != nil && res.(bool) {
		klog.CtxInfof(ctx, "[job leader] I am leader with new release")
		return
	}
	klog.CtxInfof(ctx, "[job follower] leader already exists in lease duration")
}

// renewLease
// notice: ts not from PD, but value like TSO format
// value: uuid(36) | now(8)
func (m *LeaderChecker) renewLease(txn *transaction.KVTxn) error {
	return txn.Set(jobEncodeLeaderKey(),
		append([]byte(m.uuid.String()), PutInt64(time.Now().UTC().Unix())...))
}

// checkVal val should be in format uuid(36 bytes)+time(8 bytes)
func (m *LeaderChecker) checkVal(val []byte) ([]byte, time.Time, error) {
	if val == nil || len(val) != 44 {
		return nil, time.Time{}, ErrLeaderValSize
	}

	uuid, tsBytes := val[:36], val[36:]
	ts, err := Int64(tsBytes, nil)
	if err != nil {
		return nil, time.Time{}, err
	}

	tsTime := time.Unix(ts, 0).UTC()
	return uuid, tsTime, nil
}

// IsLeader
// if check leader val format and lease not time out, is true
func (m *LeaderChecker) IsLeader(ctx context.Context) bool {
	leaderKey := jobEncodeLeaderKey()
	val, err := m.kvClient.GetKVClient().Get(ctx, leaderKey)
	if err != nil {
		return false
	}

	uuid, tsTime, err := m.checkVal(val)
	if err != nil {
		return false
	}

	now := time.Now().UTC()
	if string(uuid) == m.uuid.String() &&
		now.Sub(tsTime) < time.Duration(m.opts.LeaderLeaseDuration)*time.Second {
		return true
	}
	return false
}
